# Lot 3 : Logs et Analytics (Cassandra)

## 1. Choix technologique : Apache Cassandra (colonnes larges)

| Besoin métier | Réponse de Cassandra |
|---|---|
| **Millions d'événements par jour**, chaque clic = une écriture | Écriture *append-only* (commit log, puis memtable, puis SSTable) : pas de lecture avant écriture ni de verrou. C'est l'un des moteurs les plus rapides en écriture |
| Volume en croissance continue sur **plusieurs mois ou années** | **Scalabilité horizontale linéaire** : on ajoute des nœuds et les données se rééquilibrent automatiquement (anneau, hachage cohérent) |
| Disponibilité permanente (un log perdu, c'est une donnée analytique perdue) | Architecture **sans maître** : chaque nœud accepte les écritures, il n'y a pas de point unique de défaillance |
| Données horodatées, consultées par période | **Clustering columns** triées sur disque : lire « les 7 derniers jours » revient à lire une tranche contiguë |
| Conservation limitée des logs bruts | **TTL natif** par table, et compaction **TWCS** qui supprime des fichiers entiers une fois expirés |

**Pourquoi pas les autres ?**
- **MongoDB** : il sait écrire beaucoup, mais il repose sur un primaire unique par shard. À 20 millions d'écritures par jour et sur des années, il demanderait un sharding complexe à opérer, et ses index secondaires ralentiraient chaque insertion.
- **Redis** : stockage limité par la RAM, inadapté à des téraoctets d'historique.
- **Neo4j** : modèle sans rapport avec des séries temporelles.

**Ce que Cassandra ne fait pas, et qu'on assume :** pas de jointures, pas de `GROUP BY` libre, pas de tri sur une colonne qui n'est pas une clustering column. D'où une conception **Query-Driven** : on part des requêtes et on crée **une table par requête**, en acceptant de dupliquer les données.

### Hypothèses de volumétrie

| Hypothèse | Valeur retenue |
|---|---|
| Événements par jour | ~20 millions (pages vues, clics, recherches, ajouts au panier) |
| Heure de pointe | ~10 % du jour, soit 2 millions d'événements |
| Taille d'un événement | ~250 octets |
| Partition maximale recommandée | ~100 Mo |

### Positionnement CAP

Cassandra est **AP** : en cas de partition réseau, chaque nœud continue d'accepter lectures et écritures, et la cohérence est **éventuelle**, réconciliée ensuite par *hinted handoff*, *read repair* et `nodetool repair`.

Sa force est la **cohérence réglable par requête**. Avec un facteur de réplication **RF = 3** :

| Donnée | Écriture | Lecture | Raisonnement |
|---|---|---|---|
| Événements de navigation | `LOCAL_ONE` | `LOCAL_ONE` | On privilégie le débit : un clic qui manque au comptage n'a pas d'impact métier |
| Historique des commandes | `LOCAL_QUORUM` | `LOCAL_QUORUM` | 2 + 2 > 3, donc **R + W > RF** : la lecture voit toujours la dernière écriture. Un client doit voir la commande qu'il vient de passer |

Cette garantie ne vaut que si la commande est écrite **avant** d'être confirmée au client. C'est pourquoi `orders_by_customer` est écrite **de façon synchrone par l'API au paiement**, et non par un consommateur Kafka (voir § 5) : si la commande passait par Kafka, un retard du consommateur la rendrait invisible quelques secondes sur « Mes commandes », quel que soit le niveau de cohérence.

---

## 2. Démarche Query-Driven : des requêtes aux tables

| Requête métier | Table | Partition Key | Clustering Columns |
|---|---|---|---|
| « Combien de vues pour ce produit **aujourd'hui** ? » | `product_views_by_day` | `sku` | `day DESC` |
| « **Top produits** consultés cette semaine ? » | `top_products_by_period` (pré-calculée) | `(period_type, period)` | `rank ASC` |
| Source du classement (lue par le job) | `daily_views_by_product` | `(day, bucket)` | `sku` |
| « **Historique des commandes** d'un client » | `orders_by_customer` | `customer_id` | `order_date DESC, order_id` |
| Parcours d'un visiteur (support, personnalisation) | `events_by_visitor` | `(visitor_id, month)` | `event_time DESC, event_id` |
| Rejouer ou analyser tous les événements d'une période | `events_by_hour` | `(day, hour, bucket)` | `event_time DESC, event_id` |
| Recherches les plus tapées du jour | `top_products_by_period` (`period_type = 'search_day'`), alimentée depuis `search_terms_by_day` | `(period_type, period)` / `(day, bucket)` | `rank ASC` / `term` |

**Convention de temps :** `day`, `hour` et `month` sont toujours calculés **en UTC**, comme `event_time`. Sinon, un clic à 21 h 04 heure de Paris (20 h 04 UTC) serait rangé dans la partition `hour = 21` alors que son horodatage dit 20 h, et le passage à l'heure d'été ou d'hiver décalerait les partitions. La conversion en heure locale se fait à l'affichage.

Le schéma complet est dans [`schema.cql`](schema.cql) et les requêtes dans [`requetes.cql`](requetes.cql).

---

## 3. Modélisation détaillée

### Rappel : Partition Key et Clustering Columns

- La **Partition Key** décide **sur quel nœud** se trouvent les données. Une requête efficace cible **une seule partition**, dont elle doit fournir toute la clé.
- Les **Clustering Columns** décident de **l'ordre de tri** des lignes **à l'intérieur** de la partition. Elles permettent les tranches (`>=`, `<`) et le tri sans coût.

Une bonne partition est **ni trop petite** (sinon la requête doit en parcourir des milliers) **ni trop grosse** (au-delà d'environ 100 Mo, on a des *hot spots*, une compaction lente et des risques de timeout). Elle doit aussi **répartir la charge** sur tout le cluster.

### T1. `events_by_hour` : le journal brut

```sql
PRIMARY KEY ((day, hour, bucket), event_time, event_id)
```

- **Pourquoi pas `day` seul en partition ?** 20 millions × 250 octets = 5 Go dans une seule partition, posée sur seulement 3 nœuds, qui recevraient **toutes** les écritures de la journée : un *hot spot* majeur.
- **Avec `hour`**, on passe à 500 Mo à l'heure de pointe, ce qui reste trop.
- **Avec un `bucket`** (`hash(session_id) % 8`), on obtient environ 62 Mo par partition, et les écritures d'une même heure sont réparties sur 8 partitions, donc sur plusieurs nœuds. Le nombre de buckets est ajustable si le trafic grossit.
- **`event_id` (timeuuid)** garantit l'unicité si deux événements tombent à la même milliseconde. Sans lui, le second écraserait le premier : en Cassandra, un `INSERT` est un *upsert*.
- **Compromis** : lire une heure complète demande **8 requêtes** (une par bucket), lancées en parallèle par l'application. Ce coût de lecture est faible, puisque cette table sert surtout aux jobs d'analyse et à l'archive, et il est largement compensé par la répartition des écritures.

### T2. `events_by_visitor` : le parcours d'un visiteur

```sql
PRIMARY KEY ((visitor_id, month), event_time, event_id)
```

Si la partition était `visitor_id` seul, elle grossirait **sans limite** au fil des années (un client fidèle, un robot). Le découpage **par mois** la borne : c'est le *time bucketing*.

### T3. `product_views_by_day` : « combien de vues aujourd'hui ? »

```sql
views counter,  PRIMARY KEY ((sku), day)  -- CLUSTERING ORDER BY (day DESC)
```

- Un **compteur** est incrémenté à chaque vue (`views = views + 1`). La réponse est **pré-agrégée** : la lecture renvoie 1 ligne au lieu de compter des milliers d'événements.
- Partition = 1 produit, avec une ligne par jour, soit environ 365 lignes par an : très petite, même sur 10 ans.
- Grâce à la clustering column `day`, la même table répond aussi à « vues des 7 derniers jours » (tranche contiguë).

### T4 et T5 : le « top produits de la semaine »

C'est le cas difficile : **Cassandra ne sait pas trier par la valeur d'un compteur**. Un `ORDER BY views` est impossible, car seules les clustering columns sont triables, et un compteur ne peut pas en être une.

**Solution en deux temps :**
1. `daily_views_by_product` (partition = `(day, bucket)`, clustering = `sku`) : chaque vue incrémente aussi ce compteur.
   - **Pourquoi un `bucket` ?** Avec `day` seul en partition, **toutes** les vues de la journée incrémenteraient la même partition, donc les 3 mêmes nœuds : c'est exactement le *hot spot* qu'on a évité pour T1.
   - `bucket = hash(sku) % 8` répartit les écritures sur 8 partitions. Un produit tombe toujours dans le même bucket, donc chacun n'a qu'une ligne par jour. Une partition contient environ 27 000 / 8 ≈ 3 400 lignes.
   - Un produit très consulté reste concentré sur son bucket. C'est déjà le cas dans T3 (partition = 1 produit), et c'est absorbé par la **pré-agrégation** dans le consommateur Kafka (§ 5).
2. **Un job d'agrégation horaire** (Spark, ou simple script) lit les **7 jours × 8 buckets = 56 partitions** de la semaine, additionne, trie, puis écrit le top 100 dans `top_products_by_period`, où **le rang devient la clustering column**. La lecture du top est alors instantanée : 1 partition, déjà triée.

**Compromis** : le classement a jusqu'à 1 heure de retard. C'est acceptable pour un bloc « tendances de la semaine ». Pour un top **en temps réel** (par exemple pendant le lancement d'une carte graphique), un **Sorted Set Redis** (`ZINCRBY`, `ZREVRANGE`) serait l'outil adapté. On peut le citer comme extension, là encore en persistance polyglotte.

### T6. `search_terms_by_day` : les recherches du jour

```sql
searches counter,  PRIMARY KEY ((day, bucket), term)   -- bucket = hash(term) % 8
```

- **Même problème que T4**, en plus fort : chaque recherche incrémente la table, et le nombre de termes différents est **ouvert** (fautes de frappe, références exactes…). Avec `day` seul, la partition recevrait toutes les écritures et pourrait contenir des centaines de milliers de lignes.
- Le `bucket` répartit écritures et volume sur 8 partitions. Les termes sont **normalisés** avant écriture (minuscules, espaces et accents), ce qui regroupe les variantes.
- Comme pour les produits, Cassandra ne trie pas sur le compteur. Le **même job horaire** calcule le top des recherches et l'écrit dans `top_products_by_period` avec `period_type = 'search_day'` : la lecture du classement reste à **une partition** (Q6).

### T7. `orders_by_customer` : historique des commandes

```sql
PRIMARY KEY ((customer_id), order_date, order_id)  -- CLUSTERING ORDER BY (order_date DESC)
items list<frozen<order_item>>
```

- Partition = 1 client : la page « Mes commandes » lit **une partition**, déjà triée de la plus récente à la plus ancienne.
- Un client passe quelques dizaines de commandes en plusieurs années, donc la partition reste petite sans découpage temporel.
- **Les lignes de commande sont imbriquées** (liste d'un *User Defined Type*) : on n'a pas besoin de jointure, et le nom et le prix sont **figés au moment de l'achat**. C'est voulu : une facture ne doit pas changer si le prix du catalogue évolue.
- `order_id` en dernière clustering column évite qu'une commande en écrase une autre passée à la même seconde.
- **Écriture synchrone et idempotente** : l'API écrit la commande au paiement, en `LOCAL_QUORUM`, après la réservation du stock dans MongoDB (Lot 1, Q9). La commande n'est confirmée au client qu'après l'acquittement de Cassandra. En cas d'échec réseau, l'API rejoue le même `INSERT` : la clé primaire est identique, donc la ligne est simplement réécrite, sans doublon.
- **Source de vérité des commandes** : c'est cette table. Cassandra est AP par nature, mais `LOCAL_QUORUM` en écriture et en lecture (R + W > RF) donne une lecture cohérente dans le datacenter. Le seul point qui exige une garantie forte, ne pas vendre un produit absent, est traité en amont par MongoDB (CP, décrément conditionnel).

---

## 4. Conservation des données (TTL et compaction)

| Table | Rétention | Mécanisme |
|---|---|---|
| `events_by_hour`, `events_by_visitor` | **13 mois**, pour comparer une période à la même période de l'année précédente. C'est un choix de minimisation : la CNIL admet jusqu'à **25 mois** pour les données de mesure d'audience (les traceurs eux-mêmes étant limités à 13 mois) | `default_time_to_live = 34128000` |
| Compteurs (T3, T4, T6) | Plusieurs années (quelques Mo seulement) | Les compteurs **ne supportent pas le TTL** : purge par un job annuel si nécessaire |
| `top_products_by_period` | Illimitée (une centaine de lignes par période) | aucun |
| `orders_by_customer` | **10 ans** (obligation comptable) | Pas de TTL |

**TimeWindowCompactionStrategy (TWCS)** sur les tables d'événements : les SSTables sont regroupées par fenêtre de temps. Quand toutes les données d'une fenêtre ont expiré, **le fichier entier est supprimé**, sans réécriture coûteuse. C'est la stratégie recommandée pour les séries temporelles avec TTL.

**Taille de fenêtre : 14 jours.** La règle usuelle est de viser 20 à 50 fenêtres sur la durée de rétention. Avec 13 mois (395 jours), une fenêtre d'1 jour en créerait 395 (trop de fichiers à ouvrir par lecture), une fenêtre de 14 jours en crée environ 29.

---

## 5. Chemin d'écriture d'un clic

Un seul clic sur une fiche produit déclenche **4 écritures**. C'est le prix de la dénormalisation, et Cassandra est fait pour cela :

```
clic sur la fiche AMD-R7-7800X3D
 ├─ INSERT events_by_hour         (journal brut)
 ├─ INSERT events_by_visitor      (parcours du visiteur)
 ├─ UPDATE product_views_by_day   views + 1
 └─ UPDATE daily_views_by_product views + 1
```

- Les écritures sont envoyées en **asynchrone et en parallèle**, et non dans un `BATCH`. Un batch multi-partitions surcharge le coordinateur, et un batch ne peut pas mélanger compteurs et tables normales.
- En pratique, l'application dépose l'événement dans une file (Kafka), et un consommateur l'écrit dans Cassandra. Le site n'attend jamais l'écriture du log.
- **Pré-agrégation des compteurs** : le consommateur cumule les incréments en mémoire pendant quelques secondes, puis écrit `views = views + 37` au lieu de 37 écritures `+ 1`. Lors d'une sortie de GPU, cela divise d'autant la charge sur la partition du produit vedette. Le journal brut (`events_by_hour`) garde, lui, chaque événement.

**Exception : la commande.** Elle ne passe **pas** par ce chemin asynchrone. L'API l'écrit directement dans `orders_by_customer` (`LOCAL_QUORUM`) au moment du paiement, puis publie l'événement « commande validée » dans Kafka pour Neo4j et l'analytique.

---

## 6. Compromis assumés

| Compromis | Pourquoi on l'accepte |
|---|---|
| **Duplication** : un même événement est écrit dans 2 tables et 2 compteurs | Le disque coûte peu ; une lecture à 1 partition est ce qui permet de tenir la charge |
| **Une nouvelle question demande une nouvelle table** (et un éventuel rechargement depuis `events_by_hour`) | C'est le principe du Query-Driven. Le journal brut sert justement de source pour reconstruire une nouvelle vue |
| **Compteurs non idempotents** : si une écriture est rejouée après un timeout, la vue peut être comptée deux fois | Une erreur de l'ordre de 0,1 % est négligeable pour de l'analytique. Les chiffres exacts peuvent être recalculés depuis le journal brut |
| **Top produits avec jusqu'à 1 h de retard** | Suffisant pour un bloc « tendances » ; Redis peut prendre le relais pour le temps réel |
| **Buckets sur les compteurs du jour** : le job doit lire 8 partitions au lieu d'1 | Lecture rare (un job par heure) contre des millions d'écritures réparties sur le cluster |
| **Cohérence éventuelle** sur les logs | Un compteur légèrement en retard n'a aucun impact. Les commandes, elles, utilisent `LOCAL_QUORUM` |
