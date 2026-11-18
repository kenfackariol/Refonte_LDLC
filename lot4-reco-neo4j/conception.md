# Lot 4 — Recommandations Produits (Neo4j)

## 1. Choix technologique : Neo4j (base orientée graphe)

| Besoin métier | Réponse de Neo4j |
|---|---|
| Relations complexes entre produits (compatibilité, gamme, marque) | Les relations sont des **objets de premier rang**, stockées physiquement, typées et porteuses de propriétés |
| **Parcours de relations** sur plusieurs sauts (« clients qui ont acheté X ont aussi acheté Y », chaîne CPU → carte mère → RAM) | **Index-free adjacency** : chaque nœud pointe directement vers ses voisins. Suivre une relation coûte un saut de pointeur, **indépendamment de la taille totale de la base** |
| Requêtes de recommandation lisibles et évolutives | **Cypher** décrit des motifs visuels : `(client)-[:PLACED]->(commande)-[:CONTAINS]->(produit)` |

**Pourquoi pas les autres ?**
- **Relationnel** : « ont aussi acheté » demande 4 jointures (produit, lignes, commandes, lignes, produit) sur des tables de millions de lignes. Le coût **explose avec chaque saut supplémentaire**, car chaque jointure passe par un index dont le coût augmente avec le volume.
- **MongoDB** : `$graphLookup` existe, mais reste limité (un seul type de relation, pas de motifs complexes, coûteux en profondeur).
- **Cassandra** : aucune notion de relation ; chaque parcours devrait être pré-calculé à l'avance dans une table.

### Rôle dans l'architecture polyglotte

Neo4j **ne remplace pas** le catalogue. Il ne stocke que **l'identité des produits** (`sku`, `name`, `price`) et **leurs liens**. La fiche complète reste dans MongoDB (Lot 1), et le `sku` fait le lien entre les deux.

Neo4j est alimenté par deux flux :
- les **change streams MongoDB** (création ou modification d'un produit, de ses standards techniques) ;
- le **flux des commandes validées** : l'événement « commande validée » que l'API publie dans Kafka après avoir écrit la commande dans Cassandra (`orders_by_customer`, Lot 3). Le consommateur Neo4j crée la commande et met à jour `BOUGHT_WITH` (Q7).

### Volumétrie

| Élément | Volume estimé |
|---|---|
| Nœuds `Product` | ~27 000 |
| Nœuds `Customer` | quelques millions |
| Nœuds `Order` | ~9 millions par an (25 000 colis par jour) |
| Relations `CONTAINS` | ~25 millions par an |

Ces volumes sont confortables pour Neo4j : le graphe tient en mémoire (*page cache*) sur une machine bien dimensionnée.

### Positionnement CAP

Un cluster Neo4j repose sur des **primaires** qui valident les écritures par consensus **Raft** (majorité requise) : le cœur est **CP**. En cas de partition, le côté minoritaire refuse les écritures.

Les **secondaires** (*read replicas*) sont mis à jour de façon **asynchrone**, donc la lecture y est **à cohérence éventuelle**.

**Choix :** toutes les requêtes de recommandation sont envoyées vers les secondaires. Une recommandation calculée sur des données vieilles de quelques secondes reste parfaitement pertinente, et on décharge les primaires, qui ne gèrent que l'ingestion des commandes et des produits.

---

## 2. Modèle de données

### Nœuds

| Label | Propriétés | Rôle |
|---|---|---|
| `Product` | `sku` (unique), `name`, `price`, `lengthMm`, `maxGpuLengthMm`… | Le produit (identité et propriétés utiles aux filtres) |
| `Category` | `name` | Catégorie (Processeurs, Cartes mères…) |
| `Brand` | `name` | Marque |
| `Range` | `name` | Gamme (Ryzen 7, GeForce RTX 50…) |
| `Standard` | `code`, `kind` (`socket`, `memoire`, `format`) | **Norme technique partagée** : AM5, DDR5, ATX… |
| `Customer` | `id` | Client (identifiant seulement : pas de données personnelles dans le graphe) |
| `Order` | `id`, `date` | Commande |

### Relations

| Relation | Propriétés | Signification |
|---|---|---|
| `(Product)-[:IN_CATEGORY]->(Category)` | — | Appartenance à une catégorie |
| `(Product)-[:MADE_BY]->(Brand)` | — | Marque |
| `(Product)-[:IN_RANGE]->(Range)` | — | Gamme |
| `(Product)-[:USES]->(Standard)` | — | Le produit **est** de ce standard (CPU AM5, carte mère ATX) |
| `(Product)-[:ACCEPTS]->(Standard)` | — | Le produit **accueille** ce standard (boîtier qui accepte l'ATX et l'ITX) |
| `(Customer)-[:PLACED]->(Order)` | — | Le client a passé la commande |
| `(Order)-[:CONTAINS]->(Product)` | `quantity` | Ligne de commande |
| `(Product)-[:BOUGHT_WITH]-(Product)` | `count`, `updatedAt` | **Relation matérialisée** : nombre de commandes contenant les deux produits |

### Schéma visuel

```
                (Category)   (Brand)   (Range)
                      ▲          ▲         ▲
           IN_CATEGORY│   MADE_BY│ IN_RANGE│
                      │          │         │
(Customer)─PLACED─▶(Order)─CONTAINS─▶(Product)◀─BOUGHT_WITH─▶(Product)
                                     │      │
                                 USES│      │ACCEPTS
                                     ▼      ▼
                                   (Standard : AM5, DDR5, ATX, ITX…)
```

---

## 3. Choix de modélisation justifiés

### a) La compatibilité passe par des nœuds `Standard`, pas par des liens directs produit-produit

**Solution naïve :** une relation `COMPATIBLE_WITH` entre chaque CPU AM5 et chaque carte mère AM5. Avec 50 CPU AM5 et 150 cartes mères AM5, cela fait **7 500 relations** à créer et maintenir. Chaque nouvelle carte mère demanderait de la relier à **tous** les CPU concernés.

**Solution retenue :** chaque produit est relié **une seule fois** à ses standards. 50 + 150 = **200 relations**. Une nouvelle carte mère AM5 se relie au seul nœud `AM5` et devient **automatiquement** compatible avec tous les CPU AM5.

La compatibilité devient un **parcours** : `(cpu)-[:USES]->(:Standard)<-[:USES]-(carte_mère)`.

**`USES` et `ACCEPTS` sont distincts** parce que la compatibilité n'est pas toujours symétrique : un boîtier ATX **accueille** des cartes ATX, mATX et ITX, alors qu'une carte mère **est** d'un seul format.

**Les contraintes numériques** (longueur de la carte graphique par rapport à la place dans le boîtier) restent des **propriétés** comparées dans la requête. Créer un nœud par longueur n'aurait pas de sens.

### b) Le nœud `Order` entre le client et le produit

On pourrait relier directement `(Customer)-[:BOUGHT]->(Product)`. Le nœud `Order` permet de distinguer :
- **acheté dans la même commande** : signal fort de complémentarité (le CPU et la carte mère d'une même configuration) ;
- **acheté par le même client** à des moments différents : signal plus faible, utilisé pour la personnalisation (Q5).

Le compromis est un saut de plus à chaque parcours.

### c) La relation matérialisée `BOUGHT_WITH` : le problème des supernœuds

Un produit phare (par exemple le Ryzen 7 7800X3D) peut figurer dans **50 000 commandes**. Calculer « achetés ensemble » en temps réel oblige alors à parcourir 50 000 commandes, puis toutes leurs lignes, à **chaque affichage de la fiche**. C'est le problème du **supernœud**.

**Solution :** les co-achats sont stockés sur une relation `BOUGHT_WITH {count}`. La fiche produit n'a plus qu'**un seul saut** à faire (Q1 bis).

**Comment la tenir à jour ?** Un recalcul complet chaque nuit déplacerait le problème au lieu de le supprimer : le job devrait lui-même reparcourir les 50 000 commandes du produit vedette. On met donc la relation à jour **de façon incrémentale, à l'ingestion de chaque commande** (Q7) :
- une commande contient quelques produits, donc seulement quelques paires à incrémenter (`count + 1`), quel que soit le nombre total de commandes ;
- l'ingestion est **idempotente** : si Kafka rejoue l'événement, la commande existe déjà et rien n'est recompté ;
- un **recalcul complet hebdomadaire**, hors heures de pointe, sert de réconciliation (rattraper une erreur, supprimer des paires obsolètes).

| Version | Fraîcheur | Coût à la lecture | Usage |
|---|---|---|---|
| Parcours en direct (Q1) | Temps réel | Proportionnel au nombre de commandes | Analyse ponctuelle, produits peu vendus |
| `BOUGHT_WITH` (Q1 bis) | Quelques secondes (délai du consommateur Kafka) | 1 saut, quasi constant | **Fiche produit en production** |

C'est le même raisonnement que le pattern **Computed** du Lot 1 : on calcule à l'écriture, ou en différé, ce qui est lu très souvent.

### d) Le graphe ne contient que le nécessaire

Ni description, ni avis, ni données personnelles des clients : seulement des identifiants. Le graphe reste compact, ce qui maximise la part tenue en mémoire et donc la vitesse des parcours. Les clients sont pseudonymisés (RGPD).

---

## 4. Requêtes types

Le jeu de données est dans [`schema.cypher`](schema.cypher) et les requêtes dans [`requetes.cypher`](requetes.cypher). Le résultat attendu est indiqué sous chaque requête.

**Q1 — « Les clients qui ont acheté ce processeur ont aussi acheté cette carte mère »**
```cypher
MATCH (p:Product {sku: 'AMD-R7-7800X3D'})<-[:CONTAINS]-(o:Order)-[:CONTAINS]->(autre:Product)
      -[:IN_CATEGORY]->(:Category {name: 'Cartes mères'})
RETURN autre.name AS produit, count(DISTINCT o) AS commandes_communes
ORDER BY commandes_communes DESC LIMIT 5
```
Résultat : ASUS TUF B650-Plus (2 commandes), MSI B650M (1).

**Q1 bis — Même question en production, via la relation matérialisée**
```cypher
MATCH (p:Product {sku: 'AMD-R7-7800X3D'})-[r:BOUGHT_WITH]-(autre:Product)
RETURN autre.name, r.count ORDER BY r.count DESC LIMIT 5
```

**Q2 — « Produits compatibles avec ce boîtier »**
```cypher
MATCH (boitier:Product {sku: 'CM-NR200P'})-[:ACCEPTS]->(f:Standard {kind: 'format'})<-[:USES]-(cm:Product)
RETURN 'Carte mère' AS type, cm.name AS produit
UNION
MATCH (boitier:Product {sku: 'CM-NR200P'}), (gpu:Product)-[:IN_CATEGORY]->(:Category {name: 'Cartes graphiques'})
WHERE gpu.lengthMm <= boitier.maxGpuLengthMm
RETURN 'Carte graphique' AS type, gpu.name AS produit
```
Résultat : ASRock B650I (ITX) et RTX 5070 (300 mm ≤ 336 mm). La RTX 5090, avec ses 357 mm, est exclue.

**Q3 — Configurateur : parcours CPU → carte mère → mémoire → boîtier**
```cypher
MATCH (cpu:Product {sku: 'AMD-R7-7800X3D'})-[:USES]->(:Standard {kind: 'socket'})
      <-[:USES]-(cm:Product)-[:IN_CATEGORY]->(:Category {name: 'Cartes mères'})
MATCH (cm)-[:USES]->(:Standard {kind: 'memoire'})<-[:USES]-(ram:Product)-[:IN_CATEGORY]->(:Category {name: 'Mémoire'})
MATCH (cm)-[:USES]->(fmt:Standard {kind: 'format'})<-[:ACCEPTS]-(boitier:Product)
RETURN cm.name, fmt.code, collect(DISTINCT ram.name), collect(DISTINCT boitier.name)
```
C'est l'équivalent produit des « amis d'amis » : on parcourt **6 sauts** en une seule requête, ce qui serait très coûteux en SQL.

**Q4 — Produits similaires avec un score** (même gamme = 3, même marque = 2, +1 par standard technique commun, prix à ±30 %)

Seuls les standards **techniques** (socket, mémoire) comptent. Le **format** physique (ATX, ITX…) est exclu : deux cartes mères ATX, l'une AMD et l'autre Intel, ne sont pas similaires pour un client.

Résultat pour l'ASUS B650-Plus : MSI B650M (AM5 + DDR5 : score 2), ASRock B650I (2), Gigabyte Z890 (DDR5 seulement : score 1).

**Q5 — Recommandations personnalisées (filtrage collaboratif)**
```cypher
MATCH (moi:Customer {id: 'U-42'})-[:PLACED]->(:Order)-[:CONTAINS]->(p:Product)
      <-[:CONTAINS]-(:Order)<-[:PLACED]-(autre:Customer)
WHERE autre <> moi
MATCH (autre)-[:PLACED]->(:Order)-[:CONTAINS]->(reco:Product)
WHERE NOT EXISTS { (moi)-[:PLACED]->(:Order)-[:CONTAINS]->(reco) }
RETURN reco.name, count(DISTINCT autre) AS clients_similaires
ORDER BY clients_similaires DESC LIMIT 5
```
U-42 vient d'acheter un 7800X3D. Les 3 autres acheteurs de ce processeur ont pris l'ASUS B650-Plus, la DDR5 Corsair et la RTX 5070 (2 clients chacun) : ce sont les recommandations.

**Q6 — Produits « pivots » pour composer des packs** (merchandising) : classement par nombre total de co-achats.

**Q7 — Ingestion d'une commande et mise à jour incrémentale de `BOUGHT_WITH`** (consommateur Kafka)
```cypher
WITH {id: 'CMD-7', customer: 'U-42', date: date('2026-11-18'),
      items: ['AMD-R7-7800X3D', 'ASUS-B650-PLUS']} AS o
MERGE (c:Customer {id: o.customer})
WITH c, o WHERE NOT EXISTS { MATCH (:Order {id: o.id}) }      // idempotence
CREATE (c)-[:PLACED]->(ord:Order {id: o.id, date: o.date})
WITH ord, o UNWIND o.items AS sku
MATCH (p:Product {sku: sku}) CREATE (ord)-[:CONTAINS {quantity: 1}]->(p)
WITH collect(p) AS produits
UNWIND produits AS pa UNWIND produits AS pb
WITH pa, pb WHERE pa.sku < pb.sku
MERGE (pa)-[r:BOUGHT_WITH]-(pb)
  ON CREATE SET r.count = 1  ON MATCH SET r.count = r.count + 1
```
La paire Ryzen 7 / ASUS B650-Plus passe de 2 à 3 co-achats. Relancer la requête ne change rien.

---

## 5. Compromis assumés

| Compromis | Pourquoi on l'accepte |
|---|---|
| **Données dupliquées** depuis MongoDB (sku, nom, prix) et depuis le flux des commandes | Le graphe doit être autonome pour parcourir vite. La synchronisation par change streams est asynchrone, et un léger décalage est sans conséquence sur une recommandation |
| **`BOUGHT_WITH` dénormalisé** : une écriture de plus par paire de produits à chaque commande | Quelques paires par commande, environ 25 000 commandes par jour : charge faible. En échange, la lecture sur la fiche produit coûte un seul saut, même pour un supernœud |
| **Compteurs incrémentaux** : une erreur d'ingestion fausse durablement un `count` | Ingestion idempotente, et réconciliation hebdomadaire par recalcul complet |
| **Lecture sur les secondaires** : cohérence éventuelle | Une recommandation n'est pas une donnée critique |
| **Scalabilité en écriture limitée** (primaires Raft, pas de sharding automatique comme Cassandra) | Les écritures sont modestes (~25 000 commandes par jour, quelques produits par jour). La charge est en lecture, et elle se répartit en ajoutant des secondaires |
| **Les règles de compatibilité dépendent de la qualité des données** (un standard oublié = un produit invisible) | Les standards sont saisis une seule fois par produit dans le back-office du catalogue, puis propagés automatiquement |
