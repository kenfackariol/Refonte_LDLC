# Lot 2 — Panier et Sessions Utilisateurs (Redis)

## 1. Choix technologique : Redis (clé-valeur en mémoire)

| Besoin métier | Réponse de Redis |
|---|---|
| Latence **sub-milliseconde** en lecture et en écriture | Données **en RAM** ; opérations en O(1) sur les clés et les Hash ; un aller-retour typique prend environ 0,1 à 0,5 ms sur le réseau local |
| Paniers **volatils**, expiration après inactivité | **TTL natif** sur chaque clé (`EXPIRE`) : Redis supprime seul les clés expirées, sans job de purge |
| Sessions **partagées entre plusieurs serveurs web** | Redis est un service central : n'importe quel serveur web lit la session. Les serveurs deviennent **sans état**, et le load balancer n'a plus besoin de *sticky sessions* |
| Mises à jour concurrentes (double clic, plusieurs onglets) | Commandes **atomiques** (`HINCRBY`) et scripts **Lua** exécutés sans interruption |

**Pourquoi pas les autres ?**
- **MongoDB** : persistance sur disque, latence de l'ordre de la milliseconde, et le TTL n'est géré que par un index purgé environ toutes les 60 secondes. C'est trop lent et trop imprécis pour une session.
- **Cassandra** : optimisé pour les écritures massives et durables. Ici, les données sont petites, éphémères et très souvent relues.
- **Sessions en mémoire du serveur web** : elles ne sont pas partagées. Un utilisateur renvoyé vers un autre serveur perd son panier.

### Dimensionnement

Une session pèse environ 300 octets et un panier environ 200 octets. Même avec **200 000 visiteurs actifs** lors d'un pic (sortie d'une nouvelle carte graphique), cela représente moins de 200 Mo de RAM. Une seule instance Redis tient la charge, avec plus de 100 000 opérations par seconde.

Architecture retenue : **Redis Sentinel**, avec 1 primaire, 2 réplicas et 3 sentinelles. Cela assure la haute disponibilité avec bascule automatique. **Redis Cluster** (sharding) ne deviendrait utile que si la volumétrie dépassait la RAM d'une machine. Pour ce cas, les noms de clés prévoient des *hash tags* (voir § 4).

On déploie **deux instances séparées**, parce que leurs données n'ont pas la même valeur :

| Instance | Contenu | Politique mémoire (`maxmemory-policy`) | Raison |
|---|---|---|---|
| `redis-sessions` | Sessions, paniers, produits récents | `noeviction` | Une session ou un panier ne doit **jamais** être supprimé pour faire de la place : le client serait déconnecté ou perdrait son panier en plein pic. Si la mémoire est pleine, les écritures sont refusées et l'alerte se déclenche (la RAM est dimensionnée très au-dessus du besoin, voir ci-dessus) |
| `redis-cache` | Cache produit `product:<sku>` | `allkeys-lru` | Un cache peut être évincé sans risque : la donnée est relue dans MongoDB |

### Positionnement CAP

La réplication Redis est **asynchrone** : le primaire confirme l'écriture au client avant que les réplicas l'aient reçue. Redis privilégie donc la **latence et la disponibilité** au détriment de la cohérence : lors d'une bascule, les dernières millisecondes d'écritures peuvent être perdues. On le range côté **AP**, avec une nuance à connaître :

- Redis Sentinel n'est **ni strictement AP ni strictement CP**. La bascule exige l'accord d'une **majorité de sentinelles** : un côté minoritaire de la partition ne peut pas élire de nouveau primaire, et ses clients perdent l'accès en écriture (ce n'est donc pas une disponibilité totale).
- À l'inverse, un ancien primaire isolé peut continuer à accepter des écritures **qui seront perdues** quand il redeviendra réplica (ce n'est donc pas de la cohérence forte). Les options `min-replicas-to-write 1` et `min-replicas-max-lag 10` limitent cette fenêtre : un primaire qui ne voit plus aucun réplica refuse les écritures.

**Ce compromis est acceptable ici, et c'est un choix assumé :**
- Perdre le dernier « ajouter au panier » lors d'une panne rare est une gêne mineure : le client reclique.
- Un panier lent ou indisponible, en revanche, fait perdre des ventes à chaque instant.
- **La commande validée ne vit pas dans Redis.** Au paiement, le panier est relu, le prix est revérifié et le stock est **réservé** sur le primaire MongoDB (décrément conditionnel, Lot 1, Q9), puis la commande est écrite de façon synchrone dans Cassandra (`orders_by_customer` en `LOCAL_QUORUM`, Lot 3). Le panier n'est supprimé qu'ensuite.

**Persistance** : AOF avec `appendfsync everysec` et snapshots RDB. Au pire, on perd 1 seconde de données lors d'un redémarrage, sans pénaliser la latence (pas de `fsync` à chaque écriture).

**Politique mémoire** : voir le tableau des deux instances ci-dessus (`noeviction` pour les sessions et paniers, `allkeys-lru` pour le cache).

---

## 2. Modèle de données (clés, structures, TTL)

Convention de nommage : `objet:type:identifiant`. Elle rend les clés lisibles et permet de les parcourir par préfixe avec `SCAN`. Dans ce tableau, `<…>` désigne une valeur variable (les accolades `{…}` sont réservées aux *hash tags* de Redis Cluster, voir § 4).

| Clé | Structure | Contenu | TTL | Instance |
|---|---|---|---|---|
| `session:<sessionId>` | **Hash** | `userId`, `createdAt`, `lastSeen`, `ip`, `ua`, `csrf` | **30 min**, glissant (réarmé à chaque requête) | sessions |
| `cart:anon:<sessionId>` | **Hash** | `sku → quantité` | **7 jours** après la dernière modification | sessions |
| `cart:user:<userId>` | **Hash** | `sku → quantité` | **30 jours** après la dernière modification | sessions |
| `user:<userId>:sessions` | **Set** | identifiants des sessions ouvertes du client | 30 jours | sessions |
| `recent:<sessionId>` | **List** (bornée à 10) | derniers SKU consultés | 7 jours | sessions |
| `product:<sku>` | **Hash** | `name`, `price`, `stock` (cache de MongoDB) | **5 min** | cache |

### Pourquoi ces structures ?

- **Hash pour le panier** plutôt qu'une chaîne JSON : on modifie **une ligne** avec `HINCRBY` sans relire ni réécrire tout le panier. L'opération est atomique, donc deux ajouts simultanés donnent bien +2 (avec du JSON, on aurait un problème de *lost update* : lire, modifier, réécrire). Un petit Hash est aussi encodé de façon très compacte en mémoire (*listpack*).
- **Le panier ne stocke que des quantités**, pas les prix. Le prix affiché vient du catalogue (cache `product:{sku}` ou MongoDB). On évite ainsi d'afficher un prix périmé si une promotion change pendant les 7 jours de vie du panier.
- **Hash pour la session** : on lit tous les champs en une commande (`HGETALL`) ou on en modifie un seul (`HSET lastSeen`).
- **Set pour les sessions d'un client** : pas de doublons, et cela permet de « déconnecter tous mes appareils » (on parcourt le Set et on supprime chaque `session:*`). Limite : une session expirée par TTL ne se retire pas seule du Set. On nettoie donc **à la lecture** : pour chaque identifiant, si `EXISTS session:<id>` renvoie 0, on fait `SREM` (Q9). Le Set reste petit (quelques appareils par client) et expire lui-même après 30 jours sans connexion.
- **List bornée pour l'historique de navigation** : `LPUSH` suivi de `LTRIM 0 9` conserve automatiquement les 10 derniers produits vus.

### Deux paniers distincts : anonyme et client

Un visiteur non connecté a un panier attaché à sa **session** (`cart:anon:`). Quand il se connecte, ce panier est **fusionné** dans celui de son compte (`cart:user:`), qui le suit sur tous ses appareils et dure plus longtemps. La fusion est un **script Lua** ([`fusion_panier.lua`](fusion_panier.lua)) : Redis l'exécute en un bloc, sans qu'aucune autre commande ne s'intercale. Le client ne peut donc pas perdre un article ajouté pendant la fusion.

---

## 3. Gestion de l'expiration (TTL)

Le besoin « volatil après **inactivité** » se traduit par un **TTL glissant** : à chaque action, l'application **réarme** le compteur.

```
Requête HTTP ─▶ HGETALL session:<id>      (lecture de la session)
             └▶ EXPIRE  session:<id> 1800 (le compte à rebours repart à 30 min)

Ajout panier ─▶ HINCRBY cart:anon:<id> <sku> 1
             └▶ EXPIRE  cart:anon:<id> 604800
```

Les deux commandes sont envoyées en **pipeline** (un seul aller-retour réseau), ou dans un bloc `MULTI`/`EXEC` si l'on veut qu'elles soient indissociables.

Sans activité, Redis supprime seul la clé à l'échéance. Il n'y a ni tâche planifiée ni table à purger.

| Durée | Justification métier |
|---|---|
| Session : 30 min | Standard de sécurité : une session oubliée sur un poste partagé ne reste pas ouverte |
| Panier anonyme : 7 jours | Le visiteur qui compare pendant quelques jours retrouve son panier ; au-delà, c'est un abandon |
| Panier client : 30 jours | Le client connecté a montré une intention plus forte, et son panier le suit d'un appareil à l'autre |
| Cache produit : 5 min | Compromis entre fraîcheur du prix et du stock et décharge de MongoDB lors des pics |

---

## 4. Passage à l'échelle avec Redis Cluster (si besoin)

En mode Cluster, les clés sont réparties sur 16 384 *slots* selon le hash de leur nom. Un script Lua ou un `MULTI` ne peut toucher que des clés du **même slot**. Pour la fusion des paniers, on utiliserait alors des **hash tags** : seule la partie entre accolades est hachée.

```
cart:anon:{s-7f3a9c}   et   session:{s-7f3a9c}   → même slot
```

La fusion `anon → user` impliquant deux identifiants différents, elle serait alors réalisée côté application (lecture du panier anonyme, puis `HINCRBY` dans le panier client, puis `DEL`). Ce cas est rare (une fois par connexion) et peut tolérer cette légère perte d'atomicité.

---

## 5. Requêtes types

Le script complet se trouve dans [`commandes.redis`](commandes.redis) (`redis-cli < commandes.redis`). Il demande **Redis 4 ou plus** (`HSET` avec plusieurs champs) et commence par `FLUSHDB` : à lancer sur une base de test uniquement.

**Q1 — Créer une session à la première visite**
```
HSET session:s-7f3a9c userId "" createdAt 1794945600 lastSeen 1794945600 ip 82.64.12.34 csrf tk_91ab
EXPIRE session:s-7f3a9c 1800
```

**Q2 — Lire la session à chaque requête et prolonger son TTL**
```
HGETALL session:s-7f3a9c
EXPIRE  session:s-7f3a9c 1800
```

**Q3 — Ajouter un produit au panier (atomique)**
```
HINCRBY cart:anon:s-7f3a9c AMD-R7-7800X3D 1
EXPIRE  cart:anon:s-7f3a9c 604800
```

**Q4 — Afficher le panier et le badge de l'en-tête**
```
HGETALL cart:anon:s-7f3a9c      → AMD-R7-7800X3D 2, ASUS-B650-PLUS 1
HLEN    cart:anon:s-7f3a9c      → 2
```

**Q5 — Modifier une quantité / retirer un produit**
```
HSET cart:anon:s-7f3a9c AMD-R7-7800X3D 1
HDEL cart:anon:s-7f3a9c ASUS-B650-PLUS
```

**Q6 — Connexion : fusionner le panier anonyme dans le panier du client**
```
redis-cli --eval fusion_panier.lua cart:anon:s-7f3a9c cart:user:42 , 2592000
```
Résultat obtenu au test : `AMD-R7-7800X3D 2, LOGI-GPRO-X2 1, ASUS-B650-PLUS 1`. Les quantités communes sont additionnées et le panier anonyme est supprimé.

**Q7 — Produits consultés récemment**
```
LPUSH  recent:s-7f3a9c AMD-R7-7800X3D
LTRIM  recent:s-7f3a9c 0 9
LRANGE recent:s-7f3a9c 0 -1
```

**Q8 — Cache produit (cache-aside devant MongoDB)**
```
HMGET product:AMD-R7-7800X3D name price     → si vide : lecture MongoDB, puis
HSET  product:AMD-R7-7800X3D name "AMD Ryzen 7 7800X3D" price 389.95 stock 142
EXPIRE product:AMD-R7-7800X3D 300
```

**Q9 — Lister les sessions actives d'un client (avec nettoyage) et les déconnecter**
```
SMEMBERS user:42:sessions        → pour chaque id :
EXISTS   session:<id>            →   0 : SREM user:42:sessions <id>   (session expirée)
DEL      session:<id>            → pour « déconnecter tous mes appareils »
DEL      user:42:sessions
```

---

## 6. Compromis assumés

| Compromis | Pourquoi on l'accepte |
|---|---|
| Perte possible des dernières écritures lors d'une bascule (réplication asynchrone) | Un panier n'est pas une commande. La latence et la disponibilité priment, et la commande est rendue durable ailleurs |
| Données limitées par la RAM | Le volume est très faible (sessions et paniers éphémères, nettoyés par les TTL) |
| Pas de requêtes par contenu (« quels paniers contiennent ce produit ? ») | Ce n'est pas un besoin du parcours d'achat. Pour l'analyse des paniers abandonnés, les événements sont envoyés dans Cassandra (Lot 3) |
| Prix non figé dans le panier | Le prix est toujours celui du catalogue et il est revérifié au paiement, ce qui évite d'afficher un prix périmé |
