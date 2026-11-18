# Refonte LDLC : Architecture de données NoSQL

**Projet NSL NoSQL**

Binôme :
- Ariol **Kenfack**
- Raphael **Deutou**

## Description courte

LDLC (e-commerce high-tech : plus de 27 000 références, plus de 600 marques, jusqu'à 25 000 colis par jour) remplace sa base relationnelle monolithique par une architecture **NoSQL polyglotte**. Chaque besoin métier est confié à la technologie la mieux adaptée :

| Lot | Besoin | Technologie | Modèle | CAP |
|---|---|---|---|---|
| 1 | Catalogue produits | MongoDB | Document | CP |
| 2 | Panier et sessions utilisateurs | Redis | Clé-valeur (en mémoire) | AP |
| 3 | Logs et analytics | Cassandra | Colonnes larges | AP |
| 4 | Recommandations produits | Neo4j | Graphe | CP |

**Principe directeur :** CP là où une erreur coûte de l'argent (prix, stock, graphe), AP là où l'indisponibilité coûte plus cher qu'une incohérence passagère (panier, clics). Les bases communiquent par événements (Kafka, change streams MongoDB).

**Parcours d'un paiement :** le stock est réservé dans MongoDB par un décrément conditionnel (impossible de vendre deux fois la dernière unité), puis la commande est écrite de façon synchrone dans Cassandra (`LOCAL_QUORUM`) avant d'être confirmée au client. Les autres bases sont alimentées ensuite par Kafka.

### Schéma d'architecture globale

```mermaid
flowchart TB
    U["👤 Clients web & mobile"]
    LB["⚖️ Load balancer — sans sticky session"]
    BO["🛠️ Back-office LDLC"]

    subgraph APP["Couche applicative — serveurs API / Web sans état (×N)"]
        direction LR
        API1["API 1"] ~~~ API2["API 2"] ~~~ APIN["API N"]
    end

    K{{"📨 Kafka — bus d'événements<br/>clics · vues · recherches · commandes validées"}}

    R[("<b>Lot 2 — Redis</b><br/>Sessions & paniers (noeviction)<br/>Cache produit (allkeys-lru)<br/>Sentinel 1P + 2R · <b>AP</b>")]
    M[("<b>Lot 1 — MongoDB</b><br/>Catalogue produits · stock<br/>Replica set ×3 · <b>CP</b>")]
    N[("<b>Lot 4 — Neo4j</b><br/>Graphe de recommandation<br/>Raft + read replicas · <b>CP</b>")]
    C[("<b>Lot 3 — Cassandra</b><br/>Logs · Compteurs · Commandes<br/>RF = 3 · <b>AP</b>")]

    J["⏱️ Jobs batch<br/>top produits et recherches (horaire)<br/>réconciliation BOUGHT_WITH (hebdo)"]

    U --> LB --> APP
    BO -- "édition fiches · w: majority" --> M

    APP -- "session, panier · &lt; 1 ms" --> R
    APP -- "fiches : secondaryPreferred<br/>paiement : réservation du stock<br/>(primaire, décrément conditionnel)" --> M
    APP -- "recommandations<br/>lues sur réplicas" --> N
    APP -- "paiement : écriture commande<br/>(LOCAL_QUORUM)<br/>top produits, mes commandes" --> C
    APP -. "événements" .-> K

    M -. "change streams<br/>invalidation cache" .-> R
    M -. "change streams<br/>produits, standards" .-> N
    K -. "commandes validées<br/>+ BOUGHT_WITH incrémental" .-> N
    K -. "logs, compteurs<br/>(pré-agrégés)" .-> C

    J -. "BOUGHT_WITH" .-> N
    J -. "top_products" .-> C

    classDef redis fill:#fde8e8,stroke:#c0392b,color:#000
    classDef mongo fill:#e6f4ea,stroke:#2e7d32,color:#000
    classDef neo   fill:#e8eefc,stroke:#1a56db,color:#000
    classDef cass  fill:#e3f3f8,stroke:#1b6f8c,color:#000
    classDef infra fill:#f6f6f6,stroke:#777,color:#000
    classDef bus   fill:#fff4dd,stroke:#c98a00,color:#000
    class R redis
    class M mongo
    class N neo
    class C cass
    class U,LB,BO,API1,API2,APIN,J infra
    class K bus
    style APP  fill:#fafafa,stroke:#999
```

Flèches pleines : appels synchrones. Flèches pointillées : flux asynchrones. Détail des flux et des compromis : [`architecture/`](architecture/).

## Sommaire des livrables

| Livrable | Emplacement |
|---|---|
| **Document de conception (PDF, 10 pages)** | [`docs/Document_de_conception.pdf`](docs/Document_de_conception.pdf) |
| Schéma d'architecture globale et flux | [`architecture/`](architecture/) |
| Lot 1 : Catalogue (MongoDB), conception et script mongosh | [`lot1-catalogue-mongodb/`](lot1-catalogue-mongodb/) |
| Lot 2 : Panier et sessions (Redis), conception, commandes et script Lua | [`lot2-panier-redis/`](lot2-panier-redis/) |
| Lot 3 : Logs et analytics (Cassandra), conception, schéma CQL et requêtes | [`lot3-logs-cassandra/`](lot3-logs-cassandra/) |
| Lot 4 : Recommandations (Neo4j), conception, schéma Cypher et requêtes | [`lot4-reco-neo4j/`](lot4-reco-neo4j/) |

## Exécuter les scripts

| Lot | Commande |
|---|---|
| 1 | `mongosh lot1-catalogue-mongodb/requetes.js` |
| 2 | `redis-cli < lot2-panier-redis/commandes.redis` |
| 3 | `cqlsh -f lot3-logs-cassandra/schema.cql` puis `cqlsh -f lot3-logs-cassandra/requetes.cql` |
| 4 | Coller `schema.cypher` puis `requetes.cypher` dans Neo4j Browser |
