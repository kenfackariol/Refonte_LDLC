# Refonte LDLC — Architecture de données NoSQL

**Projet NSL NoSQL** — Binôme :
- Ariol **Kenfack**
- Raphael **Deutou**

## Description courte

LDLC (e-commerce high-tech : plus de 27 000 références, plus de 600 marques, jusqu'à 25 000 colis par jour) remplace sa base relationnelle monolithique par une architecture **NoSQL polyglotte**. Chaque besoin métier est confié à la technologie la mieux adaptée :

| Lot | Besoin | Technologie | Modèle |
|---|---|---|---|
| 1 | Catalogue produits | MongoDB | Document |
| 2 | Panier & sessions utilisateurs | Redis | Clé-valeur (en mémoire) |
| 3 | Logs & analytics | Cassandra | Colonnes larges |
| 4 | Recommandations produits | Neo4j | Graphe |

> Les choix sont justifiés lot par lot dans le document de conception (CAP, patterns, Query-Driven Design).

## Sommaire des livrables

| Livrable | Emplacement | Statut |
|---|---|---|
| Schéma d'architecture globale | [`architecture/`](architecture/) | À faire |
| Lot 1 — Catalogue (MongoDB) | [`lot1-catalogue-mongodb/`](lot1-catalogue-mongodb/) | À faire |
| Lot 2 — Panier & sessions (Redis) | [`lot2-panier-redis/`](lot2-panier-redis/) | À faire |
| Lot 3 — Logs & analytics (Cassandra) | [`lot3-logs-cassandra/`](lot3-logs-cassandra/) | À faire |
| Lot 4 — Recommandations (Neo4j) | [`lot4-reco-neo4j/`](lot4-reco-neo4j/) | À faire |
| Document de conception (10 p. max) | [`docs/`](docs/) | À faire |
| Bonus — Prototype API | [`api/`](api/) | Optionnel |
