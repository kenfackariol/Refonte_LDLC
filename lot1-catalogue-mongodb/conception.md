# Lot 1 — Catalogue Produits (MongoDB)

## 1. Choix technologique : MongoDB (base orientée document)

| Besoin métier | Réponse de MongoDB |
|---|---|
| Schémas variables selon la catégorie (un CPU n'a pas les mêmes caractéristiques qu'une souris) | Documents JSON/BSON sans schéma imposé : chaque fiche porte ses propres champs |
| Évolution sans migration (nouveaux attributs, nouvelles catégories) | Ajouter un champ à un document ne touche pas les autres, il n'y a pas d'`ALTER TABLE` |
| Lectures très fréquentes, écritures rares | Une fiche = un document, lu en **une seule requête sans jointure** ; lectures réparties sur les secondaires du replica set |
| Sous-documents (avis, specs, produits compatibles) | Imbrication native (objets et tableaux) |

**Pourquoi pas les autres modèles ?**
- **Clé-valeur (Redis)** : il ne permet pas de filtrer sur le contenu (« CPU AM5 à moins de 300 € »).
- **Colonnes (Cassandra)** : il faut connaître les requêtes à l'avance et le modèle est rigide par table, ce qui convient mal à un catalogue navigable par filtres multiples.
- **Relationnel** : c'est le problème de départ (table EAV ou une table par catégorie, avec des migrations à chaque nouvelle famille de produits).

**Volumétrie :** environ 27 000 produits, ce qui est faible pour MongoDB. Un **replica set de 3 nœuds** suffit, sans sharding. Si le catalogue grossit fortement, on pourra sharder sur `{ _id: "hashed" }`.

### Positionnement CAP

MongoDB en replica set est **CP** : un seul primaire accepte les écritures. En cas de partition réseau, le côté minoritaire ne peut plus écrire.

On règle ensuite la cohérence **requête par requête**, selon le besoin métier :
- **Fiche produit, listes et recherche** : `readPreference: "secondaryPreferred"`. Une description lue avec quelques millisecondes de retard ne pose aucun problème, et cela soulage le primaire pendant les pics de trafic (sorties de GPU…).
- **Prix et stock au moment de l'ajout au panier ou de la commande** : lecture sur le primaire avec `readConcern: "majority"`, car un prix erroné n'est pas acceptable.
- **Réservation du stock au paiement** : décrément **conditionnel et atomique** sur le primaire (`stock.quantity >= quantité demandée`), dans une transaction multi-documents quand la commande contient plusieurs produits (Q9). C'est ce qui empêche de vendre deux fois la dernière carte graphique lors d'une sortie très attendue.
- **Écritures** (back-office) : `writeConcern: { w: "majority" }`, pour qu'aucune modification acquittée ne soit perdue lors d'une bascule.

---

## 2. Modèle de données

### Vue d'ensemble des collections

```
categories  1 ──< products >── 1  brands
                     │
                     └──< reviews     (tous les avis, référencés)
```

| Collection | Rôle | Volume estimé |
|---|---|---|
| `products` | Fiches produits (document central, lu en une requête) | ~27 000 |
| `categories` | Arborescence des catégories | quelques centaines |
| `brands` | Marques partenaires | ~600 |
| `reviews` | Avis clients complets | plusieurs centaines de milliers, croissance illimitée |

### Document `products` (exemple : un processeur)

```js
{
  _id: ObjectId("..."),
  schemaVersion: 2,                       // Schema Versioning
  sku: "AMD-R7-7800X3D",
  slug: "amd-ryzen-7-7800x3d",
  name: "AMD Ryzen 7 7800X3D",
  type: "cpu",                            // Polymorphic : discriminant
  description: "Processeur 8 cœurs ...",

  // Extended Reference : copie des champs affichés, _id pour le lien
  category: { _id: ObjectId("..."), name: "Processeurs", path: "composants/processeurs" },
  brand:    { _id: ObjectId("..."), name: "AMD" },

  price: { amount: 389.95, currency: "EUR", promo: null },
  stock: { quantity: 142, status: "in_stock" },

  // Polymorphic : structure propre au type "cpu"
  specs: {
    socket: "AM5",
    cores: 8,
    threads: 16,
    baseClockGHz: 4.2,
    boostClockGHz: 5.0,
    tdpW: 120,
    cache: { l2MB: 8, l3MB: 96 }
  },

  // Attribute pattern : specs filtrables, sous forme clé / valeur / unité
  attributes: [
    { k: "socket",    v: "AM5" },
    { k: "cores",     v: 8 },
    { k: "tdp",       v: 120, u: "W" },
    { k: "frequence", v: 5.0, u: "GHz" }
  ],

  // Subset + Computed : résumé des avis + 5 derniers avis imbriqués
  reviews: {
    count: 1284,
    sum: 6034,
    avg: 4.7,                             // = sum / count, recalculé à chaque avis
    recent: [
      { _id: ObjectId("..."), author: "Julien M.", rating: 5,
        title: "Une bête en jeu", createdAt: ISODate("2026-11-10") }
      // ... 5 au maximum
    ]
  },

  // Extended Reference vers d'autres produits (affichage uniquement)
  compatible: [
    { _id: ObjectId("..."), sku: "ASUS-B650-PLUS", name: "ASUS TUF B650-Plus" }
  ],

  createdAt: ISODate("2025-04-06"),
  updatedAt: ISODate("2026-11-15")
}
```

Une souris aurait le même socle commun (`sku`, `name`, `price`, `brand`…) mais des `specs` différents (`dpi`, `buttons`, `wireless`…). Un logiciel aurait `specs.licence`, `specs.platforms`, etc. Les deux cohabitent dans la même collection.

### Document `reviews`

```js
{
  _id: ObjectId("..."),
  productId: ObjectId("..."),             // référence vers products
  customerId: "C-458712",
  author: "Julien M.",
  rating: 5,
  title: "Une bête en jeu",
  body: "...",
  verifiedPurchase: true,
  helpfulVotes: 37,
  createdAt: ISODate("2026-11-10")
}
```

---

## 3. Justification : Embedded vs References

La règle appliquée : **on imbrique ce qui est lu avec la fiche et reste borné ; on référence ce qui croît sans limite ou vit indépendamment.**

| Donnée | Choix | Raison |
|---|---|---|
| `specs`, `price`, `stock` | **Embedded** | Relation 1-1, toujours lue avec la fiche, n'existe pas seule |
| `attributes` | **Embedded** | Borné (une dizaine d'entrées), sert au filtrage |
| Catégorie, marque | **Extended Reference** | On copie `name` pour afficher la fiche sans jointure, et on garde `_id` pour la source de vérité. Ces noms changent très rarement, donc la duplication coûte peu |
| Avis | **Subset** : 5 récents imbriqués + **Reference** pour le reste | Un produit populaire peut avoir des milliers d'avis. Tout imbriquer ferait grossir le document sans limite (plafond de 16 Mo, et chaque lecture de fiche chargerait des avis jamais affichés). La fiche n'en montre que quelques-uns ; le reste est paginé à la demande dans `reviews` |
| Produits compatibles | **Extended Reference** (liste courte) | Sert seulement à afficher « compatible avec » sur la fiche. Le **parcours** des compatibilités (compatibles des compatibles, recommandations) relève du **Lot 4 (Neo4j)**, qui en est la source de vérité |

### Patterns utilisés

| Pattern | Où | Problème résolu |
|---|---|---|
| **Polymorphic** | `type` + `specs` variable | Des produits de natures différentes dans une seule collection, interrogeables ensemble (recherche, listes, promotions) |
| **Attribute** | `attributes: [{k, v, u}]` | Sans lui, il faudrait un index par caractéristique possible (socket, dpi, VRAM…), soit des centaines. Avec lui, **un seul index composé** `{ "attributes.k": 1, "attributes.v": 1 }` couvre tous les filtres, y compris ceux des futures catégories |
| **Extended Reference** | `category`, `brand`, `compatible` | Évite des `$lookup` à chaque affichage de fiche |
| **Subset** | `reviews.recent` | Garde le document petit et la lecture rapide |
| **Computed** | `reviews.count`, `reviews.sum`, `reviews.avg` | La note moyenne est calculée **à l'écriture** (rare) plutôt qu'à chaque lecture (fréquente) |
| **Schema Versioning** | `schemaVersion` | Les fiches évoluent sans migration globale : l'application sait lire les versions 1 et 2, et met à jour une fiche quand elle la modifie |

### Compromis assumés

- **Duplication** : si une marque est renommée, il faut mettre à jour ses produits (`updateMany` sur `brand._id`). C'est rare et peu coûteux au regard des millions de lectures économisées.
- **Double écriture pour un avis** : insertion dans `reviews` puis mise à jour du résumé dans `products`. Si la seconde échoue, le résumé est temporairement faux. On peut l'envelopper dans une **transaction multi-documents**, ou accepter une incohérence brève, corrigée par un recalcul périodique.
- **Attribute pattern** : la requête est un peu moins lisible (`$elemMatch`) et `v` mélange des types (texte et nombres). Le gain en indexation le justifie.

---

## 4. Index

```js
db.products.createIndex({ sku: 1 },  { unique: true })
db.products.createIndex({ slug: 1 }, { unique: true })              // page produit
db.products.createIndex({ "category.path": 1, "price.amount": 1 }) // liste par catégorie, triée par prix (Q2, Q6)
db.products.createIndex({ "brand._id": 1 })
db.products.createIndex({ "attributes.k": 1, "attributes.v": 1 })   // Attribute pattern
db.products.createIndex({ name: "text", description: "text" },
                        { default_language: "french" })              // recherche plein texte
db.reviews.createIndex({ productId: 1, createdAt: -1 })             // avis paginés d'un produit
```

L'index composé porte sur `category.path` parce que c'est le champ filtré par les listes (Q2, Q6) : l'égalité sur `category.path` puis le tri sur `price.amount` sont servis par le même index, sans tri en mémoire. Le renommage d'une catégorie (`updateMany` sur `category._id`) est rare et peut se permettre un parcours de la collection (27 000 documents).

---

## 5. Requêtes types

Le script exécutable se trouve dans [`requetes.js`](requetes.js).

**Q1 — Afficher une fiche produit (requête la plus fréquente)**
```js
db.products.findOne({ slug: "amd-ryzen-7-7800x3d" })
```
Une seule lecture, sans jointure, sur l'index `slug`.

**Q2 — Lister les CPU AM5 de plus de 8 cœurs, du moins cher au plus cher**
```js
db.products.find({
  "category.path": "composants/processeurs",
  attributes: { $all: [
    { $elemMatch: { k: "socket", v: "AM5" } },
    { $elemMatch: { k: "cores",  v: { $gte: 8 } } }
  ]}
}).sort({ "price.amount": 1 })
```

**Q3 — Ajouter un attribut sans migration** (nouvelle norme, par exemple la compatibilité PCIe 5.0)
```js
db.products.updateOne(
  { sku: "AMD-R7-7800X3D" },
  { $push: { attributes: { k: "pcie", v: "5.0" } }, $set: { "specs.pcieVersion": "5.0" } }
)
```
Les autres documents ne sont pas touchés.

**Q4 — Publier un avis (Subset + Computed en une écriture atomique sur le produit)**
```js
db.reviews.insertOne(review)
db.products.updateOne({ _id: review.productId }, [
  { $set: {
      "reviews.count": { $add: ["$reviews.count", 1] },
      "reviews.sum":   { $add: ["$reviews.sum", review.rating] },
      "reviews.recent": { $slice: [ { $concatArrays: [[ /* résumé de l'avis */ ], "$reviews.recent"] }, 5 ] }
  }},
  { $set: { "reviews.avg": { $round: [ { $divide: ["$reviews.sum", "$reviews.count"] }, 1 ] } } }
])
```

**Q5 — Avis paginés d'un produit (au-delà des 5 imbriqués)**
```js
db.reviews.find({ productId: id }).sort({ createdAt: -1 }).skip(5).limit(10)
```

**Q6 — Analyse : note moyenne et nombre de produits par marque, pour une catégorie**
```js
db.products.aggregate([
  { $match: { "category.path": "composants/processeurs" } },
  { $group: { _id: "$brand.name", produits: { $sum: 1 }, noteMoyenne: { $avg: "$reviews.avg" } } },
  { $sort: { noteMoyenne: -1 } }
])
```

**Q9 — Réserver le stock au paiement (décrément conditionnel, atomique)**
```js
const session = db.getMongo().startSession();
session.withTransaction(() => {
  const products = session.getDatabase("ldlc_catalogue").products;
  for (const { sku, qty } of panier) {
    const r = products.updateOne(
      { sku, "stock.quantity": { $gte: qty } },   // condition : assez de stock
      { $inc: { "stock.quantity": -qty } }
    );
    if (r.modifiedCount === 0) throw new Error(`Rupture de stock : ${sku}`); // annule toute la transaction
  }
}, { writeConcern: { w: "majority" }, readConcern: { level: "majority" } });
```
Le filtre et le décrément sont évalués ensemble sur le document : deux clients qui achètent la dernière unité en même temps ne peuvent pas réussir tous les deux. La transaction garantit le « tout ou rien » pour une commande de plusieurs produits. Si le paiement échoue ensuite, une écriture de compensation remet le stock (`$inc` positif).
