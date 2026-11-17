// Lot 1 — Catalogue produits (MongoDB)
// Exécution : mongosh "mongodb://localhost:27017" requetes.js

db = db.getSiblingDB("ldlc_catalogue");
db.dropDatabase();

// ---------------------------------------------------------------
// 1. Données de référence
// ---------------------------------------------------------------
const catCpu    = { _id: new ObjectId(), name: "Processeurs",   path: "composants/processeurs" };
const catMb     = { _id: new ObjectId(), name: "Cartes mères",  path: "composants/cartes-meres" };
const catSouris = { _id: new ObjectId(), name: "Souris",        path: "peripheriques/souris" };
db.categories.insertMany([catCpu, catMb, catSouris]);

const amd      = { _id: new ObjectId(), name: "AMD" };
const intel    = { _id: new ObjectId(), name: "Intel" };
const asus     = { _id: new ObjectId(), name: "ASUS" };
const logitech = { _id: new ObjectId(), name: "Logitech" };
db.brands.insertMany([amd, intel, asus, logitech]);

const ref = (doc) => ({ _id: doc._id, name: doc.name });
const emptyReviews = () => ({ count: 0, sum: 0, avg: null, recent: [] });

// ---------------------------------------------------------------
// 2. Produits polymorphes (Polymorphic + Attribute + Extended Reference)
// ---------------------------------------------------------------
const mbId = new ObjectId();

db.products.insertMany([
  {
    schemaVersion: 2, sku: "AMD-R7-7800X3D", slug: "amd-ryzen-7-7800x3d",
    name: "AMD Ryzen 7 7800X3D", type: "cpu",
    description: "Processeur 8 cœurs avec 3D V-Cache pour le jeu",
    category: catCpu, brand: ref(amd),
    price: { amount: 389.95, currency: "EUR", promo: null },
    stock: { quantity: 142, status: "in_stock" },
    specs: { socket: "AM5", cores: 8, threads: 16, baseClockGHz: 4.2, boostClockGHz: 5.0, tdpW: 120 },
    attributes: [
      { k: "socket", v: "AM5" }, { k: "cores", v: 8 },
      { k: "tdp", v: 120, u: "W" }, { k: "frequence", v: 5.0, u: "GHz" }
    ],
    reviews: emptyReviews(),
    compatible: [{ _id: mbId, sku: "ASUS-B650-PLUS", name: "ASUS TUF B650-Plus" }],
    createdAt: new Date("2025-04-06"), updatedAt: new Date()
  },
  {
    schemaVersion: 2, sku: "AMD-R9-9950X", slug: "amd-ryzen-9-9950x",
    name: "AMD Ryzen 9 9950X", type: "cpu",
    description: "Processeur 16 cœurs pour la création",
    category: catCpu, brand: ref(amd),
    price: { amount: 599.95, currency: "EUR", promo: { amount: 549.95, until: new Date("2026-11-30") } },
    stock: { quantity: 37, status: "in_stock" },
    specs: { socket: "AM5", cores: 16, threads: 32, baseClockGHz: 4.3, boostClockGHz: 5.7, tdpW: 170 },
    attributes: [
      { k: "socket", v: "AM5" }, { k: "cores", v: 16 },
      { k: "tdp", v: 170, u: "W" }, { k: "frequence", v: 5.7, u: "GHz" }
    ],
    reviews: emptyReviews(), compatible: [],
    createdAt: new Date("2025-08-15"), updatedAt: new Date()
  },
  {
    schemaVersion: 2, sku: "INTEL-CU7-265K", slug: "intel-core-ultra-7-265k",
    name: "Intel Core Ultra 7 265K", type: "cpu",
    description: "Processeur 20 cœurs Arrow Lake",
    category: catCpu, brand: ref(intel),
    price: { amount: 329.95, currency: "EUR", promo: null },
    stock: { quantity: 0, status: "out_of_stock" },
    specs: { socket: "LGA1851", cores: 20, threads: 20, baseClockGHz: 3.9, boostClockGHz: 5.5, tdpW: 125 },
    attributes: [
      { k: "socket", v: "LGA1851" }, { k: "cores", v: 20 },
      { k: "tdp", v: 125, u: "W" }, { k: "frequence", v: 5.5, u: "GHz" }
    ],
    reviews: emptyReviews(), compatible: [],
    createdAt: new Date("2024-10-24"), updatedAt: new Date()
  },
  {
    _id: mbId,
    schemaVersion: 2, sku: "ASUS-B650-PLUS", slug: "asus-tuf-b650-plus",
    name: "ASUS TUF B650-Plus", type: "motherboard",
    description: "Carte mère ATX AM5",
    category: catMb, brand: ref(asus),
    price: { amount: 189.95, currency: "EUR", promo: null },
    stock: { quantity: 58, status: "in_stock" },
    specs: { socket: "AM5", chipset: "B650", formFactor: "ATX", ramSlots: 4, ramType: "DDR5", m2Slots: 2 },
    attributes: [
      { k: "socket", v: "AM5" }, { k: "chipset", v: "B650" },
      { k: "format", v: "ATX" }, { k: "memoire", v: "DDR5" }
    ],
    reviews: emptyReviews(), compatible: [],
    createdAt: new Date("2023-03-10"), updatedAt: new Date()
  },
  {
    // Ancienne fiche en version 1 (pas de tableau attributes) : elle coexiste sans migration
    schemaVersion: 1, sku: "LOGI-GPRO-X2", slug: "logitech-g-pro-x-superlight-2",
    name: "Logitech G Pro X Superlight 2", type: "mouse",
    description: "Souris gaming sans fil 60 g",
    category: catSouris, brand: ref(logitech),
    price: { amount: 159.95, currency: "EUR", promo: null },
    stock: { quantity: 210, status: "in_stock" },
    specs: { dpi: 32000, buttons: 5, wireless: true, weightG: 60 },
    reviews: emptyReviews(), compatible: [],
    createdAt: new Date("2023-09-12"), updatedAt: new Date()
  }
]);

// ---------------------------------------------------------------
// 3. Index
// ---------------------------------------------------------------
db.products.createIndex({ sku: 1 },  { unique: true });
db.products.createIndex({ slug: 1 }, { unique: true });
db.products.createIndex({ "category._id": 1, "price.amount": 1 });
db.products.createIndex({ "brand._id": 1 });
db.products.createIndex({ "attributes.k": 1, "attributes.v": 1 });
db.products.createIndex({ name: "text", description: "text" }, { default_language: "french" });
db.reviews.createIndex({ productId: 1, createdAt: -1 });

// ---------------------------------------------------------------
// 4. Requêtes types
// ---------------------------------------------------------------
const titre = (t) => print(`\n===== ${t} =====`);

titre("Q1 - Fiche produit par slug");
printjson(db.products.findOne({ slug: "amd-ryzen-7-7800x3d" }, { name: 1, price: 1, specs: 1, compatible: 1 }));

titre("Q2 - CPU AM5 avec au moins 8 cœurs, triés par prix");
db.products.find({
  "category.path": "composants/processeurs",
  attributes: { $all: [
    { $elemMatch: { k: "socket", v: "AM5" } },
    { $elemMatch: { k: "cores",  v: { $gte: 8 } } }
  ]}
}, { _id: 0, name: 1, "price.amount": 1 }).sort({ "price.amount": 1 }).forEach(printjson);

titre("Q3 - Ajout d'un attribut sans migration");
db.products.updateOne(
  { sku: "AMD-R7-7800X3D" },
  { $push: { attributes: { k: "pcie", v: "5.0" } }, $set: { "specs.pcieVersion": "5.0", updatedAt: new Date() } }
);
printjson(db.products.findOne({ sku: "AMD-R7-7800X3D" }, { _id: 0, attributes: 1 }));

titre("Q4 - Publication d'avis (Subset + Computed)");
function publierAvis(productSku, author, rating, title, body) {
  const product = db.products.findOne({ sku: productSku }, { _id: 1 });
  const review = {
    _id: new ObjectId(), productId: product._id, customerId: "C-" + Math.floor(Math.random() * 1e6),
    author, rating, title, body, verifiedPurchase: true, helpfulVotes: 0, createdAt: new Date()
  };
  db.reviews.insertOne(review);
  const resume = { _id: review._id, author, rating, title, createdAt: review.createdAt };
  db.products.updateOne({ _id: product._id }, [
    { $set: {
        "reviews.count":  { $add: ["$reviews.count", 1] },
        "reviews.sum":    { $add: ["$reviews.sum", rating] },
        "reviews.recent": { $slice: [{ $concatArrays: [[resume], "$reviews.recent"] }, 5] }
    }},
    { $set: { "reviews.avg": { $round: [{ $divide: ["$reviews.sum", "$reviews.count"] }, 1] } } }
  ]);
}
const notes = [5, 5, 4, 5, 3, 5, 4];
notes.forEach((n, i) => publierAvis("AMD-R7-7800X3D", `Client ${i + 1}`, n, `Avis n°${i + 1}`, "..."));
publierAvis("AMD-R9-9950X", "Sarah L.", 5, "Parfait en rendu 3D", "...");
publierAvis("INTEL-CU7-265K", "Marc D.", 3, "Correct", "...");

const r = db.products.findOne({ sku: "AMD-R7-7800X3D" }, { _id: 0, reviews: 1 }).reviews;
print(`count=${r.count} avg=${r.avg} avis imbriqués=${r.recent.length} (plafonné à 5)`);
print(`avis dans la collection reviews : ${db.reviews.countDocuments({ productId: db.products.findOne({ sku: "AMD-R7-7800X3D" })._id })}`);

titre("Q5 - Avis paginés au-delà des 5 imbriqués");
const pid = db.products.findOne({ sku: "AMD-R7-7800X3D" })._id;
db.reviews.find({ productId: pid }, { _id: 0, author: 1, rating: 1 })
  .sort({ createdAt: -1 }).skip(5).limit(10).forEach(printjson);

titre("Q6 - Note moyenne par marque (processeurs)");
db.products.aggregate([
  { $match: { "category.path": "composants/processeurs" } },
  { $group: { _id: "$brand.name", produits: { $sum: 1 }, noteMoyenne: { $avg: "$reviews.avg" } } },
  { $sort: { noteMoyenne: -1 } }
]).forEach(printjson);

titre("Q7 - Recherche plein texte");
db.products.find({ $text: { $search: "processeur jeu" } }, { _id: 0, name: 1, score: { $meta: "textScore" } })
  .sort({ score: { $meta: "textScore" } }).forEach(printjson);

titre("Q8 - Polymorphisme : produits v1 et v2 lus ensemble");
db.products.find({}, { _id: 0, name: 1, type: 1, schemaVersion: 1 }).forEach(printjson);
