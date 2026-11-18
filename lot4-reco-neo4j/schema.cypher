// =====================================================================
// Lot 4 — Recommandations produits (Neo4j 5)
// Exécution : cypher-shell -f schema.cypher   (ou copier dans Neo4j Browser)
// =====================================================================

// Remise à zéro (jeu de démonstration uniquement)
MATCH (n) DETACH DELETE n;

// ---------------------------------------------------------------------
// 1. Contraintes d'unicité (créent aussi les index de recherche)
// ---------------------------------------------------------------------
CREATE CONSTRAINT product_sku   IF NOT EXISTS FOR (p:Product)  REQUIRE p.sku  IS UNIQUE;
CREATE CONSTRAINT category_name IF NOT EXISTS FOR (c:Category) REQUIRE c.name IS UNIQUE;
CREATE CONSTRAINT brand_name    IF NOT EXISTS FOR (b:Brand)    REQUIRE b.name IS UNIQUE;
CREATE CONSTRAINT range_name    IF NOT EXISTS FOR (r:Range)    REQUIRE r.name IS UNIQUE;
CREATE CONSTRAINT standard_code IF NOT EXISTS FOR (s:Standard) REQUIRE s.code IS UNIQUE;
CREATE CONSTRAINT customer_id   IF NOT EXISTS FOR (c:Customer) REQUIRE c.id   IS UNIQUE;
CREATE CONSTRAINT order_id      IF NOT EXISTS FOR (o:Order)    REQUIRE o.id   IS UNIQUE;

// ---------------------------------------------------------------------
// 2. Catégories, marques, gammes, standards techniques
// ---------------------------------------------------------------------
UNWIND ['Processeurs', 'Cartes mères', 'Mémoire', 'Cartes graphiques', 'Boîtiers'] AS nom
CREATE (:Category {name: nom});

UNWIND ['AMD', 'Intel', 'ASUS', 'MSI', 'Gigabyte', 'ASRock', 'Corsair', 'NVIDIA', 'NZXT', 'Cooler Master'] AS nom
CREATE (:Brand {name: nom});

UNWIND ['Ryzen 7', 'Ryzen 9', 'Core Ultra 7', 'TUF Gaming', 'GeForce RTX 50'] AS nom
CREATE (:Range {name: nom});

// Standard = norme partagée entre produits compatibles (socket, mémoire, format)
UNWIND [
  {code: 'AM5',     kind: 'socket'},
  {code: 'LGA1851', kind: 'socket'},
  {code: 'DDR5',    kind: 'memoire'},
  {code: 'ATX',     kind: 'format'},
  {code: 'mATX',    kind: 'format'},
  {code: 'ITX',     kind: 'format'}
] AS s
CREATE (:Standard {code: s.code, kind: s.kind});

// ---------------------------------------------------------------------
// 3. Produits (seulement les propriétés utiles au graphe ;
//    la fiche complète reste dans MongoDB, reliée par le sku)
// ---------------------------------------------------------------------
UNWIND [
  {sku: 'AMD-R7-7800X3D',   name: 'AMD Ryzen 7 7800X3D',        price: 389.95, cat: 'Processeurs',       brand: 'AMD',           range: 'Ryzen 7'},
  {sku: 'AMD-R9-9950X',     name: 'AMD Ryzen 9 9950X',          price: 599.95, cat: 'Processeurs',       brand: 'AMD',           range: 'Ryzen 9'},
  {sku: 'INTEL-CU7-265K',   name: 'Intel Core Ultra 7 265K',    price: 329.95, cat: 'Processeurs',       brand: 'Intel',         range: 'Core Ultra 7'},
  {sku: 'ASUS-B650-PLUS',   name: 'ASUS TUF B650-Plus',         price: 189.95, cat: 'Cartes mères',      brand: 'ASUS',          range: 'TUF Gaming'},
  {sku: 'MSI-B650M-A',      name: 'MSI B650M Gaming Plus',      price: 149.95, cat: 'Cartes mères',      brand: 'MSI',           range: null},
  {sku: 'ASROCK-B650I',     name: 'ASRock B650I Lightning',     price: 199.95, cat: 'Cartes mères',      brand: 'ASRock',        range: null},
  {sku: 'GIGA-Z890-EAGLE',  name: 'Gigabyte Z890 Eagle',        price: 239.95, cat: 'Cartes mères',      brand: 'Gigabyte',      range: null},
  {sku: 'CORSAIR-DDR5-32',  name: 'Corsair Vengeance DDR5 32 Go', price: 64.90, cat: 'Mémoire',          brand: 'Corsair',       range: null},
  {sku: 'NVIDIA-RTX-5070',  name: 'NVIDIA GeForce RTX 5070',    price: 649.95, cat: 'Cartes graphiques', brand: 'NVIDIA',        range: 'GeForce RTX 50'},
  {sku: 'NVIDIA-RTX-5090',  name: 'NVIDIA GeForce RTX 5090',    price: 2399.95, cat: 'Cartes graphiques', brand: 'NVIDIA',       range: 'GeForce RTX 50'},
  {sku: 'NZXT-H5-FLOW',     name: 'NZXT H5 Flow',               price: 99.95,  cat: 'Boîtiers',          brand: 'NZXT',          range: null},
  {sku: 'CM-NR200P',        name: 'Cooler Master NR200P',       price: 109.95, cat: 'Boîtiers',          brand: 'Cooler Master', range: null}
] AS p
MATCH (c:Category {name: p.cat}), (b:Brand {name: p.brand})
CREATE (prod:Product {sku: p.sku, name: p.name, price: p.price})
CREATE (prod)-[:IN_CATEGORY]->(c)
CREATE (prod)-[:MADE_BY]->(b)
WITH prod, p WHERE p.range IS NOT NULL
MATCH (r:Range {name: p.range})
CREATE (prod)-[:IN_RANGE]->(r);

// Propriétés physiques utiles à la compatibilité (longueur des GPU, place dans les boîtiers)
MATCH (p:Product {sku: 'NVIDIA-RTX-5070'}) SET p.lengthMm = 300;
MATCH (p:Product {sku: 'NVIDIA-RTX-5090'}) SET p.lengthMm = 357;
MATCH (p:Product {sku: 'NZXT-H5-FLOW'})    SET p.maxGpuLengthMm = 365;
MATCH (p:Product {sku: 'CM-NR200P'})       SET p.maxGpuLengthMm = 336;

// ---------------------------------------------------------------------
// 4. Compatibilité via les standards
//    USES    : le produit EST de ce standard (CPU AM5, carte mère ATX…)
//    ACCEPTS : le produit ACCUEILLE ce standard (boîtier qui accepte l'ATX…)
// ---------------------------------------------------------------------
UNWIND [
  ['AMD-R7-7800X3D',  'AM5'], ['AMD-R9-9950X', 'AM5'], ['INTEL-CU7-265K', 'LGA1851'],
  ['ASUS-B650-PLUS',  'AM5'], ['ASUS-B650-PLUS', 'ATX'],  ['ASUS-B650-PLUS', 'DDR5'],
  ['MSI-B650M-A',     'AM5'], ['MSI-B650M-A',   'mATX'], ['MSI-B650M-A',   'DDR5'],
  ['ASROCK-B650I',    'AM5'], ['ASROCK-B650I',  'ITX'],  ['ASROCK-B650I',  'DDR5'],
  ['GIGA-Z890-EAGLE', 'LGA1851'], ['GIGA-Z890-EAGLE', 'ATX'], ['GIGA-Z890-EAGLE', 'DDR5'],
  ['CORSAIR-DDR5-32', 'DDR5']
] AS u
MATCH (p:Product {sku: u[0]}), (s:Standard {code: u[1]})
CREATE (p)-[:USES]->(s);

UNWIND [
  ['NZXT-H5-FLOW', 'ATX'], ['NZXT-H5-FLOW', 'mATX'], ['NZXT-H5-FLOW', 'ITX'],
  ['CM-NR200P',    'ITX']
] AS a
MATCH (p:Product {sku: a[0]}), (s:Standard {code: a[1]})
CREATE (p)-[:ACCEPTS]->(s);

// ---------------------------------------------------------------------
// 5. Clients et commandes (alimentés depuis le flux des commandes)
// ---------------------------------------------------------------------
UNWIND ['U-42', 'U-77', 'U-108', 'U-215', 'U-310'] AS id
CREATE (:Customer {id: id});

UNWIND [
  {id: 'CMD-1', customer: 'U-77',  date: date('2026-10-02'), items: ['AMD-R7-7800X3D', 'ASUS-B650-PLUS', 'CORSAIR-DDR5-32']},
  {id: 'CMD-2', customer: 'U-108', date: date('2026-10-15'), items: ['AMD-R7-7800X3D', 'ASUS-B650-PLUS', 'NVIDIA-RTX-5070']},
  {id: 'CMD-3', customer: 'U-215', date: date('2026-10-28'), items: ['AMD-R7-7800X3D', 'MSI-B650M-A', 'CORSAIR-DDR5-32']},
  {id: 'CMD-4', customer: 'U-310', date: date('2026-11-03'), items: ['AMD-R9-9950X', 'ASUS-B650-PLUS', 'NVIDIA-RTX-5090']},
  {id: 'CMD-5', customer: 'U-77',  date: date('2026-11-10'), items: ['NVIDIA-RTX-5070', 'NZXT-H5-FLOW']},
  {id: 'CMD-6', customer: 'U-42',  date: date('2026-11-17'), items: ['AMD-R7-7800X3D']}
] AS o
MATCH (c:Customer {id: o.customer})
CREATE (c)-[:PLACED]->(ord:Order {id: o.id, date: o.date})
WITH ord, o
UNWIND o.items AS sku
MATCH (p:Product {sku: sku})
CREATE (ord)-[:CONTAINS {quantity: 1}]->(p);

// ---------------------------------------------------------------------
// 6. Relation matérialisée BOUGHT_WITH : calcul initial
//    Évite de reparcourir toutes les commandes à chaque affichage.
//    En production, elle est ensuite mise à jour à chaque commande ingérée
//    (requetes.cypher, Q7). Ce calcul complet ne sert qu'à la construction
//    initiale et à une réconciliation hebdomadaire hors heures de pointe.
// ---------------------------------------------------------------------
MATCH (a:Product)<-[:CONTAINS]-(o:Order)-[:CONTAINS]->(b:Product)
WHERE a.sku < b.sku
WITH a, b, count(DISTINCT o) AS n
MERGE (a)-[r:BOUGHT_WITH]-(b)
SET r.count = n, r.updatedAt = date();
