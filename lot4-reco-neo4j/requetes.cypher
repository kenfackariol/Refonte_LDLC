// =====================================================================
// Lot 4 : Requêtes types de recommandation (Neo4j 5)
// Prérequis : avoir exécuté schema.cypher
// =====================================================================

// ---------------------------------------------------------------------
// Q1. « Les clients qui ont acheté ce processeur ont aussi acheté
//      cette carte mère » (parcours en temps réel via les commandes)
// ---------------------------------------------------------------------
MATCH (p:Product {sku: 'AMD-R7-7800X3D'})<-[:CONTAINS]-(o:Order)-[:CONTAINS]->(autre:Product)
      -[:IN_CATEGORY]->(:Category {name: 'Cartes mères'})
RETURN autre.sku AS sku, autre.name AS produit, count(DISTINCT o) AS commandes_communes
ORDER BY commandes_communes DESC
LIMIT 5;
// Attendu : ASUS-B650-PLUS (2), MSI-B650M-A (1)

// ---------------------------------------------------------------------
// Q1 bis. Même question via la relation matérialisée (1 seul saut)
//         -> version utilisée en production sur la fiche produit
// ---------------------------------------------------------------------
MATCH (p:Product {sku: 'AMD-R7-7800X3D'})-[r:BOUGHT_WITH]-(autre:Product)
RETURN autre.sku AS sku, autre.name AS produit, r.count AS achats_communs
ORDER BY achats_communs DESC
LIMIT 5;

// ---------------------------------------------------------------------
// Q2. « Produits compatibles avec ce boîtier »
//     Cartes mères dont le format est accepté + GPU qui rentrent en longueur
// ---------------------------------------------------------------------
MATCH (boitier:Product {sku: 'CM-NR200P'})-[:ACCEPTS]->(f:Standard {kind: 'format'})<-[:USES]-(cm:Product)
RETURN 'Carte mère' AS type, cm.sku AS sku, cm.name AS produit, f.code AS critere
UNION
MATCH (boitier:Product {sku: 'CM-NR200P'}), (gpu:Product)-[:IN_CATEGORY]->(:Category {name: 'Cartes graphiques'})
WHERE gpu.lengthMm <= boitier.maxGpuLengthMm
RETURN 'Carte graphique' AS type, gpu.sku AS sku, gpu.name AS produit,
       toString(gpu.lengthMm) + ' mm <= ' + toString(boitier.maxGpuLengthMm) + ' mm' AS critere;
// Attendu : ASRock B650I (ITX) et RTX 5070 (300 mm). La RTX 5090 (357 mm) est exclue.

// ---------------------------------------------------------------------
// Q3. Configurateur : à partir d'un CPU, parcourir la chaîne de compatibilité
//     CPU -[socket]- carte mère -[mémoire]- RAM -> et boîtiers acceptant le format
//     (parcours de relations sur plusieurs sauts)
// ---------------------------------------------------------------------
MATCH (cpu:Product {sku: 'AMD-R7-7800X3D'})-[:USES]->(socket:Standard {kind: 'socket'})
      <-[:USES]-(cm:Product)-[:IN_CATEGORY]->(:Category {name: 'Cartes mères'})
MATCH (cm)-[:USES]->(mem:Standard {kind: 'memoire'})<-[:USES]-(ram:Product)
      -[:IN_CATEGORY]->(:Category {name: 'Mémoire'})
MATCH (cm)-[:USES]->(fmt:Standard {kind: 'format'})<-[:ACCEPTS]-(boitier:Product)
RETURN cm.name AS carte_mere, fmt.code AS format,
       collect(DISTINCT ram.name)     AS memoires_compatibles,
       collect(DISTINCT boitier.name) AS boitiers_compatibles
ORDER BY carte_mere;

// ---------------------------------------------------------------------
// Q4. Produits similaires : même catégorie, même gamme ou même marque,
//     prix à ±30 %, avec un score de similarité.
//     Seuls les standards techniques (socket, mémoire) comptent : partager
//     le format physique (ATX) ne rend pas deux cartes mères similaires.
// ---------------------------------------------------------------------
MATCH (p:Product {sku: 'ASUS-B650-PLUS'})-[:IN_CATEGORY]->(c:Category)<-[:IN_CATEGORY]-(sim:Product)
WHERE sim <> p AND abs(sim.price - p.price) <= p.price * 0.3
OPTIONAL MATCH (p)-[:IN_RANGE]->(r:Range)<-[:IN_RANGE]-(sim)
OPTIONAL MATCH (p)-[:MADE_BY]->(b:Brand)<-[:MADE_BY]-(sim)
OPTIONAL MATCH (p)-[:USES]->(s:Standard)<-[:USES]-(sim)
WHERE s.kind IN ['socket', 'memoire']
WITH sim, r, b, count(DISTINCT s) AS standards_communs
WITH sim, standards_communs,
     (CASE WHEN r IS NULL THEN 0 ELSE 3 END) +
     (CASE WHEN b IS NULL THEN 0 ELSE 2 END) +
     standards_communs AS score
RETURN sim.sku AS sku, sim.name AS produit, sim.price AS prix, standards_communs, score
ORDER BY score DESC, prix ASC
LIMIT 5;
// Attendu : MSI B650M (AM5 + DDR5 : score 2), ASRock B650I (AM5 + DDR5 : score 2),
//           Gigabyte Z890 (DDR5 seulement : score 1 ; son format ATX commun n'est pas compté)

// ---------------------------------------------------------------------
// Q5. Recommandations personnalisées (filtrage collaboratif) :
//     ce qu'ont acheté les clients ayant des achats en commun avec U-42,
//     sauf ce que U-42 possède déjà
// ---------------------------------------------------------------------
MATCH (moi:Customer {id: 'U-42'})-[:PLACED]->(:Order)-[:CONTAINS]->(p:Product)
      <-[:CONTAINS]-(:Order)<-[:PLACED]-(autre:Customer)
WHERE autre <> moi
MATCH (autre)-[:PLACED]->(:Order)-[:CONTAINS]->(reco:Product)
WHERE NOT EXISTS { (moi)-[:PLACED]->(:Order)-[:CONTAINS]->(reco) }
RETURN reco.sku AS sku, reco.name AS produit, count(DISTINCT autre) AS clients_similaires
ORDER BY clients_similaires DESC
LIMIT 5;
// Attendu : ASUS-B650-PLUS (2), CORSAIR-DDR5-32 (2), NVIDIA-RTX-5070 (2), ...

// ---------------------------------------------------------------------
// Q6. Analyse : produits « pivots » les plus souvent achetés avec d'autres
//     (utile au merchandising pour composer des packs)
// ---------------------------------------------------------------------
MATCH (p:Product)-[r:BOUGHT_WITH]-()
RETURN p.name AS produit, count(r) AS nb_produits_associes, sum(r.count) AS total_achats_communs
ORDER BY total_achats_communs DESC
LIMIT 5;

// ---------------------------------------------------------------------
// Q7. Ingestion d'une nouvelle commande (consommateur Kafka)
//     + mise à jour INCRÉMENTALE de BOUGHT_WITH.
//     Une commande contient quelques produits, donc quelques paires :
//     le coût ne dépend pas du nombre total de commandes (pas de supernœud).
//     Idempotent : si Kafka rejoue l'événement, la commande existe déjà
//     et rien n'est recompté.
// ---------------------------------------------------------------------
WITH {id: 'CMD-7', customer: 'U-42', date: date('2026-11-18'),
      items: ['AMD-R7-7800X3D', 'ASUS-B650-PLUS']} AS o
MERGE (c:Customer {id: o.customer})
WITH c, o
WHERE NOT EXISTS { MATCH (:Order {id: o.id}) }
CREATE (c)-[:PLACED]->(ord:Order {id: o.id, date: o.date})
WITH ord, o
UNWIND o.items AS sku
MATCH (p:Product {sku: sku})
CREATE (ord)-[:CONTAINS {quantity: 1}]->(p)
WITH collect(p) AS produits
UNWIND produits AS pa
UNWIND produits AS pb
WITH pa, pb WHERE pa.sku < pb.sku
MERGE (pa)-[r:BOUGHT_WITH]-(pb)
  ON CREATE SET r.count = 1, r.updatedAt = date()
  ON MATCH  SET r.count = r.count + 1, r.updatedAt = date()
RETURN pa.sku, pb.sku, r.count AS achats_communs;
// Attendu : AMD-R7-7800X3D / ASUS-B650-PLUS passe de 2 à 3.
// Relancer la requête ne renvoie rien : la commande CMD-7 existe déjà.
