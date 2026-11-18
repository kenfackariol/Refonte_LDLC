-- =====================================================================
-- Lot 3 — Données d'exemple et requêtes types
-- Exécution : cqlsh -f schema.cql  puis  cqlsh -f requetes.cql
-- Convention : day, hour et month sont calculés en UTC, comme event_time.
-- =====================================================================
USE ldlc_analytics;

-- =====================================================================
-- ÉCRITURES : un clic sur une fiche produit
-- Le consommateur Kafka écrit dans plusieurs tables (dénormalisation),
-- en requêtes asynchrones parallèles, avec consistency LOCAL_ONE.
-- Clic à 21:04 heure de Paris = 20:04 UTC : partition hour = 20.
-- =====================================================================
INSERT INTO events_by_hour (day, hour, bucket, event_time, event_id, event_type,
                            visitor_id, session_id, sku, page, device)
VALUES ('2026-11-17', 20, 3, '2026-11-17 20:04:12+0000', now(), 'product_view',
        'U-42', 's-7f3a9c', 'AMD-R7-7800X3D', '/fiche/amd-ryzen-7-7800x3d', 'desktop');

INSERT INTO events_by_visitor (visitor_id, month, event_time, event_id, event_type, sku, page)
VALUES ('U-42', '2026-11', '2026-11-17 20:04:12+0000', now(), 'product_view',
        'AMD-R7-7800X3D', '/fiche/amd-ryzen-7-7800x3d');

-- Compteurs. bucket = hash(sku) % 8, calculé par l'application
-- (valeurs fixées à la main dans ce jeu d'exemple : AMD-R7-7800X3D -> 3).
-- En production, le consommateur Kafka pré-agrège les incréments sur
-- quelques secondes : une écriture « views + 37 » au lieu de 37 « views + 1 ».
UPDATE product_views_by_day  SET views = views + 1 WHERE sku = 'AMD-R7-7800X3D' AND day = '2026-11-17';
UPDATE daily_views_by_product SET views = views + 1 WHERE day = '2026-11-17' AND bucket = 3 AND sku = 'AMD-R7-7800X3D';

-- Une recherche (terme normalisé ; bucket = hash(term) % 8 -> 'ryzen 7' : 4)
INSERT INTO events_by_hour (day, hour, bucket, event_time, event_id, event_type,
                            visitor_id, session_id, search_query, page, device)
VALUES ('2026-11-17', 20, 3, '2026-11-17 20:03:40+0000', now(), 'search',
        'U-42', 's-7f3a9c', 'ryzen 7', '/recherche?q=ryzen+7', 'desktop');
INSERT INTO events_by_visitor (visitor_id, month, event_time, event_id, event_type, search_query, page)
VALUES ('U-42', '2026-11', '2026-11-17 20:03:40+0000', now(), 'search', 'ryzen 7', '/recherche?q=ryzen+7');
UPDATE search_terms_by_day SET searches = searches + 1 WHERE day = '2026-11-17' AND bucket = 4 AND term = 'ryzen 7';

-- Volume simulé pour les autres produits et jours
-- (buckets d'exemple : RTX-5070 -> 5, ASUS-B650-PLUS -> 1, LOGI-GPRO-X2 -> 6)
UPDATE product_views_by_day  SET views = views + 1840 WHERE sku = 'AMD-R7-7800X3D' AND day = '2026-11-16';
UPDATE product_views_by_day  SET views = views + 2210 WHERE sku = 'AMD-R7-7800X3D' AND day = '2026-11-15';
UPDATE product_views_by_day  SET views = views + 1203 WHERE sku = 'AMD-R7-7800X3D' AND day = '2026-11-17';

UPDATE daily_views_by_product SET views = views + 1203 WHERE day = '2026-11-17' AND bucket = 3 AND sku = 'AMD-R7-7800X3D';
UPDATE daily_views_by_product SET views = views + 2950 WHERE day = '2026-11-17' AND bucket = 5 AND sku = 'NVIDIA-RTX-5070';
UPDATE daily_views_by_product SET views = views +  640 WHERE day = '2026-11-17' AND bucket = 1 AND sku = 'ASUS-B650-PLUS';
UPDATE daily_views_by_product SET views = views +  310 WHERE day = '2026-11-17' AND bucket = 6 AND sku = 'LOGI-GPRO-X2';

UPDATE search_terms_by_day SET searches = searches + 412 WHERE day = '2026-11-17' AND bucket = 2 AND term = 'rtx 5070';
UPDATE search_terms_by_day SET searches = searches + 198 WHERE day = '2026-11-17' AND bucket = 4 AND term = 'ryzen 7';

-- Résultat du job d'agrégation horaire (lit les 7 jours x 8 buckets de
-- daily_views_by_product, additionne, trie et écrit le top 100)
INSERT INTO top_products_by_period (period_type, period, rank, sku, name, views, computed_at)
VALUES ('week', '2026-W47', 1, 'NVIDIA-RTX-5070', 'NVIDIA GeForce RTX 5070', 18420, toTimestamp(now()));
INSERT INTO top_products_by_period (period_type, period, rank, sku, name, views, computed_at)
VALUES ('week', '2026-W47', 2, 'AMD-R7-7800X3D', 'AMD Ryzen 7 7800X3D', 11367, toTimestamp(now()));
INSERT INTO top_products_by_period (period_type, period, rank, sku, name, views, computed_at)
VALUES ('week', '2026-W47', 3, 'ASUS-B650-PLUS', 'ASUS TUF B650-Plus', 4105, toTimestamp(now()));

-- Même job pour les recherches du jour (le terme est rangé dans la colonne sku)
INSERT INTO top_products_by_period (period_type, period, rank, sku, views, computed_at)
VALUES ('search_day', '2026-11-17', 1, 'rtx 5070', 412, toTimestamp(now()));
INSERT INTO top_products_by_period (period_type, period, rank, sku, views, computed_at)
VALUES ('search_day', '2026-11-17', 2, 'ryzen 7', 199, toTimestamp(now()));

-- Commandes : écrites de façon SYNCHRONE par l'API au paiement, en LOCAL_QUORUM,
-- après la réservation du stock dans MongoDB. Un INSERT rejoué avec le même
-- (customer_id, order_date, order_id) écrase la même ligne : l'écriture est idempotente.
INSERT INTO orders_by_customer (customer_id, order_date, order_id, status, total, shipping_city, items)
VALUES ('U-42', '2026-11-17 20:20:00+0000', 'CMD-2026-0917345', 'payee', 579.90, 'Lyon',
        [ {sku: 'AMD-R7-7800X3D', name: 'AMD Ryzen 7 7800X3D', quantity: 1, unit_price: 389.95},
          {sku: 'ASUS-B650-PLUS', name: 'ASUS TUF B650-Plus',  quantity: 1, unit_price: 189.95} ]);

INSERT INTO orders_by_customer (customer_id, order_date, order_id, status, total, shipping_city, items)
VALUES ('U-42', '2026-03-02 13:11:00+0000', 'CMD-2026-0210877', 'livree', 159.95, 'Lyon',
        [ {sku: 'LOGI-GPRO-X2', name: 'Logitech G Pro X Superlight 2', quantity: 1, unit_price: 159.95} ]);

INSERT INTO orders_by_customer (customer_id, order_date, order_id, status, total, shipping_city, items)
VALUES ('U-42', '2025-12-20 09:45:00+0000', 'CMD-2025-1198402', 'livree', 64.90, 'Lyon',
        [ {sku: 'CORSAIR-DDR5-32', name: 'Corsair Vengeance DDR5 32 Go', quantity: 1, unit_price: 64.90} ]);


-- =====================================================================
-- LECTURES : requêtes types
-- =====================================================================

-- Q1. Combien de vues pour ce produit aujourd'hui ?  (1 partition)
SELECT views FROM product_views_by_day
 WHERE sku = 'AMD-R7-7800X3D' AND day = '2026-11-17';

-- Q1 bis. Évolution des vues sur les 7 derniers jours (même partition, tranche de clustering)
SELECT day, views FROM product_views_by_day
 WHERE sku = 'AMD-R7-7800X3D' AND day >= '2026-11-11' AND day <= '2026-11-17';

-- Q2. Top produits consultés cette semaine (pré-calculé, 1 partition)
SELECT rank, sku, name, views FROM top_products_by_period
 WHERE period_type = 'week' AND period = '2026-W47' LIMIT 10;

-- Q3. Historique des commandes d'un client (la plus récente d'abord, 1 partition)
SELECT order_date, order_id, status, total, items FROM orders_by_customer
 WHERE customer_id = 'U-42';

-- Q3 bis. Commandes d'un client sur l'année 2026
SELECT order_date, order_id, total FROM orders_by_customer
 WHERE customer_id = 'U-42' AND order_date >= '2026-01-01' AND order_date < '2027-01-01';

-- Q4. Parcours d'un visiteur ce mois-ci (1 partition)
SELECT event_time, event_type, sku, search_query, page FROM events_by_visitor
 WHERE visitor_id = 'U-42' AND month = '2026-11' LIMIT 50;

-- Q5. Événements bruts d'un créneau horaire UTC (un bucket ; l'application interroge les 8 en parallèle)
SELECT event_time, event_type, visitor_id, sku, search_query FROM events_by_hour
 WHERE day = '2026-11-17' AND hour = 20 AND bucket = 3;

-- Q6. Recherches les plus tapées du jour (pré-calculé, 1 partition)
SELECT rank, sku AS terme, views AS recherches FROM top_products_by_period
 WHERE period_type = 'search_day' AND period = '2026-11-17' LIMIT 10;

-- Q7. Vues de tous les produits pour un jour (entrée du job de classement).
--     Le job lit les 8 buckets ; en production, 8 requêtes parallèles
--     plutôt qu'un IN, pour ne pas surcharger le coordinateur.
SELECT bucket, sku, views FROM daily_views_by_product
 WHERE day = '2026-11-17' AND bucket IN (0, 1, 2, 3, 4, 5, 6, 7);

-- Mise à jour du statut d'une commande : la clé primaire complète est requise
UPDATE orders_by_customer SET status = 'expediee'
 WHERE customer_id = 'U-42' AND order_date = '2026-11-17 20:20:00+0000' AND order_id = 'CMD-2026-0917345';
