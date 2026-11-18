-- =====================================================================
-- Lot 3 : Logs et Analytics (Cassandra)
-- Exécution : cqlsh -f schema.cql
-- =====================================================================

-- En production : 3 réplicas par datacenter.
--   WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3};
-- Pour un test local sur un seul nœud :
CREATE KEYSPACE IF NOT EXISTS ldlc_analytics
  WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1};

USE ldlc_analytics;

-- Convention : toutes les dates (day, hour, month) sont calculées en UTC,
-- comme event_time. Le changement d'heure été/hiver ne décale donc aucune partition.

-- ---------------------------------------------------------------------
-- T1. Journal brut de tous les événements (source de vérité, rejouable)
-- Partition = 1 heure d'un jour, découpée en 8 buckets
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS events_by_hour (
    day          date,           -- jour UTC
    hour         tinyint,        -- heure UTC (0-23)
    bucket       tinyint,        -- hash(session_id) % 8 : répartit la charge
    event_time   timestamp,
    event_id     timeuuid,       -- unicité si deux événements ont le même instant
    event_type   text,           -- page_view | product_view | click | search | add_to_cart
    visitor_id   text,
    session_id   text,
    sku          text,
    page         text,
    search_query text,
    device       text,
    PRIMARY KEY ((day, hour, bucket), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC, event_id DESC)
  AND default_time_to_live = 34128000          -- 395 jours (13 mois)
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_unit': 'DAYS',
                    'compaction_window_size': 14}; -- 395 / 14 ≈ 29 fenêtres (cible : 20 à 50)

-- ---------------------------------------------------------------------
-- T2. Parcours d'un visiteur (support client, personnalisation)
-- Partition = 1 visiteur sur 1 mois
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS events_by_visitor (
    visitor_id   text,
    month        text,           -- '2026-11' (mois UTC)
    event_time   timestamp,
    event_id     timeuuid,
    event_type   text,
    sku          text,
    page         text,
    search_query text,
    PRIMARY KEY ((visitor_id, month), event_time, event_id)
) WITH CLUSTERING ORDER BY (event_time DESC, event_id DESC)
  AND default_time_to_live = 34128000
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_unit': 'DAYS',
                    'compaction_window_size': 14}; -- ≈ 29 fenêtres sur 13 mois

-- ---------------------------------------------------------------------
-- T3. « Combien de vues pour ce produit aujourd'hui ? »
-- Compteur, partition = 1 produit, une ligne par jour
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS product_views_by_day (
    sku    text,
    day    date,
    views  counter,
    PRIMARY KEY ((sku), day)
) WITH CLUSTERING ORDER BY (day DESC);

-- ---------------------------------------------------------------------
-- T4. Vues de tous les produits pour un jour (base du classement)
-- Compteur, partition = 1 jour x 1 bucket (environ 3 400 lignes)
-- Sans bucket, toutes les vues de la journée frapperaient UNE partition,
-- donc les 3 mêmes nœuds : c'est le hot spot évité pour T1.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS daily_views_by_product (
    day    date,
    bucket tinyint,                -- hash(sku) % 8 : un produit est toujours dans le même bucket
    sku    text,
    views  counter,
    PRIMARY KEY ((day, bucket), sku)
);

-- ---------------------------------------------------------------------
-- T5. « Top produits consultés cette semaine » (pré-calculé)
-- Alimentée chaque heure par un job d'agrégation à partir de T4
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS top_products_by_period (
    period_type  text,           -- 'day' | 'week' | 'month' | 'search_day'
    period       text,           -- '2026-11-17' | '2026-W47' | '2026-11'
    rank         int,
    sku          text,           -- pour 'search_day' : le terme de recherche
    name         text,
    views        bigint,
    computed_at  timestamp,
    PRIMARY KEY ((period_type, period), rank)
) WITH CLUSTERING ORDER BY (rank ASC);

-- ---------------------------------------------------------------------
-- T6. Termes de recherche tapés par jour
-- Même découpage que T4 : partition = 1 jour x 1 bucket.
-- Les termes sont normalisés avant écriture (minuscules, espaces, accents).
-- Le classement des recherches est pré-calculé par le job horaire dans
-- top_products_by_period (period_type = 'search_day').
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS search_terms_by_day (
    day      date,
    bucket   tinyint,              -- hash(term) % 8
    term     text,
    searches counter,
    PRIMARY KEY ((day, bucket), term)
);

-- ---------------------------------------------------------------------
-- T7. « Historique des commandes d'un client »
-- Partition = 1 client, commandes de la plus récente à la plus ancienne
-- Écrite de façon SYNCHRONE par l'API au paiement (LOCAL_QUORUM), et non
-- via Kafka : la commande est confirmée au client seulement une fois écrite.
-- ---------------------------------------------------------------------
CREATE TYPE IF NOT EXISTS order_item (
    sku        text,
    name       text,
    quantity   int,
    unit_price decimal
);

CREATE TABLE IF NOT EXISTS orders_by_customer (
    customer_id      text,
    order_date       timestamp,
    order_id         text,
    status           text,       -- payee | expediee | livree | retournee
    total            decimal,
    shipping_city    text,
    items            list<frozen<order_item>>,
    PRIMARY KEY ((customer_id), order_date, order_id)
) WITH CLUSTERING ORDER BY (order_date DESC, order_id ASC);
-- Pas de TTL : conservation légale (10 ans pour les pièces comptables)
