-- =============================================================================
--  BPCL FUEL DISTRIBUTION & OUTLET SEGMENTATION -- SQL-NATIVE ANALYTICS PIPELINE
-- =============================================================================
--  Author   : Shubham M.
--  Engine   : PostgreSQL 13+
--  Purpose  : End-to-end SQL analytics pipeline that replaces a Python/ML
--             clustering workflow with a fully SQL-native approach, built the
--             way a Business Analyst would deliver it inside a data warehouse:
--             raw tables -> feature views -> segmentation -> KPIs -> executive
--             reporting -- no external tooling required to reproduce a single
--             number in this file.
--
--  HOW TO RUN
--  ----------
--    createdb bpcl_analytics
--    psql -d bpcl_analytics -f BPCL_Fuel_Distribution_SQL_Analytics.sql
--
--  FILE MAP
--  --------
--    SECTION 0  Business Problem & Objective
--    SECTION 1  Schema Design
--    SECTION 2  Synthetic Data Generation (transparently disclosed, see notes)
--    SECTION 3  Feature Engineering (views)
--    SECTION 4  Exploratory Data Analysis
--    SECTION 5  Outlet Segmentation -- percentile / business-rule driven (CORE)
--    SECTION 6  Segment-Level KPIs & Revenue Contribution
--    SECTION 7  Pareto (80/20) Revenue Concentration Analysis
--    SECTION 8  Statistical Anomaly Detection (Z-score)
--    SECTION 9  Supply-Chain Priority Scoring
--    SECTION 10 Executive Summary & Action Queue
--    SECTION 11 Business Recommendations (narrative, tied to query outputs)
--    APPENDIX A Optional bonus: pure-SQL K-Means approximation
-- =============================================================================


-- =============================================================================
-- SECTION 0 -- BUSINESS PROBLEM & OBJECTIVE
-- =============================================================================
--  Bharat Petroleum Corporation Limited (BPCL) supplies fuel to a wide network
--  of petrol pumps ("outlets") across urban, rural and highway locations.
--  Demand varies sharply by outlet due to differences in customer behaviour,
--  traffic patterns and logistics usage, which creates three recurring
--  business problems:
--    1. Fuel is not always distributed to where demand is highest
--    2. Inventory / replenishment planning is reactive rather than data-driven
--    3. High-value and at-risk outlets are not consistently identified
--
--  OBJECTIVE
--  ---------
--  Segment outlets by demand pattern, quantify how revenue is concentrated
--  across the network, flag outlets behaving abnormally, and produce a supply
--  priority score that can plug directly into a monthly allocation decision --
--  all reproducible with SQL alone.
-- =============================================================================


-- =============================================================================
-- SECTION 1 -- SCHEMA DESIGN
-- =============================================================================
-- Two tables: a slowly-changing outlet dimension and a monthly fact table.
-- This mirrors how BPCL's own data would actually be modelled in a warehouse
-- (star-schema style) rather than one flat spreadsheet-shaped table.

DROP VIEW IF EXISTS vw_action_queue CASCADE;
DROP VIEW IF EXISTS vw_executive_summary CASCADE;
DROP VIEW IF EXISTS vw_supply_priority CASCADE;
DROP VIEW IF EXISTS vw_outlet_anomalies CASCADE;
DROP VIEW IF EXISTS vw_pareto_revenue CASCADE;
DROP VIEW IF EXISTS vw_segment_kpis CASCADE;
DROP VIEW IF EXISTS vw_outlet_segments CASCADE;
DROP VIEW IF EXISTS vw_outlet_mom_trend CASCADE;
DROP VIEW IF EXISTS vw_outlet_profile CASCADE;
DROP VIEW IF EXISTS vw_outlet_monthly_features CASCADE;
DROP TABLE IF EXISTS monthly_fuel_sales CASCADE;
DROP TABLE IF EXISTS outlets CASCADE;
DROP FUNCTION IF EXISTS synth_normal(NUMERIC, NUMERIC);

CREATE TABLE outlets (
    outlet_id        SERIAL PRIMARY KEY,
    outlet_code      VARCHAR(20)  UNIQUE NOT NULL,
    location_type    VARCHAR(10)  NOT NULL CHECK (location_type IN ('Urban','Rural','Highway')),
    region           VARCHAR(50)  NOT NULL,
    outlet_type      VARCHAR(20)  NOT NULL CHECK (outlet_type IN ('Company Owned','Dealer Owned')),
    commissioned_on  DATE         NOT NULL
);

CREATE TABLE monthly_fuel_sales (
    sale_id          SERIAL PRIMARY KEY,
    outlet_id        INTEGER NOT NULL REFERENCES outlets(outlet_id) ON DELETE CASCADE,
    sales_month      DATE    NOT NULL,
    petrol_liters    NUMERIC(10,2) NOT NULL CHECK (petrol_liters   >= 0),
    diesel_liters    NUMERIC(10,2) NOT NULL CHECK (diesel_liters   >= 0),
    kerosene_liters  NUMERIC(10,2) NOT NULL CHECK (kerosene_liters >= 0),
    monthly_visits   INTEGER       NOT NULL CHECK (monthly_visits  >= 0),
    UNIQUE (outlet_id, sales_month)
);

CREATE INDEX idx_sales_outlet ON monthly_fuel_sales(outlet_id);
CREATE INDEX idx_sales_month  ON monthly_fuel_sales(sales_month);


-- =============================================================================
-- SECTION 2 -- SYNTHETIC DATA GENERATION
-- =============================================================================
--  NOTE ON DATA: BPCL's real transactional data is confidential and cannot be
--  used here. As disclosed transparently, the figures below are a synthetic
--  sample engineered to reflect realistic, well-documented industry patterns:
--  higher diesel demand on highways (freight/logistics), higher footfall and
--  transaction frequency in urban retail outlets, and lower but steadier
--  volumes in rural areas. Six trailing months of history are generated per
--  outlet so the pipeline can demonstrate trend analysis, not just a snapshot.
--
--  synth_normal() approximates a Gaussian draw using the classic
--  "sum of twelve uniforms" trick (Irwin-Hall approximation of the CLT), so
--  the synthetic figures look like real-world continuous measurements rather
--  than uniformly-random noise. Implemented in PL/pgSQL (rather than a plain
--  SQL function) so the NUMERIC return type is enforced reliably regardless
--  of PostgreSQL version -- note that PostgreSQL 16+ ships its own native
--  random_normal(), which this project intentionally avoids depending on so
--  the script runs unmodified on PostgreSQL 13-15 as well.
CREATE OR REPLACE FUNCTION synth_normal(p_mean NUMERIC, p_stddev NUMERIC)
RETURNS NUMERIC AS $$
DECLARE
    v_sum DOUBLE PRECISION;
BEGIN
    SELECT SUM(random()) INTO v_sum FROM generate_series(1, 12);
    RETURN p_mean + p_stddev * (v_sum - 6);
END;
$$ LANGUAGE plpgsql;

-- Fixed seed so every clone of this repo reproduces the same synthetic
-- dataset -- and therefore the same numbers quoted in the README.
SELECT setseed(0.42);

-- 2.1  Outlets: 150 outlets spread across location types and Indian regions
INSERT INTO outlets (outlet_code, location_type, region, outlet_type, commissioned_on)
SELECT
    'BPCL-' || LPAD(g::TEXT, 4, '0'),
    loc.location_type,
    reg.region,
    (CASE WHEN random() < 0.55 THEN 'Dealer Owned' ELSE 'Company Owned' END),
    (DATE '2010-01-01' + (random() * 5000)::INT)
FROM generate_series(1, 150) AS g
CROSS JOIN LATERAL (
    -- 30% Urban, 30% Rural, 40% Highway -- highway-heavy network, as BPCL's
    -- real distribution skews toward national/state highway corridors
    SELECT CASE
        WHEN g % 10 < 3 THEN 'Urban'
        WHEN g % 10 < 6 THEN 'Rural'
        ELSE 'Highway'
    END AS location_type
) loc
CROSS JOIN LATERAL (
    SELECT (ARRAY['Maharashtra','Gujarat','Karnataka','Tamil Nadu','Rajasthan',
                  'Uttar Pradesh','Madhya Pradesh','Telangana'])[1 + floor(random()*8)::INT] AS region
) reg;

-- 2.2  Monthly fuel sales: 6 trailing months per outlet, generated from
--      location-specific demand profiles (mean/stddev pairs below mirror the
--      domain assumptions used in the original Python analysis).
INSERT INTO monthly_fuel_sales (outlet_id, sales_month, petrol_liters, diesel_liters, kerosene_liters, monthly_visits)
SELECT
    o.outlet_id,
    (DATE_TRUNC('month', CURRENT_DATE) - (m || ' months')::INTERVAL)::DATE AS sales_month,
    GREATEST(0, ROUND(
        synth_normal(
            CASE o.location_type WHEN 'Urban' THEN 4000 WHEN 'Highway' THEN 2500 ELSE 1500 END,
            CASE o.location_type WHEN 'Urban' THEN 500  WHEN 'Highway' THEN 400  ELSE 300  END
        ), 2)) AS petrol_liters,
    GREATEST(0, ROUND(
        synth_normal(
            CASE o.location_type WHEN 'Urban' THEN 2000 WHEN 'Highway' THEN 5000 ELSE 1200 END,
            CASE o.location_type WHEN 'Urban' THEN 400  WHEN 'Highway' THEN 800  ELSE 300  END
        ), 2)) AS diesel_liters,
    GREATEST(0, ROUND(
        synth_normal(
            CASE o.location_type WHEN 'Urban' THEN 500  WHEN 'Highway' THEN 300  ELSE 800  END,
            CASE o.location_type WHEN 'Urban' THEN 100  WHEN 'Highway' THEN 80   ELSE 200  END
        ), 2)) AS kerosene_liters,
    GREATEST(0, ROUND(
        synth_normal(
            CASE o.location_type WHEN 'Urban' THEN 300  WHEN 'Highway' THEN 175 ELSE 140  END,
            30
        )))::INT AS monthly_visits
FROM outlets o
CROSS JOIN generate_series(0, 5) AS m;

-- 2.3  Inject a handful of deliberate anomalies so Section 8's z-score check
--      has something real to find (mirrors a supply disruption / data-entry
--      spike you would actually see in production data).
UPDATE monthly_fuel_sales
SET diesel_liters = diesel_liters * 2.6
WHERE outlet_id = (SELECT outlet_id FROM outlets WHERE location_type = 'Highway' ORDER BY outlet_id LIMIT 1)
  AND sales_month = (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month')::DATE;

UPDATE monthly_fuel_sales
SET petrol_liters = petrol_liters * 0.25, monthly_visits = (monthly_visits * 0.3)::INT
WHERE outlet_id = (SELECT outlet_id FROM outlets WHERE location_type = 'Urban' ORDER BY outlet_id DESC LIMIT 1)
  AND sales_month = (DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '2 months')::DATE;


-- =============================================================================
-- SECTION 3 -- FEATURE ENGINEERING (VIEWS)
-- =============================================================================
-- 3.1  Row-level engineered features, computed once and reused everywhere
--      downstream so business logic (e.g. the revenue formula) lives in a
--      single place instead of being copy-pasted across queries.
CREATE OR REPLACE VIEW vw_outlet_monthly_features AS
SELECT
    o.outlet_id,
    o.outlet_code,
    o.location_type,
    o.region,
    o.outlet_type,
    s.sales_month,
    s.petrol_liters,
    s.diesel_liters,
    s.kerosene_liters,
    s.monthly_visits,
    (s.petrol_liters + s.diesel_liters + s.kerosene_liters)                       AS total_fuel_liters,
    ROUND((s.petrol_liters + s.diesel_liters + s.kerosene_liters)
          / NULLIF(s.monthly_visits, 0), 2)                                      AS fuel_per_visit,
    ROUND(s.diesel_liters
          / NULLIF((s.petrol_liters + s.diesel_liters + s.kerosene_liters), 0), 4) AS diesel_ratio,
    -- Illustrative price assumptions (INR/liter): petrol 100, diesel 90, kerosene 70
    ROUND(s.petrol_liters * 100 + s.diesel_liters * 90 + s.kerosene_liters * 70, 2) AS estimated_revenue
FROM monthly_fuel_sales s
JOIN outlets o ON o.outlet_id = s.outlet_id;

-- 3.2  Outlet-level profile: trailing 6-month averages/totals, one row per
--      outlet. This is the table every downstream segmentation query reads.
CREATE OR REPLACE VIEW vw_outlet_profile AS
SELECT
    outlet_id,
    outlet_code,
    location_type,
    region,
    outlet_type,
    ROUND(AVG(total_fuel_liters), 2)     AS avg_total_fuel,
    ROUND(AVG(monthly_visits), 1)        AS avg_monthly_visits,
    ROUND(AVG(fuel_per_visit), 2)        AS avg_fuel_per_visit,
    ROUND(AVG(diesel_ratio), 4)          AS avg_diesel_ratio,
    ROUND(AVG(estimated_revenue), 2)     AS avg_monthly_revenue,
    ROUND(SUM(estimated_revenue), 2)     AS total_revenue_6mo,
    COUNT(*)                             AS months_observed
FROM vw_outlet_monthly_features
GROUP BY outlet_id, outlet_code, location_type, region, outlet_type;

-- 3.3  Month-over-month trend, using LAG() to avoid a self-join.
CREATE OR REPLACE VIEW vw_outlet_mom_trend AS
SELECT
    outlet_id,
    outlet_code,
    location_type,
    sales_month,
    total_fuel_liters,
    LAG(total_fuel_liters) OVER (PARTITION BY outlet_id ORDER BY sales_month) AS prev_month_fuel,
    ROUND(
        100.0 * (total_fuel_liters - LAG(total_fuel_liters) OVER (PARTITION BY outlet_id ORDER BY sales_month))
        / NULLIF(LAG(total_fuel_liters) OVER (PARTITION BY outlet_id ORDER BY sales_month), 0)
    , 2) AS mom_growth_pct
FROM vw_outlet_monthly_features;


-- =============================================================================
-- SECTION 4 -- EXPLORATORY DATA ANALYSIS
-- =============================================================================
-- 4.1  Demand profile by location type -- sets up the "why segment at all"
--      story: highway outlets are diesel-heavy and low-frequency, urban
--      outlets are high-frequency and petrol-heavy, rural outlets lag on both.
SELECT
    location_type,
    COUNT(DISTINCT outlet_id)                AS outlet_count,
    ROUND(AVG(avg_total_fuel), 0)            AS avg_monthly_fuel_liters,
    ROUND(AVG(avg_monthly_visits), 0)        AS avg_monthly_visits,
    ROUND(AVG(avg_diesel_ratio), 3)          AS avg_diesel_ratio,
    ROUND(AVG(avg_monthly_revenue), 0)       AS avg_monthly_revenue
FROM vw_outlet_profile
GROUP BY location_type
ORDER BY avg_monthly_revenue DESC;

-- 4.2  Spread/variability check (are outlets within a location type actually
--      homogeneous, or is more granular segmentation justified?)
SELECT
    location_type,
    ROUND(MIN(avg_total_fuel), 0)   AS min_fuel,
    ROUND(MAX(avg_total_fuel), 0)   AS max_fuel,
    ROUND(STDDEV(avg_total_fuel),0) AS stddev_fuel
FROM vw_outlet_profile
GROUP BY location_type;


-- =============================================================================
-- SECTION 5 -- OUTLET SEGMENTATION (PERCENTILE / BUSINESS-RULE DRIVEN) -- CORE
-- =============================================================================
--  Design choice: rather than a black-box clustering algorithm, this
--  segmentation uses NTILE() quartiles and PERCENT_RANK() on the two metrics
--  that actually drive supply decisions -- revenue and visit frequency -- plus
--  a diesel-share percentile to catch logistics-heavy outlets. Every outlet's
--  segment is fully explainable from its own numbers, which matters far more
--  in a business review than marginal gains in a similarity metric.
CREATE OR REPLACE VIEW vw_outlet_segments AS
WITH scored AS (
    SELECT
        p.*,
        NTILE(4)       OVER (ORDER BY avg_monthly_revenue)  AS revenue_quartile,
        NTILE(4)       OVER (ORDER BY avg_monthly_visits)   AS frequency_quartile,
        PERCENT_RANK() OVER (ORDER BY avg_monthly_revenue)  AS revenue_percentile,
        PERCENT_RANK() OVER (ORDER BY avg_diesel_ratio)     AS diesel_percentile
    FROM vw_outlet_profile p
)
SELECT
    *,
    CASE
        WHEN revenue_quartile = 4 AND diesel_percentile >= 0.75
            THEN 'Strategic High-Volume (Diesel/Logistics)'
        WHEN revenue_quartile = 4
            THEN 'Strategic High-Volume'
        WHEN frequency_quartile = 4 AND revenue_quartile <= 2
            THEN 'High-Frequency / Low-Ticket Retail'
        WHEN diesel_percentile >= 0.75 AND revenue_quartile IN (2, 3)
            THEN 'Diesel-Heavy Logistics Hub'
        WHEN revenue_quartile = 1 AND frequency_quartile = 1
            THEN 'Underperforming / Needs Review'
        ELSE 'Steady Mid-Tier Outlet'
    END AS business_segment
FROM scored;

-- 5.1  Sanity check: segment sizes and headline metrics
SELECT
    business_segment,
    COUNT(*)                            AS outlet_count,
    ROUND(AVG(avg_monthly_revenue), 0)  AS avg_monthly_revenue,
    ROUND(AVG(avg_diesel_ratio), 3)     AS avg_diesel_ratio,
    ROUND(AVG(avg_monthly_visits), 0)   AS avg_monthly_visits
FROM vw_outlet_segments
GROUP BY business_segment
ORDER BY avg_monthly_revenue DESC;


-- =============================================================================
-- SECTION 6 -- SEGMENT-LEVEL KPIs & REVENUE CONTRIBUTION
-- =============================================================================
CREATE OR REPLACE VIEW vw_segment_kpis AS
SELECT
    business_segment,
    COUNT(*)                                                          AS outlet_count,
    ROUND(SUM(total_revenue_6mo), 2)                                  AS segment_revenue_6mo,
    ROUND(100.0 * SUM(total_revenue_6mo)
          / SUM(SUM(total_revenue_6mo)) OVER (), 2)                   AS pct_of_total_revenue,
    ROUND(AVG(avg_total_fuel), 0)                                     AS avg_fuel_per_outlet,
    ROUND(AVG(avg_diesel_ratio), 3)                                   AS avg_diesel_ratio
FROM vw_outlet_segments
GROUP BY business_segment
ORDER BY segment_revenue_6mo DESC;

SELECT * FROM vw_segment_kpis;


-- =============================================================================
-- SECTION 7 -- PARETO (80/20) REVENUE CONCENTRATION ANALYSIS
-- =============================================================================
--  Classic BA question: "how many outlets actually drive the business?"
CREATE OR REPLACE VIEW vw_pareto_revenue AS
WITH ranked AS (
    SELECT
        outlet_id,
        outlet_code,
        location_type,
        total_revenue_6mo,
        ROW_NUMBER() OVER (ORDER BY total_revenue_6mo DESC)               AS revenue_rank,
        SUM(total_revenue_6mo) OVER (ORDER BY total_revenue_6mo DESC)     AS running_revenue,
        SUM(total_revenue_6mo) OVER ()                                   AS grand_total_revenue,
        COUNT(*) OVER ()                                                 AS total_outlets
    FROM vw_outlet_profile
)
SELECT
    outlet_id,
    outlet_code,
    location_type,
    total_revenue_6mo,
    revenue_rank,
    ROUND(100.0 * running_revenue / grand_total_revenue, 2)  AS cumulative_pct_revenue,
    ROUND(100.0 * revenue_rank / total_outlets, 2)           AS cumulative_pct_outlets
FROM ranked;

-- 7.1  How many outlets (and what %) generate 80% of total revenue?
SELECT
    MIN(revenue_rank)                              AS outlets_needed_for_80pct_revenue,
    ROUND(MIN(cumulative_pct_outlets), 1)          AS pct_of_network
FROM vw_pareto_revenue
WHERE cumulative_pct_revenue >= 80;


-- =============================================================================
-- SECTION 8 -- STATISTICAL ANOMALY DETECTION (Z-SCORE)
-- =============================================================================
--  Flags outlet-months that deviate more than 2 standard deviations from
--  their own location-type peer group in the same month -- catches both
--  supply disruptions (unexpected drop) and potential data quality issues
--  (unexpected spike) that a static threshold would miss.
CREATE OR REPLACE VIEW vw_outlet_anomalies AS
WITH stats AS (
    SELECT
        outlet_id,
        outlet_code,
        location_type,
        sales_month,
        total_fuel_liters,
        AVG(total_fuel_liters)    OVER (PARTITION BY sales_month, location_type) AS peer_mean,
        STDDEV(total_fuel_liters) OVER (PARTITION BY sales_month, location_type) AS peer_stddev
    FROM vw_outlet_monthly_features
)
SELECT
    *,
    ROUND((total_fuel_liters - peer_mean) / NULLIF(peer_stddev, 0), 2) AS z_score,
    (ABS((total_fuel_liters - peer_mean) / NULLIF(peer_stddev, 0)) >= 2) AS is_anomaly
FROM stats;

-- 8.1  Current anomalies, most extreme first
SELECT outlet_code, location_type, sales_month, total_fuel_liters, z_score
FROM vw_outlet_anomalies
WHERE is_anomaly
ORDER BY ABS(z_score) DESC;


-- =============================================================================
-- SECTION 9 -- SUPPLY-CHAIN PRIORITY SCORING
-- =============================================================================
--  Composite, weighted score used to rank outlets for the next allocation
--  cycle. Weights are a business judgment call, documented here so they can
--  be challenged/tuned rather than hidden inside a model:
--    45% revenue percentile   -- protect the outlets that drive the P&L
--    30% diesel percentile    -- logistics/highway outlets stock out fastest
--    25% growth percentile    -- rising demand needs supply to keep pace
CREATE OR REPLACE VIEW vw_supply_priority AS
WITH trend AS (
    SELECT outlet_id, AVG(mom_growth_pct) AS avg_mom_growth
    FROM vw_outlet_mom_trend
    WHERE mom_growth_pct IS NOT NULL
    GROUP BY outlet_id
),
combined AS (
    SELECT
        s.outlet_id,
        s.outlet_code,
        s.location_type,
        s.business_segment,
        s.revenue_percentile,
        s.diesel_percentile,
        COALESCE(t.avg_mom_growth, 0)                                   AS avg_mom_growth,
        PERCENT_RANK() OVER (ORDER BY COALESCE(t.avg_mom_growth, 0))    AS growth_percentile
    FROM vw_outlet_segments s
    LEFT JOIN trend t ON t.outlet_id = s.outlet_id
)
SELECT
    *,
    ROUND((0.45 * revenue_percentile + 0.30 * diesel_percentile + 0.25 * growth_percentile)::NUMERIC, 4)
        AS supply_priority_score,
    CASE
        WHEN 0.45 * revenue_percentile + 0.30 * diesel_percentile + 0.25 * growth_percentile >= 0.70 THEN 'High'
        WHEN 0.45 * revenue_percentile + 0.30 * diesel_percentile + 0.25 * growth_percentile >= 0.40 THEN 'Medium'
        ELSE 'Low'
    END AS supply_priority_tier
FROM combined;

-- 9.1  Top 10 outlets for next allocation cycle
SELECT outlet_code, location_type, business_segment, supply_priority_score, supply_priority_tier
FROM vw_supply_priority
ORDER BY supply_priority_score DESC
LIMIT 10;


-- =============================================================================
-- SECTION 10 -- EXECUTIVE SUMMARY & ACTION QUEUE
-- =============================================================================
-- 10.1  Single-row rollup -- the numbers a leadership deck would open with.
CREATE OR REPLACE VIEW vw_executive_summary AS
SELECT
    (SELECT COUNT(*) FROM outlets)                                              AS total_outlets,
    (SELECT ROUND(SUM(total_revenue_6mo), 0) FROM vw_outlet_profile)            AS total_revenue_6mo,
    (SELECT COUNT(*) FROM vw_outlet_segments
        WHERE business_segment LIKE 'Strategic%')                              AS high_value_outlets,
    (SELECT COUNT(*) FROM vw_outlet_anomalies WHERE is_anomaly)                 AS anomaly_flags,
    (SELECT COUNT(*) FROM vw_supply_priority WHERE supply_priority_tier = 'High') AS high_priority_outlets,
    (SELECT MIN(revenue_rank) FROM vw_pareto_revenue WHERE cumulative_pct_revenue >= 80)
                                                                                 AS outlets_driving_80pct_revenue;

SELECT * FROM vw_executive_summary;

-- 10.2  Action queue -- outlets that are BOTH high supply priority AND
--       currently flagged anomalous: the shortlist an ops manager should
--       actually call this week.
CREATE OR REPLACE VIEW vw_action_queue AS
SELECT
    sp.outlet_code,
    sp.location_type,
    sp.business_segment,
    sp.supply_priority_tier,
    a.sales_month      AS anomaly_month,
    a.z_score
FROM vw_supply_priority sp
JOIN vw_outlet_anomalies a ON a.outlet_id = sp.outlet_id AND a.is_anomaly
WHERE sp.supply_priority_tier IN ('High', 'Medium')
ORDER BY sp.supply_priority_tier, ABS(a.z_score) DESC;

SELECT * FROM vw_action_queue;


-- =============================================================================
-- SECTION 11 -- BUSINESS RECOMMENDATIONS
-- =============================================================================
--  1. PROTECT THE CORE: "Strategic High-Volume" outlets (Section 5/6) drive a
--     disproportionate share of revenue (see Section 7 Pareto output) -- lock
--     in guaranteed monthly allocation and prioritize them in any shortage.
--  2. DE-RISK LOGISTICS CORRIDORS: "Diesel-Heavy Logistics Hub" and
--     "Strategic High-Volume (Diesel/Logistics)" outlets sit on highway
--     corridors with high diesel_ratio -- these stock out fastest under
--     disruption and should get shorter replenishment cycles, not the same
--     cadence as retail outlets.
--  3. GROW, DON'T JUST MAINTAIN, HIGH-FREQUENCY RETAIL: "High-Frequency /
--     Low-Ticket Retail" outlets have volume upside via loyalty/subscription
--     style programs even though average ticket size is low.
--  4. INVESTIGATE, DON'T IGNORE, UNDERPERFORMERS: "Underperforming / Needs
--     Review" outlets warrant a site visit before assuming low demand is
--     permanent -- could be a local competitive or operational issue.
--  5. USE THE ACTION QUEUE OPERATIONALLY: Section 10.2 is designed to be run
--     monthly and handed directly to the supply planning team -- it is the
--     one table that turns this entire pipeline into a repeatable process
--     rather than a one-off analysis.
-- =============================================================================


-- =============================================================================
-- APPENDIX A -- OPTIONAL BONUS: PURE-SQL K-MEANS APPROXIMATION (k=4)
-- =============================================================================
--  Not part of the core pipeline (see Section 5 for the production approach).
--  Included only to demonstrate that the underlying ML logic can also be
--  reproduced natively in SQL via iterative centroid assignment on
--  min-max normalized features. Four fixed starting centroids + one
--  reassignment pass is enough to show the technique; a real implementation
--  would loop this to convergence in a procedural block (PL/pgSQL) or an
--  orchestration layer.
WITH normalized AS (
    SELECT
        outlet_id,
        (avg_total_fuel     - MIN(avg_total_fuel)     OVER ()) / NULLIF(MAX(avg_total_fuel)     OVER () - MIN(avg_total_fuel)     OVER (), 0) AS n_fuel,
        (avg_monthly_visits - MIN(avg_monthly_visits) OVER ()) / NULLIF(MAX(avg_monthly_visits) OVER () - MIN(avg_monthly_visits) OVER (), 0) AS n_visits
    FROM vw_outlet_profile
),
centroids (centroid_id, c_fuel, c_visits) AS (
    VALUES (1, 0.9, 0.2), (2, 0.2, 0.9), (3, 0.5, 0.5), (4, 0.1, 0.1)
),
distances AS (
    SELECT
        n.outlet_id,
        c.centroid_id,
        SQRT(POWER(n.n_fuel - c.c_fuel, 2) + POWER(n.n_visits - c.c_visits, 2)) AS distance,
        ROW_NUMBER() OVER (PARTITION BY n.outlet_id ORDER BY SQRT(POWER(n.n_fuel - c.c_fuel, 2) + POWER(n.n_visits - c.c_visits, 2))) AS rn
    FROM normalized n
    CROSS JOIN centroids c
)
SELECT outlet_id, centroid_id AS approx_kmeans_cluster, ROUND(distance, 4) AS distance_to_centroid
FROM distances
WHERE rn = 1
ORDER BY outlet_id
LIMIT 15;
-- Only the first 15 rows are shown here since this is a demo of technique,
-- not the production segmentation -- see vw_outlet_segments (Section 5) for
-- the actual, fully-explainable logic used throughout the rest of this file.
