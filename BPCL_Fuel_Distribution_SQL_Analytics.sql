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


-- ============================================================================
-- SECTION 7 -- PARETO (80/20) REVENUE CONCENTRATION ANALYSIS
-- =============================================================================e,
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
LS0gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT0KLS0gIEJQQ0wgRlVFTCBESVNUUklCVVRJT04gJiBPVVRMRVQgU0VHTUVOVEFUSU9OIC0tIFNRTC1OQVRJVkUgQU5BTFlUSUNTIFBJUEVMSU5FCi0tID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09Ci0tICBBdXRob3IgICA6IFNodWJoYW0gTS4KLS0gIEVuZ2luZSAgIDogUG9zdGdyZVNRTCAxMysKLS0gIFB1cnBvc2UgIDogRW5kLXRvLWVuZCBTUUwgYW5hbHl0aWNzIHBpcGVsaW5lIHRoYXQgcmVwbGFjZXMgYSBQeXRob24vTUwKLS0gICAgICAgICAgICAgY2x1c3RlcmluZyB3b3JrZmxvdyB3aXRoIGEgZnVsbHkgU1FMLW5hdGl2ZSBhcHByb2FjaCwgYnVpbHQgdGhlCi0tICAgICAgICAgICAgIHdheSBhIEJ1c2luZXNzIEFuYWx5c3Qgd291bGQgZGVsaXZlciBpdCBpbnNpZGUgYSBkYXRhIHdhcmVob3VzZToKLS0gICAgICAgICAgICAgcmF3IHRhYmxlcyAtPiBmZWF0dXJlIHZpZXdzIC0+IHNlZ21lbnRhdGlvbiAtPiBLUElzIC0+IGV4ZWN1dGl2ZQotLSAgICAgICAgICAgICByZXBvcnRpbmcgLS0gbm8gZXh0ZXJuYWwgdG9vbGluZyByZXF1aXJlZCB0byByZXByb2R1Y2UgYSBzaW5nbGUKLS0gICAgICAgICAgICAgbnVtYmVyIGluIHRoaXMgZmlsZS4KLS0KLS0gIEhPVyBUTyBSVU4KLS0gIC0tLS0tLS0tLS0KLS0gICAgY3JlYXRlZGIgYnBjbF9hbmFseXRpY3MKLS0gICAgcHNxbCAtZCBicGNsX2FuYWx5dGljcyAtZiBCUENMX0Z1ZWxfRGlzdHJpYnV0aW9uX1NRTF9BbmFseXRpY3Muc3FsCi0tCi0tICBGSUxFIE1BUAotLSAgLS0tLS0tLS0KLS0gICAgU0VDVElPTiAwICBCdXNpbmVzcyBQcm9ibGVtICYgT2JqZWN0aXZlCi0tICAgIFNFQ1RJT04gMSAgU2NoZW1hIERlc2lnbgotLSAgICBTRUNUSU9OIDIgIFN5bnRoZXRpYyBEYXRhIEdlbmVyYXRpb24gKHRyYW5zcGFyZW50bHkgZGlzY2xvc2VkLCBzZWUgbm90ZXMpCi0tICAgIFNFQ1RJT04gMyAgRmVhdHVyZSBFbmdpbmVlcmluZyAodmlld3MpCi0tICAgIFNFQ1RJT04gNCAgRXhwbG9yYXRvcnkgRGF0YSBBbmFseXNpcwotLSAgICBTRUNUSU9OIDUgIE91dGxldCBTZWdtZW50YXRpb24gLS0gcGVyY2VudGlsZSAvIGJ1c2luZXNzLXJ1bGUgZHJpdmVuIChDT1JFKQotLSAgICBTRUNUSU9OIDYgIFNlZ21lbnQtTGV2ZWwgS1BJcyAmIFJldmVudWUgQ29udHJpYnV0aW9uCi0tICAgIFNFQ1RJT04gNyAgUGFyZXRvICg4MC8yMCkgUmV2ZW51ZSBDb25jZW50cmF0aW9uIEFuYWx5c2lzCi0tICAgIFNFQ1RJT04gOCAgU3RhdGlzdGljYWwgQW5vbWFseSBEZXRlY3Rpb24gKFotc2NvcmUpCi0tICAgIFNFQ1RJT04gOSAgU3VwcGx5LUNoYWluIFByaW9yaXR5IFNjb3JpbmcKLS0gICAgU0VDVElPTiAxMCBFeGVjdXRpdmUgU3VtbWFyeSAmIEFjdGlvbiBRdWV1ZQotLSAgICBTRUNUSU9OIDExIEJ1c2luZXNzIFJlY29tbWVuZGF0aW9ucyAobmFycmF0aXZlLCB0aWVkIHRvIHF1ZXJ5IG91dHB1dHMpCi0tICAgIEFQUEVORElYIEEgT3B0aW9uYWwgYm9udXM6IHB1cmUtU1FMIEstTWVhbnMgYXBwcm94aW1hdGlvbgotLSA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PQoKCi0tID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09Ci0tIFNFQ1RJT04gMCAtLSBCVVNJTkVTUyBQUk9CTEVNICYgT0JKRUNUSVZFCi0tID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09Ci0tICBCaGFyYXQgUGV0cm9sZXVtIENvcnBvcmF0aW9uIExpbWl0ZWQgKEJQQ0wpIHN1cHBsaWVzIGZ1ZWwgdG8gYSB3aWRlIG5ldHdvcmsKLS0gIG9mIHBldHJvbCBwdW1wcyAoIm91dGxldHMiKSBhY3Jvc3MgdXJiYW4sIHJ1cmFsIGFuZCBoaWdod2F5IGxvY2F0aW9ucy4KLS0gIERlbWFuZCB2YXJpZXMgc2hhcnBseSBieSBvdXRsZXQgZHVlIHRvIGRpZmZlcmVuY2VzIGluIGN1c3RvbWVyIGJlaGF2aW91ciwKLS0gIHRyYWZmaWMgcGF0dGVybnMgYW5kIGxvZ2lzdGljcyB1c2FnZSwgd2hpY2ggY3JlYXRlcyB0aHJlZSByZWN1cnJpbmcKLS0gIGJ1c2luZXNzIHByb2JsZW1zOgotLSAgICAxLiBGdWVsIGlzIG5vdCBhbHdheXMgZGlzdHJpYnV0ZWQgdG8gd2hlcmUgZGVtYW5kIGlzIGhpZ2hlc3QKLS0gICAgMi4gSW52ZW50b3J5IC8gcmVwbGVuaXNobWVudCBwbGFubmluZyBpcyByZWFjdGl2ZSByYXRoZXIgdGhhbiBkYXRhLWRyaXZlbgotLSAgICAzLiBIaWdoLXZhbHVlIGFuZCBhdC1yaXNrIG91dGxldHMgYXJlIG5vdCBjb25zaXN0ZW50bHkgaWRlbnRpZmllZAotLQotLSAgT0JKRUNUSVZFCi0tICAtLS0tLS0tLS0KLS0gIFNlZ21lbnQgb3V0bGV0cyBieSBkZW1hbmQgcGF0dGVybiwgcXVhbnRpZnkgaG93IHJldmVudWUgaXMgY29uY2VudHJhdGVkCi0tICBhY3Jvc3MgdGhlIG5ldHdvcmssIGZsYWcgb3V0bGV0cyBiZWhhdmluZyBhYm5vcm1hbGx5LCBhbmQgcHJvZHVjZSBhIHN1cHBseQotLSAgcHJpb3JpdHkgc2NvcmUgdGhhdCBjYW4gcGx1ZyBkaXJlY3RseSBpbnRvIGEgbW9udGhseSBhbGxvY2F0aW9uIGRlY2lzaW9uIC0tCi0tICBhbGwgcmVwcm9kdWNpYmxlIHdpdGggU1FMIGFsb25lLgotLSA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PQoKCi0tID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09Ci0tIFNFQ1RJT04gMSAtLSBTQ0hFTUEgREVTSUdOCi0tID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09Ci0tIFR3byB0YWJsZXM6IGEgc2xvd2x5LWNoYW5naW5nIG91dGxldCBkaW1lbnNpb24gYW5kIGEgbW9udGhseSBmYWN0IHRhYmxlLgotLSBUaGlzIG1pcnJvcnMgaG93IEJQQ0wncyBvd24gZGF0YSB3b3VsZCBhY3R1YWxseSBiZSBtb2RlbGxlZCBpbiBhIHdhcmVob3VzZQotLSAoc3Rhci1zY2hlbWEgc3R5bGUpIHJhdGhlciB0aGFuIG9uZSBmbGF0IHNwcmVhZHNoZWV0LXNoYXBlZCB0YWJsZS4KCkRST1AgVklFVyBJRiBFWElTVFMgdndfYWN0aW9uX3F1ZXVlIENBU0NBREU7CkRST1AgVklFVyBJRiBFWElTVFMgdndfZXhlY3V0aXZlX3N1bW1hcnkgQ0FTQ0FERTsKRFJPUCBWSUVXIElGIEVYSVNUUyB2d19zdXBwbHlfcHJpb3JpdHkgQ0FTQ0FERTsKRFJPUCBWSUVXIElGIEVYSVNUUyB2d19vdXRsZXRfYW5vbWFsaWVzIENBU0NBREU7CkRST1AgVklFVyBJRiBFWElTVFMgdndfcGFyZXRvX3JldmVudWUgQ0FTQ0FERTsKRFJPUCBWSUVXIElGIEVYSVNUUyB2d19zZWdtZW50X2twaXMgQ0FTQ0FERTsKRFJPUCBWSUVXIElGIEVYSVNUUyB2d19vdXRsZXRfc2VnbWVudHMgQ0FTQ0FERTsKRFJPUCBWSUVXIElGIEVYSVNUUyB2d19vdXRsZXRfbW9tX3RyZW5kIENBU0NBREU7CkRST1AgVklFVyBJRiBFWElTVFMgdndfb3V0bGV0X3Byb2ZpbGUgQ0FTQ0FERTsKRFJPUCBWSUVXIElGIEVYSVNUUyB2d19vdXRsZXRfbW9udGhseV9mZWF0dXJlcyBDQVNDQURFOwpEUk9QIFRBQkxFIElGIEVYSVNUUyBtb250aGx5X2Z1ZWxfc2FsZXMgQ0FTQ0FERTsKRFJPUCBUQUJMRSBJRiBFWElTVFMgb3V0bGV0cyBDQVNDQURFOwpEUk9QIEZVTkNUSU9OIElGIEVYSVNUUyBzeW50aF9ub3JtYWwoTlVNRVJJQywgTlVNRVJJQyk7CgpDUkVBVEUgVEFCTEUgb3V0bGV0cyAoCiAgICBvdXRsZXRfaWQgICAgICAgIFNFUklBTCBQUklNQVJZIEtFWSwKICAgIG91dGxldF9jb2RlICAgICAgVkFSQ0hBUigyMCkgIFVOSVFVRSBOT1QgTlVMTCwKICAgIGxvY2F0aW9uX3R5cGUgICAgVkFSQ0hBUigxMCkgIE5PVCBOVUxMIENIRUNLIChsb2NhdGlvbl90eXBlIElOICgnVXJiYW4nLCdSdXJhbCcsJ0hpZ2h3YXknKSksCiAgICByZWdpb24gICAgICAgICAgIFZBUkNIQVIoNTApICBOT1QgTlVMTCwKICAgIG91dGxldF90eXBlICAgICAgVkFSQ0hBUigyMCkgIE5PVCBOVUxMIENIRUNLIChvdXRsZXRfdHlwZSBJTiAoJ0NvbXBhbnkgT3duZWQnLCdEZWFsZXIgT3duZWQnKSksCiAgICBjb21taXNzaW9uZWRfb24gIERBVEUgICAgICAgICBOT1QgTlVMTAopOwoKQ1JFQVRFIFRBQkxFIG1vbnRobHlfZnVlbF9zYWxlcyAoCiAgICBzYWxlX2lkICAgICAgICAgIFNFUklBTCBQUklNQVJZIEtFWSwKICAgIG91dGxldF9pZCAgICAgICAgSU5URUdFUiBOT1QgTlVMTCBSRUZFUkVOQ0VTIG91dGxldHMob3V0bGV0X2lkKSBPTiBERUxFVEUgQ0FTQ0FERSwKICAgIHNhbGVzX21vbnRoICAgICAgREFURSAgICBOT1QgTlVMTCwKICAgIHBldHJvbF9saXRlcnMgICAgTlVNRVJJQygxMCwyKSBOT1QgTlVMTCBDSEVDSyAocGV0cm9sX2xpdGVycyAgID49IDApLAogICAgZGllc2VsX2xpdGVycyAgICBOVU1FUklDKDEwLDIpIE5PVCBOVUxMIENIRUNLIChkaWVzZWxfbGl0ZXJzICAgPj0gMCksCiAgICBrZXJvc2VuZV9saXRlcnMgIE5VTUVSSUMoMTAsMikgTk9UIE5VTEwgQ0hFQ0sgKGtlcm9zZW5lX2xpdGVycyA+PSAwKSwKICAgIG1vbnRobHlfdmlzaXRzICAgSU5URUdFUiAgICAgICBOT1QgTlVMTCBDSEVDSyAobW9udGhseV92aXNpdHMgID49IDApLAogICAgVU5JUVVFIChvdXRsZXRfaWQsIHNhbGVzX21vbnRoKQopOwoKQ1JFQVRFIElOREVYIGlkeF9zYWxlc19vdXRsZXQgT04gbW9udGhseV9mdWVsX3NhbGVzKG91dGxldF9pZCk7CkNSRUFURSBJTkRFWCBpZHhfc2FsZXNfbW9udGggIE9OIG1vbnRobHlfZnVlbF9zYWxlcyhzYWxlc19tb250aCk7CgoKLS0gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT0KLS0gU0VDVElPTiAyIC0tIFNZTlRIRVRJQyBEQVRBIEdFTkVSQVRJT04KLS0gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT0KLS0gIE5PVEUgT04gREFUQTogQlBDTCdzIHJlYWwgdHJhbnNhY3Rpb25hbCBkYXRhIGlzIGNvbmZpZGVudGlhbCBhbmQgY2Fubm90IGJlCi0tICB1c2VkIGhlcmUuIEFzIGRpc2Nsb3NlZCB0cmFuc3BhcmVudGx5LCB0aGUgZmlndXJlcyBiZWxvdyBhcmUgYSBzeW50aGV0aWMKLS0gIHNhbXBsZSBlbmdpbmVlcmVkIHRvIHJlZmxlY3QgcmVhbGlzdGljLCB3ZWxsLWRvY3VtZW50ZWQgaW5kdXN0cnkgcGF0dGVybnM6Ci0tICBoaWdoZXIgZGllc2VsIGRlbWFuZCBvbiBoaWdod2F5cyAoZnJlaWdodC9sb2dpc3RpY3MpLCBoaWdoZXIgZm9vdGZhbGwgYW5kCi0tICB0cmFuc2FjdGlvbiBmcmVxdWVuY3kgaW4gdXJiYW4gcmV0YWlsIG91dGxldHMsIGFuZCBsb3dlciBidXQgc3RlYWRpZXIKLS0gIHZvbHVtZXMgaW4gcnVyYWwgYXJlYXMuIFNpeCB0cmFpbGluZyBtb250aHMgb2YgaGlzdG9yeSBhcmUgZ2VuZXJhdGVkIHBlcgotLSAgb3V0bGV0IHNvIHRoZSBwaXBlbGluZSBjYW4gZGVtb25zdHJhdGUgdHJlbmQgYW5hbHlzaXMsIG5vdCBqdXN0IGEgc25hcHNob3QuCi0tCi0tICBzeW50aF9ub3JtYWwoKSBhcHByb3hpbWF0ZXMgYSBHYXVzc2lhbiBkcmF3IHVzaW5nIHRoZSBjbGFzc2ljCi0tICAic3VtIG9mIHR3ZWx2ZSB1bmlmb3JtcyIgdHJpY2sgKElyd2luLUhhbGwgYXBwcm94aW1hdGlvbiBvZiB0aGUgQ0xUKSwgc28KLS0gIHRoZSBzeW50aGV0aWMgZmlndXJlcyBsb29rIGxpa2UgcmVhbC13b3JsZCBjb250aW51b3VzIG1lYXN1cmVtZW50cyByYXRoZXIKLS0gIHRoYW4gdW5pZm9ybWx5LXJhbmRvbSBub2lzZS4gSW1wbGVtZW50ZWQgaW4gUEwvcGdTUUwgKHJhdGhlciB0aGFuIGEgcGxhaW4KLS0gIFNRTCBmdW5jdGlvbikgc28gdGhlIE5VTUVSSUMgcmV0dXJuIHR5cGUgaXMgZW5mb3JjZWQgcmVsaWFibHkgcmVnYXJkbGVzcwotLSAgb2YgUG9zdGdyZVNRTCB2ZXJzaW9uIC0tIG5vdGUgdGhhdCBQb3N0Z3JlU1FMIDE2KyBzaGlwcyBpdHMgb3duIG5hdGl2ZQotLSAgcmFuZG9tX25vcm1hbCgpLCB3aGljaCB0aGlzIHByb2plY3QgaW50ZW50aW9uYWxseSBhdm9pZHMgZGVwZW5kaW5nIG9uIHNvCi0tICB0aGUgc2NyaXB0IHJ1bnMgdW5tb2RpZmllZCBvbiBQb3N0Z3JlU1FMIDEzLTE1IGFzIHdlbGwuCkNSRUFURSBPUiBSRVBMQUNFIEZVTkNUSU9OIHN5bnRoX25vcm1hbChwX21lYW4gTlVNRVJJQywgcF9zdGRkZXYgTlVNRVJJQykKUkVUVVJOUyBOVU1FUklDIEFTICQkCkRFQ0xBUkUKICAgIHZfc3VtIERPVUJMRSBQUkVDSVNJT047CkJFR0lOCiAgICBTRUxFQ1QgU1VNKHJhbmRvbSgpKSBJTlRPIHZfc3VtIEZST00gZ2VuZXJhdGVfc2VyaWVzKDEsIDEyKTsKICAgIFJFVFVSTiBwX21lYW4gKyBwX3N0ZGRldiAqICh2X3N1bSAtIDYpOwpFTkQ7CiQkIExBTkdVQUdFIHBscGdzcWw7CgotLSBGaXhlZCBzZWVkIHNvIGV2ZXJ5IGNsb25lIG9mIHRoaXMgcmVwbyByZXByb2R1Y2VzIHRoZSBzYW1lIHN5bnRoZXRpYwotLSBkYXRhc2V0IC0tIGFuZCB0aGVyZWZvcmUgdGhlIHNhbWUgbnVtYmVycyBxdW90ZWQgaW4gdGhlIFJFQURNRS4KU0VMRUNUIHNldHNlZWQoMC40Mik7CgotLSAyLjEgIE91dGxldHM6IDE1MCBvdXRsZXRzIHNwcmVhZCBhY3Jvc3MgbG9jYXRpb24gdHlwZXMgYW5kIEluZGlhbiByZWdpb25zCklOU0VSVCBJTlRPIG91dGxldHMgKG91dGxldF9jb2RlLCBsb2NhdGlvbl90eXBlLCByZWdpb24sIG91dGxldF90eXBlLCBjb21taXNzaW9uZWRfb24pClNFTEVDVAogICAgJ0JQQ0wtJyB8fCBMUEFEKGc6OlRFWFQsIDQsICcwJyksCiAgICBsb2MubG9jYXRpb25fdHlwZSwKICAgIHJlZy5yZWdpb24sCiAgICAoQ0FTRSBXSEVOIHJhbmRvbSgpIDwgMC41NSBUSEVOICdEZWFsZXIgT3duZWQnIEVMU0UgJ0NvbXBhbnkgT3duZWQnIEVORCksCiAgICAoREFURSAnMjAxMC0wMS0wMScgKyAocmFuZG9tKCkgKiA1MDAwKTo6SU5UKQpGUk9NIGdlbmVyYXRlX3NlcmllcygxLCAxNTApIEFTIGcKQ1JPU1MgSk9JTiBMQVRFUkFMICgKICAgIC0tIDMwJSBVcmJhbiwgMzAlIFJ1cmFsLCA0MCUgSGlnaHdheSAtLSBoaWdod2F5LWhlYXZ5IG5ldHdvcmssIGFzIEJQQ0wncwogICAgLS0gcmVhbCBkaXN0cmlidXRpb24gc2tld3MgdG93YXJkIG5hdGlvbmFsL3N0YXRlIGhpZ2h3YXkgY29ycmlkb3JzCiAgICBTRUxFQ1QgQ0FTRQogICAgICAgIFdIRU4gZyAlIDEwIDwgMyBUSEVOICdVcmJhbicKICAgICAgICBXSEVOIGcgJSAxMCA8IDYgVEhFTiAnUnVyYWwnCiAgICAgICAgRUxTRSAnSGlnaHdheScKICAgIEVORCBBUyBsb2NhdGlvbl90eXBlCikgbG9jCkNST1NTIEpPSU4gTEFURVJBTCAoCiAgICBTRUxFQ1QgKEFSUkFZWydNYWhhcmFzaHRyYScsJ0d1amFyYXQnLCdLYXJuYXRha2EnLCdUYW1pbCBOYWR1JywnUmFqYXN0aGFuJywKICAgICAgICAgICAgICAgICAgJ1V0dGFyIFByYWRlc2gnLCdNYWRoeWEgUHJhZGVzaCcsJ1RlbGFuZ2FuYSddKVsxICsgZmxvb3IocmFuZG9tKCkqOCk6OklOVF0gQVMgcmVnaW9uCikgcmVnOwoKLS0gMi4yICBNb250aGx5IGZ1ZWwgc2FsZXM6IDYgdHJhaWxpbmcgbW9udGhzIHBlciBvdXRsZXQsIGdlbmVyYXRlZCBmcm9tCi0tICAgICAgbG9jYXRpb24tc3BlY2lmaWMgZGVtYW5kIHByb2ZpbGVzIChtZWFuL3N0ZGRldiBwYWlycyBiZWxvdyBtaXJyb3IgdGhlCi0tICAgICAgZG9tYWluIGFzc3VtcHRpb25zIHVzZWQgaW4gdGhlIG9yaWdpbmFsIFB5dGhvbiBhbmFseXNpcykuCklOU0VSVCBJTlRPIG1vbnRobHlfZnVlbF9zYWxlcyAob3V0bGV0X2lkLCBzYWxlc19tb250aCwgcGV0cm9sX2xpdGVycywgZGllc2VsX2xpdGVycywga2Vyb3NlbmVfbGl0ZXJzLCBtb250aGx5X3Zpc2l0cykKU0VMRUNUCiAgICBvLm91dGxldF9pZCwKICAgIChEQVRFX1RSVU5DKCdtb250aCcsIENVUlJFTlRfREFURSkgLSAobSB8fCAnIG1vbnRocycpOjpJTlRFUlZBTCk6OkRBVEUgQVMgc2FsZXNfbW9udGgsCiAgICBHUkVBVEVTVCgwLCBST1VORCgKICAgICAgICBzeW50aF9ub3JtYWwoCiAgICAgICAgICAgIENBU0Ugby5sb2NhdGlvbl90eXBlIFdIRU4gJ1VyYmFuJyBUSEVOIDQwMDAgV0hFTiAnSGlnaHdheScgVEhFTiAyNTAwIEVMU0UgMTUwMCBFTkQsCiAgICAgICAgICAgIENBU0Ugby5sb2NhdGlvbl90eXBlIFdIRU4gJ1VyYmFuJyBUSEVOIDUwMCAgV0hFTiAnSGlnaHdheScgVEhFTiA0MDAgIEVMU0UgMzAwICBFTkQKICAgICAgICApLCAyKSkgQVMgcGV0cm9sX2xpdGVycywKICAgIEdSRUFURVNUKDAsIFJPVU5EKAogICAgICAgIHN5bnRoX25vcm1hbCgKICAgICAgICAgICAgQ0FTRSBvLmxvY2F0aW9uX3R5cGUgV0hFTiAnVXJiYW4nIFRIRU4gMjAwMCBXSEVOICdIaWdod2F5JyBUSEVOIDUwMDAgRUxTRSAxMjAwIEVORCwKICAgICAgICAgICAgQ0FTRSBvLmxvY2F0aW9uX3R5cGUgV0hFTiAnVXJiYW4nIFRIRU4gNDAwICBXSEVOICdIaWdod2F5JyBUSEVOIDgwMCAgRUxTRSAzMDAgIEVORAogICAgICAgICksIDIpKSBBUyBkaWVzZWxfbGl0ZXJzLAogICAgR1JFQVRFU1QoMCwgUk9VTkQoCiAgICAgICAgc3ludGhfbm9ybWFsKAogICAgICAgICAgICBDQVNFIG8ubG9jYXRpb25fdHlwZSBXSEVOICdVcmJhbicgVEhFTiA1MDAgIFdIRU4gJ0hpZ2h3YXknIFRIRU4gMzAwICBFTFNFIDgwMCAgRU5ELAogICAgICAgICAgICBDQVNFIG8ubG9jYXRpb25fdHlwZSBXSEVOICdVcmJhbicgVEhFTiAxMDAgIFdIRU4gJ0hpZ2h3YXknIFRIRU4gODAgICBFTFNFIDIwMCAgRU5ECiAgICAgICAgKSwgMikpIEFTIGtlcm9zZW5lX2xpdGVycywKICAgIEdSRUFURVNUKDAsIFJPVU5EKAogICAgICAgIHN5bnRoX25vcm1hbCgKICAgICAgICAgICAgQ0FTRSBvLmxvY2F0aW9uX3R5cGUgV0hFTiAnVXJiYW4nIFRIRU4gMzAwICBXSEVOICdIaWdod2F5JyBUSEVOIDE3NSBFTFNFIDE0MCAgRU5ELAogICAgICAgICAgICAzMAogICAgICAgICkpKTo6SU5UIEFTIG1vbnRobHlfdmlzaXRzCkZST00gb3V0bGV0cyBvCkNST1NTIEpPSU4gZ2VuZXJhdGVfc2VyaWVzKDAsIDUpIEFTIG07CgotLSAyLjMgIEluamVjdCBhIGhhbmRmdWwgb2YgZGVsaWJlcmF0ZSBhbm9tYWxpZXMgc28gU2VjdGlvbiA4J3Mgei1zY29yZSBjaGVjawotLSAgICAgIGhhcyBzb21ldGhpbmcgcmVhbCB0byBmaW5kIChtaXJyb3JzIGEgc3VwcGx5IGRpc3J1cHRpb24gLyBkYXRhLWVudHJ5Ci0tICAgICAgc3Bpa2UgeW91IHdvdWxkIGFjdHVhbGx5IHNlZSBpbiBwcm9kdWN0aW9uIGRhdGEpLgpVUERBVEUgbW9udGhseV9mdWVsX3NhbGVzClNFVCBkaWVzZWxfbGl0ZXJzID0gZGllc2VsX2xpdGVycyAqIDIuNgpXSEVSRSBvdXRsZXRfaWQgPSAoU0VMRUNUIG91dGxldF9pZCBGUk9NIG91dGxldHMgV0hFUkUgbG9jYXRpb25fdHlwZSA9ICdIaWdod2F5JyBPUkRFUiBCWSBvdXRsZXRfaWQgTElNSVQgMSkKICBBTkQgc2FsZXNfbW9udGggPSAoREFURV9UUlVOQygnbW9udGgnLCBDVVJSRU5UX0RBVEUpIC0gSU5URVJWQUwgJzEgbW9udGgnKTo6REFURTsKClVQREFURSBtb250aGx5X2Z1ZWxfc2FsZXMKU0VUIHBldHJvbF9saXRlcnMgPSBwZXRyb2xfbGl0ZXJzICogMC4yNSwgbW9udGhseV92aXNpdHMgPSAobW9udGhseV92aXNpdHMgKiAwLjMpOjpJTlQKV0hFUkUgb3V0bGV0X2lkID0gKFNFTEVDVCBvdXRsZXRfaWQgRlJPTSBvdXRsZXRzIFdIRVJFIGxvY2F0aW9uX3R5cGUgPSAnVXJiYW4nIE9SREVSIEJZIG91dGxldF9pZCBERVNDIExJTUlUIDEpCiAgQU5EIHNhbGVzX21vbnRoID0gKERBVEVfVFJVTkMoJ21vbnRoJywgQ1VSUkVOVF9EQVRFKSAtIElOVEVSVkFMICcyIG1vbnRocycpOjpEQVRFOwoKCi0tID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09Ci0tIFNFQ1RJT04gMyAtLSBGRUFUVVJFIEVOR0lORUVSSU5HIChWSUVXUykKLS0gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT0KLS0gMy4xICBSb3ctbGV2ZWwgZW5naW5lZXJlZCBmZWF0dXJlcywgY29tcHV0ZWQgb25jZSBhbmQgcmV1c2VkIGV2ZXJ5d2hlcmUKLS0gICAgICBkb3duc3RyZWFtIHNvIGJ1c2luZXNzIGxvZ2ljIChlLmcuIHRoZSByZXZlbnVlIGZvcm11bGEpIGxpdmVzIGluIGEKLS0gICAgICBzaW5nbGUgcGxhY2UgaW5zdGVhZCBvZiBiZWluZyBjb3B5LXBhc3RlZCBhY3Jvc3MgcXVlcmllcy4KQ1JFQVRFIE9SIFJFUExBQ0UgVklFVyB2d19vdXRsZXRfbW9udGhseV9mZWF0dXJlcyBBUwpTRUxFQ1QKICAgIG8ub3V0bGV0X2lkLAogICAgby5vdXRsZXRfY29kZSwKICAgIG8ubG9jYXRpb25fdHlwZSwKICAgIG8ucmVnaW9uLAogICAgby5vdXRsZXRfdHlwZSwKICAgIHMuc2FsZXNfbW9udGgsCiAgICBzLnBldHJvbF9saXRlcnMsCiAgICBzLmRpZXNlbF9saXRlcnMsCiAgICBzLmtlcm9zZW5lX2xpdGVycywKICAgIHMubW9udGhseV92aXNpdHMsCiAgICAocy5wZXRyb2xfbGl0ZXJzICsgcy5kaWVzZWxfbGl0ZXJzICsgcy5rZXJvc2VuZV9saXRlcnMpICAgICAgICAgICAgICAgICAgICAgICBBUyB0b3RhbF9mdWVsX2xpdGVycywKICAgIFJPVU5EKChzLnBldHJvbF9saXRlcnMgKyBzLmRpZXNlbF9saXRlcnMgKyBzLmtlcm9zZW5lX2xpdGVycykKICAgICAgICAgIC8gTlVMTElGKHMubW9udGhseV92aXNpdHMsIDApLCAyKSAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgQVMgZnVlbF9wZXJfdmlzaXQsCiAgICBST1VORChzLmRpZXNlbF9saXRlcnMKICAgICAgICAgIC8gTlVMTElGKChzLnBldHJvbF9saXRlcnMgKyBzLmRpZXNlbF9saXRlcnMgKyBzLmtlcm9zZW5lX2xpdGVycyksIDApLCA0KSBBUyBkaWVzZWxfcmF0aW8sCiAgICAtLSBJbGx1c3RyYXRpdmUgcHJpY2UgYXNzdW1wdGlvbnMgKElOUi9saXRlcik6IHBldHJvbCAxMDAsIGRpZXNlbCA5MCwga2Vyb3NlbmUgNzAKICAgIFJPVU5EKHMucGV0cm9sX2xpdGVycyAqIDEwMCArIHMuZGllc2VsX2xpdGVycyAqIDkwICsgcy5rZXJvc2VuZV9saXRlcnMgKiA3MCwgMikgQVMgZXN0aW1hdGVkX3JldmVudWUKRlJPTSBtb250aGx5X2Z1ZWxfc2FsZXMgcwpKT0lOIG91dGxldHMgbyBPTiBvLm91dGxldF9pZCA9IHMub3V0bGV0X2lkOwoKLS0gMy4yICBPdXRsZXQtbGV2ZWwgcHJvZmlsZTogdHJhaWxpbmcgNi1tb250aCBhdmVyYWdlcy90b3RhbHMsIG9uZSByb3cgcGVyCi0tICAgICAgb3V0bGV0LiBUaGlzIGlzIHRoZSB0YWJsZSBldmVyeSBkb3duc3RyZWFtIHNlZ21lbnRhdGlvbiBxdWVyeSByZWFkcy4KQ1JFQVRFIE9SIFJFUExBQ0UgVklFVyB2d19vdXRsZXRfcHJvZmlsZSBBUwpTRUxFQ1QKICAgIG91dGxldF9pZCwKICAgIG91dGxldF9jb2RlLAogICAgbG9jYXRpb25fdHlwZSwKICAgIHJlZ2lvbiwKICAgIG91dGxldF90eXBlLAogICAgUk9VTkQoQVZHKHRvdGFsX2Z1ZWxfbGl0ZXJzKSwgMikgICAgIEFTIGF2Z190b3RhbF9mdWVsLAogICAgUk9VTkQoQVZHKG1vbnRobHlfdmlzaXRzKSwgMSkgICAgICAgIEFTIGF2Z19tb250aGx5X3Zpc2l0cywKICAgIFJPVU5EKEFWRyhmdWVsX3Blcl92aXNpdCksIDIpICAgICAgICBBUyBhdmdfZnVlbF9wZXJfdmlzaXQsCiAgICBST1VORChBVkcoZGllc2VsX3JhdGlvKSwgNCkgICAgICAgICAgQVMgYXZnX2RpZXNlbF9yYXRpbywKICAgIFJPVU5EKEFWRyhlc3RpbWF0ZWRfcmV2ZW51ZSksIDIpICAgICBBUyBhdmdfbW9udGhseV9yZXZlbnVlLAogICAgUk9VTkQoU1VNKGVzdGltYXRlZF9yZXZlbnVlKSwgMikgICAgIEFTIHRvdGFsX3JldmVudWVfNm1vLAogICAgQ09VTlQoKikgICAgICAgICAgICAgICAgICAgICAgICAgICAgIEFTIG1vbnRoc19vYnNlcnZlZApGUk9NIHZ3X291dGxldF9tb250aGx5X2ZlYXR1cmVzCkdST1VQIEJZIG91dGxldF9pZCwgb3V0bGV0X2NvZGUsIGxvY2F0aW9uX3R5cGUsIHJlZ2lvbiwgb3V0bGV0X3R5cGU7CgotLSAzLjMgIE1vbnRoLW92ZXItbW9udGggdHJlbmQsIHVzaW5nIExBRygpIHRvIGF2b2lkIGEgc2VsZi1qb2luLgpDUkVBVEUgT1IgUkVQTEFDRSBWSUVXIHZ3X291dGxldF9tb21fdHJlbmQgQVMKU0VMRUNUCiAgICBvdXRsZXRfaWQsCiAgICBvdXRsZXRfY29kZSwKICAgIGxvY2F0aW9uX3R5cGUsCiAgICBzYWxlc19tb250aCwKICAgIHRvdGFsX2Z1ZWxfbGl0ZXJzLAogICAgTEFHKHRvdGFsX2Z1ZWxfbGl0ZXJzKSBPVkVSIChQQVJUSVRJT04gQlkgb3V0bGV0X2lkIE9SREVSIEJZIHNhbGVzX21vbnRoKSBBUyBwcmV2X21vbnRoX2Z1ZWwsCiAgICBST1VORCgKICAgICAgICAxMDAuMCAqICh0b3RhbF9mdWVsX2xpdGVycyAtIExBRyh0b3RhbF9mdWVsX2xpdGVycykgT1ZFUiAoUEFSVElUSU9OIEJZIG91dGxldF9pZCBPUkRFUiBCWSBzYWxlc19tb250aCkpCiAgICAgICAgLyBOVUxMSUYoTEFHKHRvdGFsX2Z1ZWxfbGl0ZXJzKSBPVkVSIChQQVJUSVRJT04gQlkgb3V0bGV0X2lkIE9SREVSIEJZIHNhbGVzX21vbnRoKSwgMCkKICAgICwgMikgQVMgbW9tX2dyb3d0aF9wY3QKRlJPTSB2d19vdXRsZXRfbW9udGhseV9mZWF0dXJlczsKCgotLSA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PQotLSBTRUNUSU9OIDQgLS0gRVhQTE9SQVRPUlkgREFUQSBBTkFMWVNJUwotLSA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PQotLSA0LjEgIERlbWFuZCBwcm9maWxlIGJ5IGxvY2F0aW9uIHR5cGUgLS0gc2V0cyB1cCB0aGUgIndoeSBzZWdtZW50IGF0IGFsbCIKLS0gICAgICBzdG9yeTogaGlnaHdheSBvdXRsZXRzIGFyZSBkaWVzZWwtaGVhdnkgYW5kIGxvdy1mcmVxdWVuY3ksIHVyYmFuCi0tICAgICAgb3V0bGV0cyBhcmUgaGlnaC1mcmVxdWVuY3kgYW5kIHBldHJvbC1oZWF2eSwgcnVyYWwgb3V0bGV0cyBsYWcgb24gYm90aC4KU0VMRUNUCiAgICBsb2NhdGlvbl90eXBlLAogICAgQ09VTlQoRElTVElOQ1Qgb3V0bGV0X2lkKSAgICAgICAgICAgICAgICBBUyBvdXRsZXRfY291bnQsCiAgICBST1VORChBVkcoYXZnX3RvdGFsX2Z1ZWwpLCAwKSAgICAgICAgICAgIEFTIGF2Z19tb250aGx5X2Z1ZWxfbGl0ZXJzLAogICAgUk9VTkQoQVZHKGF2Z19tb250aGx5X3Zpc2l0cyksIDApICAgICAgICBBUyBhdmdfbW9udGhseV92aXNpdHMsCiAgICBST1VORChBVkcoYXZnX2RpZXNlbF9yYXRpbyksIDMpICAgICAgICAgIEFTIGF2Z19kaWVzZWxfcmF0aW8sCiAgICBST1VORChBVkcoYXZnX21vbnRobHlfcmV2ZW51ZSksIDApICAgICAgIEFTIGF2Z19tb250aGx5X3JldmVudWUKRlJPTSB2d19vdXRsZXRfcHJvZmlsZQpHUk9VUCBCWSBsb2NhdGlvbl90eXBlCk9SREVSIEJZIGF2Z19tb250aGx5X3JldmVudWUgREVTQzsKCi0tIDQuMiAgU3ByZWFkL3ZhcmlhYmlsaXR5IGNoZWNrIChhcmUgb3V0bGV0cyB3aXRoaW4gYSBsb2NhdGlvbiB0eXBlIGFjdHVhbGx5Ci0tICAgICAgaG9tb2dlbmVvdXMsIG9yIGlzIG1vcmUgZ3JhbnVsYXIgc2VnbWVudGF0aW9uIGp1c3RpZmllZD8pClNFTEVDVAogICAgbG9jYXRpb25fdHlwZSwKICAgIFJPVU5EKE1JTihhdmdfdG90YWxfZnVlbCksIDApICAgQVMgbWluX2Z1ZWwsCiAgICBST1VORChNQVgoYXZnX3RvdGFsX2Z1ZWwpLCAwKSAgIEFTIG1heF9mdWVsLAogICAgUk9VTkQoU1REREVWKGF2Z190b3RhbF9mdWVsKSwwKSBBUyBzdGRkZXZfZnVlbApGUk9NIHZ3X291dGxldF9wcm9maWxlCkdST1VQIEJZIGxvY2F0aW9uX3R5cGU7CgoKLS0gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT0KLS0gU0VDVElPTiA1IC0tIE9VVExFVCBTRUdNRU5UQVRJT04gKFBFUkNFTlRJTEUgLyBCVVNJTkVTUy1SVUxFIERSSVZFTikgLS0gQ09SRQotLSA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PQotLSAgRGVzaWduIGNob2ljZTogcmF0aGVyIHRoYW4gYSBibGFjay1ib3ggY2x1c3RlcmluZyBhbGdvcml0aG0sIHRoaXMKLS0gIHNlZ21lbnRhdGlvbiB1c2VzIE5USUxFKCkgcXVhcnRpbGVzIGFuZCBQRVJDRU5UX1JBTksoKSBvbiB0aGUgdHdvIG1ldHJpY3MKLS0gIHRoYXQgYWN0dWFsbHkgZHJpdmUgc3VwcGx5IGRlY2lzaW9ucyAtLSByZXZlbnVlIGFuZCB2aXNpdCBmcmVxdWVuY3kgLS0gcGx1cwotLSAgYSBkaWVzZWwtc2hhcmUgcGVyY2VudGlsZSB0byBjYXRjaCBsb2dpc3RpY3MtaGVhdnkgb3V0bGV0cy4gRXZlcnkgb3V0bGV0J3MKLS0gIHNlZ21lbnQgaXMgZnVsbHkgZXhwbGFpbmFibGUgZnJvbSBpdHMgb3duIG51bWJlcnMsIHdoaWNoIG1hdHRlcnMgZmFyIG1vcmUKLS0gIGluIGEgYnVzaW5lc3MgcmV2aWV3IHRoYW4gbWFyZ2luYWwgZ2FpbnMgaW4gYSBzaW1pbGFyaXR5IG1ldHJpYy4KQ1JFQVRFIE9SIFJFUExBQ0UgVklFVyB2d19vdXRsZXRfc2VnbWVudHMgQVMKV0lUSCBzY29yZWQgQVMgKAogICAgU0VMRUNUCiAgICAgICAgcC4qLAogICAgICAgIE5USUxFKDQpICAgICAgIE9WRVIgKE9SREVSIEJZIGF2Z19tb250aGx5X3JldmVudWUpICBBUyByZXZlbnVlX3F1YXJ0aWxlLAogICAgICAgIE5USUxFKDQpICAgICAgIE9WRVIgKE9SREVSIEJZIGF2Z19tb250aGx5X3Zpc2l0cykgICBBUyBmcmVxdWVuY3lfcXVhcnRpbGUsCiAgICAgICAgUEVSQ0VOVF9SQU5LKCkgT1ZFUiAoT1JERVIgQlkgYXZnX21vbnRobHlfcmV2ZW51ZSkgIEFTIHJldmVudWVfcGVyY2VudGlsZSwKICAgICAgICBQRVJDRU5UX1JBTksoKSBPVkVSIChPUkRFUiBCWSBhdmdfZGllc2VsX3JhdGlvKSAgICAgQVMgZGllc2VsX3BlcmNlbnRpbGUKICAgIEZST00gdndfb3V0bGV0X3Byb2ZpbGUgcAopClNFTEVDVAogICAgKiwKICAgIENBU0UKICAgICAgICBXSEVOIHJldmVudWVfcXVhcnRpbGUgPSA0IEFORCBkaWVzZWxfcGVyY2VudGlsZSA+PSAwLjc1CiAgICAgICAgICAgIFRIRU4gJ1N0cmF0ZWdpYyBIaWdoLVZvbHVtZSAoRGllc2VsL0xvZ2lzdGljcyknCiAgICAgICAgV0hFTiByZXZlbnVlX3F1YXJ0aWxlID0gNAogICAgICAgICAgICBUSEVOICdTdHJhdGVnaWMgSGlnaC1Wb2x1bWUnCiAgICAgICAgV0hFTiBmcmVxdWVuY3lfcXVhcnRpbGUgPSA0IEFORCByZXZlbnVlX3F1YXJ0aWxlIDw9IDIKICAgICAgICAgICAgVEhFTiAnSGlnaC1GcmVxdWVuY3kgLyBMb3ctVGlja2V0IFJldGFpbCcKICAgICAgICBXSEVOIGRpZXNlbF9wZXJjZW50aWxlID49IDAuNzUgQU5EIHJldmVudWVfcXVhcnRpbGUgSU4gKDIsIDMpCiAgICAgICAgICAgIFRIRU4gJ0RpZXNlbC1IZWF2eSBMb2dpc3RpY3MgSHViJwogICAgICAgIFdIRU4gcmV2ZW51ZV9xdWFydGlsZSA9IDEgQU5EIGZyZXF1ZW5jeV9xdWFydGlsZSA9IDEKICAgICAgICAgICAgVEhFTiAnVW5kZXJwZXJmb3JtaW5nIC8gTmVlZHMgUmV2aWV3JwogICAgICAgIEVMU0UgJ1N0ZWFkeSBNaWQtVGllciBPdXRsZXQnCiAgICBFTkQgQVMgYnVzaW5lc3Nfc2VnbWVudApGUk9NIHNjb3JlZDsKCi0tIDUuMSAgU2FuaXR5IGNoZWNrOiBzZWdtZW50IHNpemVzIGFuZCBoZWFkbGluZSBtZXRyaWNzClNFTEVDVAogICAgYnVzaW5lc3Nfc2VnbWVudCwKICAgIENPVU5UKCopICAgICAgICAgICAgICAgICAgICAgICAgICAgIEFTIG91dGxldF9jb3VudCwKICAgIFJPVU5EKEFWRyhhdmdfbW9udGhseV9yZXZlbnVlKSwgMCkgIEFTIGF2Z19tb250aGx5X3JldmVudWUsCiAgICBST1VORChBVkcoYXZnX2RpZXNlbF9yYXRpbyksIDMpICAgICBBUyBhdmdfZGllc2VsX3JhdGlvLAogICAgUk9VTkQoQVZHKGF2Z19tb250aGx5X3Zpc2l0cyksIDApICAgQVMgYXZnX21vbnRobHlfdmlzaXRzCkZST00gdndfb3V0bGV0X3NlZ21lbnRzCkdST1VQIEJZIGJ1c2luZXNzX3NlZ21lbnQKT1JERVIgQlkgYXZnX21vbnRobHlfcmV2ZW51ZSBERVNDOwoKCi0tID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09Ci0tIFNFQ1RJT04gNiAtLSBTRUdNRU5ULUxFVkVMIEtQSXMgJiBSRVZFTlVFIENPTlRSSUJVVElPTgotLSA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PQpDUkVBVEUgT1IgUkVQTEFDRSBWSUVXIHZ3X3NlZ21lbnRfa3BpcyBBUwpTRUxFQ1QKICAgIGJ1c2luZXNzX3NlZ21lbnQsCiAgICBDT1VOVCgqKSAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBBUyBvdXRsZXRfY291bnQsCiAgICBST1VORChTVU0odG90YWxfcmV2ZW51ZV82bW8pLCAyKSAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBBUyBzZWdtZW50X3JldmVudWVfNm1vLAogICAgUk9VTkQoMTAwLjAgKiBTVU0odG90YWxfcmV2ZW51ZV82bW8pCiAgICAgICAgICAvIFNVTShTVU0odG90YWxfcmV2ZW51ZV82bW8pKSBPVkVSICgpLCAyKSAgICAgICAgICAgICAgICAgICBBUyBwY3Rfb2ZfdG90YWxfcmV2ZW51ZSwKICAgIFJPVU5EKEFWRyhhdmdfdG90YWxfZnVlbCksIDApICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgIEFTIGF2Z19mdWVsX3Blcl9vdXRsZXQsCiAgICBST1VORChBVkcoYXZnX2RpZXNlbF9yYXRpbyksIDMpICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBBUyBhdmdfZGllc2VsX3JhdGlvCkZST00gdndfb3V0bGV0X3NlZ21lbnRzCkdST1VQIEJZIGJ1c2luZXNzX3NlZ21lbnQKT1JERVIgQlkgc2VnbWVudF9yZXZlbnVlXzZtbyBERVNDOwoKU0VMRUNUICogRlJPTSB2d19zZWdtZW50X2twaXM7CgoKLS0gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PQotLSBTRUNUSU9OIDcgLS0gUEFSRVRPICg4MC8yMCkgUkVWRU5VRSBDT05DRU5UUkFUSU9OIEFOQUxZU0lTCi0tID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09Ci0tICBDbGFzc2ljIEJBIHF1ZXN0aW9uOiAiaG93IG1hbnkgb3V0bGV0cyBhY3R1YWxseSBkcml2ZSB0aGUgYnVzaW5lc3M/IgpDUkVBVEUgT1IgUkVQTEFDRSBWSUVXIHZ3X3BhcmV0b19yZXZlbnVlIEFTCldJVEggcmFua2VkIEFTICgKICAgIFNFTEVDVAogICAgICAgIG91dGxldF9pZCwKICAgICAgICBvdXRsZXRfY29kZSwKICAgICAgICBsb2NhdGlvbl90eXBlLAogICAgICAgIHRvdGFsX3JldmVudWVfNm1vLAogICAgICAgIFJPV19OVU1CRVIoKSBPVkVSIChPUkRFUiBCWSB0b3RhbF9yZXZlbnVlXzZtbyBERVNDKSAgICAgICAgICAgICAgIEFTIHJldmVudWVfcmFuaywKICAgICAgICBTVU0odG90YWxfcmV2ZW51ZV82bW8pIE9WRVIgKE9SREVSIEJZIHRvdGFsX3JldmVudWVfNm1vIERFU0MpICAgICBBUyBydW5uaW5nX3JldmVudWUsCiAgICAgICAgU1VNKHRvdGFsX3JldmVudWVfNm1vKSBPVkVSICgpICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBBUyBncmFuZF90b3RhbF9yZXZlbnVlLAogICAgICAgIENPVU5UKCopIE9WRVIgKCkgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgQVMgdG90YWxfb3V0bGV0cwogICAgRlJPTSB2d19vdXRsZXRfcHJvZmlsZQopClNFTEVDVAogICAgb3V0bGV0X2lkLAogICAgb3V0bGV0X2NvZGUsCiAgICBsb2NhdGlvbl90eXA=ZSwKICAgIHRvdGFsX3JldmVudWVfNm1vLAogICAgcmV2ZW51ZV9yYW5rLAogICAgUk9VTkQoMTAwLjAgKiBydW5uaW5nX3JldmVudWUgLyBncmFuZF90b3RhbF9yZXZlbnVlLCAyKSAgQVMgY3VtdWxhdGl2ZV9wY3RfcmV2ZW51ZSwKICAgIFJPVU5EKDEwMC4wICogcmV2ZW51ZV9yYW5rIC8gdG90YWxfb3V0bGV0cywgMikgICAgICAgICAgIEFTIGN1bXVsYXRpdmVfcGN0X291dGxldHMKRlJPTSByYW5rZWQ7CgotLSA3LjEgIEhvdyBtYW55IG91dGxldHMgKGFuZCB3aGF0ICUpIGdlbmVyYXRlIDgwJSBvZiB0b3RhbCByZXZlbnVlPwpTRUxFQ1QKICAgIE1JTihyZXZlbnVlX3JhbmspICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgQVMgb3V0bGV0c19uZWVkZWRfZm9yXzgwcGN0X3JldmVudWUsCiAgICBST1VORChNSU4oY3VtdWxhdGl2ZV9wY3Rfb3V0bGV0cyksIDEpICAgICAgICAgIEFTIHBjdF9vZl9uZXR3b3JrCkZST00gdndfcGFyZXRvX3JldmVudWUKV0hFUkUgY3VtdWxhdGl2ZV9wY3RfcmV2ZW51ZSA+PSA4MDsKCgotLSA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PQotLSBTRUNUSU9OIDggLS0gU1RBVElTVElDQUwgQU5PTUFMWSBERVRFQ1RJT04gKFotU0NPUkUpCi0tID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09Ci0tICBGbGFncyBvdXRsZXQtbW9udGhzIHRoYXQgZGV2aWF0ZSBtb3JlIHRoYW4gMiBzdGFuZGFyZCBkZXZpYXRpb25zIGZyb20KLS0gIHRoZWlyIG93biBsb2NhdGlvbi10eXBlIHBlZXIgZ3JvdXAgaW4gdGhlIHNhbWUgbW9udGggLS0gY2F0Y2hlcyBib3RoCi0tICBzdXBwbHkgZGlzcnVwdGlvbnMgKHVuZXhwZWN0ZWQgZHJvcCkgYW5kIHBvdGVudGlhbCBkYXRhIHF1YWxpdHkgaXNzdWVzCi0tICAodW5leHBlY3RlZCBzcGlrZSkgdGhhdCBhIHN0YXRpYyB0aHJlc2hvbGQgd291bGQgbWlzcy4KQ1JFQVRFIE9SIFJFUExBQ0UgVklFVyB2d19vdXRsZXRfYW5vbWFsaWVzIEFTCldJVEggc3RhdHMgQVMgKAogICAgU0VMRUNUCiAgICAgICAgb3V0bGV0X2lkLAogICAgICAgIG91dGxldF9jb2RlLAogICAgICAgIGxvY2F0aW9uX3R5cGUsCiAgICAgICAgc2FsZXNfbW9udGgsCiAgICAgICAgdG90YWxfZnVlbF9saXRlcnMsCiAgICAgICAgQVZHKHRvdGFsX2Z1ZWxfbGl0ZXJzKSAgICBPVkVSIChQQVJUSVRJT04gQlkgc2FsZXNfbW9udGgsIGxvY2F0aW9uX3R5cGUpIEFTIHBlZXJfbWVhbiwKICAgICAgICBTVERERVYodG90YWxfZnVlbF9saXRlcnMpIE9WRVIgKFBBUlRJVElPTiBCWSBzYWxlc19tb250aCwgbG9jYXRpb25fdHlwZSkgQVMgcGVlcl9zdGRkZXYKICAgIEZST00gdndfb3V0bGV0X21vbnRobHlfZmVhdHVyZXMKKQpTRUxFQ1QKICAgICosCiAgICBST1VORCgodG90YWxfZnVlbF9saXRlcnMgLSBwZWVyX21lYW4pIC8gTlVMTElGKHBlZXJfc3RkZGV2LCAwKSwgMikgQVMgel9zY29yZSwKICAgIChBQlMoKHRvdGFsX2Z1ZWxfbGl0ZXJzIC0gcGVlcl9tZWFuKSAvIE5VTExJRihwZWVyX3N0ZGRldiwgMCkpID49IDIpIEFTIGlzX2Fub21hbHkKRlJPTSBzdGF0czsKCi0tIDguMSAgQ3VycmVudCBhbm9tYWxpZXMsIG1vc3QgZXh0cmVtZSBmaXJzdApTRUxFQ1Qgb3V0bGV0X2NvZGUsIGxvY2F0aW9uX3R5cGUsIHNhbGVzX21vbnRoLCB0b3RhbF9mdWVsX2xpdGVycywgel9zY29yZQpGUk9NIHZ3X291dGxldF9hbm9tYWxpZXMKV0hFUkUgaXNfYW5vbWFseQpPUkRFUiBCWSBBQlMoel9zY29yZSkgREVTQzsKCgotLSA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PQotLSBTRUNUSU9OIDkgLS0gU1VQUExZLUNIQUlOIFBSSU9SSVRZIFNDT1JJTkcKLS0gPT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT0KLS0gIENvbXBvc2l0ZSwgd2VpZ2h0ZWQgc2NvcmUgdXNlZCB0byByYW5rIG91dGxldHMgZm9yIHRoZSBuZXh0IGFsbG9jYXRpb24KLS0gIGN5Y2xlLiBXZWlnaHRzIGFyZSBhIGJ1c2luZXNzIGp1ZGdtZW50IGNhbGwsIGRvY3VtZW50ZWQgaGVyZSBzbyB0aGV5IGNhbgotLSAgYmUgY2hhbGxlbmdlZC90dW5lZCByYXRoZXIgdGhhbiBoaWRkZW4gaW5zaWRlIGEgbW9kZWw6Ci0tICAgIDQ1JSByZXZlbnVlIHBlcmNlbnRpbGUgICAtLSBwcm90ZWN0IHRoZSBvdXRsZXRzIHRoYXQgZHJpdmUgdGhlIFAmTAotLSAgICAzMCUgZGllc2VsIHBlcmNlbnRpbGUgICAgLS0gbG9naXN0aWNzL2hpZ2h3YXkgb3V0bGV0cyBzdG9jayBvdXQgZmFzdGVzdAotLSAgICAyNSUgZ3Jvd3RoIHBlcmNlbnRpbGUgICAgLS0gcmlzaW5nIGRlbWFuZCBuZWVkcyBzdXBwbHkgdG8ga2VlcCBwYWNlCkNSRUFURSBPUiBSRVBMQUNFIFZJRVcgdndfc3VwcGx5X3ByaW9yaXR5IEFTCldJVEggdHJlbmQgQVMgKAogICAgU0VMRUNUIG91dGxldF9pZCwgQVZHKG1vbV9ncm93dGhfcGN0KSBBUyBhdmdfbW9tX2dyb3d0aAogICAgRlJPTSB2d19vdXRsZXRfbW9tX3RyZW5kCiAgICBXSEVSRSBtb21fZ3Jvd3RoX3BjdCBJUyBOT1QgTlVMTAogICAgR1JPVVAgQlkgb3V0bGV0X2lkCiksCmNvbWJpbmVkIEFTICgKICAgIFNFTEVDVAogICAgICAgIHMub3V0bGV0X2lkLAogICAgICAgIHMub3V0bGV0X2NvZGUsCiAgICAgICAgcy5sb2NhdGlvbl90eXBlLAogICAgICAgIHMuYnVzaW5lc3Nfc2VnbWVudCwKICAgICAgICBzLnJldmVudWVfcGVyY2VudGlsZSwKICAgICAgICBzLmRpZXNlbF9wZXJjZW50aWxlLAogICAgICAgIENPQUxFU0NFKHQuYXZnX21vbV9ncm93dGgsIDApICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBBUyBhdmdfbW9tX2dyb3d0aCwKICAgICAgICBQRVJDRU5UX1JBTksoKSBPVkVSIChPUkRFUiBCWSBDT0FMRVNDRSh0LmF2Z19tb21fZ3Jvd3RoLCAwKSkgICAgQVMgZ3Jvd3RoX3BlcmNlbnRpbGUKICAgIEZST00gdndfb3V0bGV0X3NlZ21lbnRzIHMKICAgIExFRlQgSk9JTiB0cmVuZCB0IE9OIHQub3V0bGV0X2lkID0gcy5vdXRsZXRfaWQKKQpTRUxFQ1QKICAgICosCiAgICBST1VORCgoMC40NSAqIHJldmVudWVfcGVyY2VudGlsZSArIDAuMzAgKiBkaWVzZWxfcGVyY2VudGlsZSArIDAuMjUgKiBncm93dGhfcGVyY2VudGlsZSk6Ok5VTUVSSUMsIDQpCiAgICAgICAgQVMgc3VwcGx5X3ByaW9yaXR5X3Njb3JlLAogICAgQ0FTRQogICAgICAgIFdIRU4gMC40NSAqIHJldmVudWVfcGVyY2VudGlsZSArIDAuMzAgKiBkaWVzZWxfcGVyY2VudGlsZSArIDAuMjUgKiBncm93dGhfcGVyY2VudGlsZSA+PSAwLjcwIFRIRU4gJ0hpZ2gnCiAgICAgICAgV0hFTiAwLjQ1ICogcmV2ZW51ZV9wZXJjZW50aWxlICsgMC4zMCAqIGRpZXNlbF9wZXJjZW50aWxlICsgMC4yNSAqIGdyb3d0aF9wZXJjZW50aWxlID49IDAuNDAgVEhFTiAnTWVkaXVtJwogICAgICAgIEVMU0UgJ0xvdycKICAgIEVORCBBUyBzdXBwbHlfcHJpb3JpdHlfdGllcgpGUk9NIGNvbWJpbmVkOwoKLS0gOS4xICBUb3AgMTAgb3V0bGV0cyBmb3IgbmV4dCBhbGxvY2F0aW9uIGN5Y2xlClNFTEVDVCBvdXRsZXRfY29kZSwgbG9jYXRpb25fdHlwZSwgYnVzaW5lc3Nfc2VnbWVudCwgc3VwcGx5X3ByaW9yaXR5X3Njb3JlLCBzdXBwbHlfcHJpb3JpdHlfdGllcgpGUk9NIHZ3X3N1cHBseV9wcmlvcml0eQpPUkRFUiBCWSBzdXBwbHlfcHJpb3JpdHlfc2NvcmUgREVTQwpMSU1JVCAxMDsKCgotLSA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PQotLSBTRUNUSU9OIDEwIC0tIEVYRUNVVElWRSBTVU1NQVJZICYgQUNUSU9OIFFVRVVFCi0tID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09Ci0tIDEwLjEgIFNpbmdsZS1yb3cgcm9sbHVwIC0tIHRoZSBudW1iZXJzIGEgbGVhZGVyc2hpcCBkZWNrIHdvdWxkIG9wZW4gd2l0aC4KQ1JFQVRFIE9SIFJFUExBQ0UgVklFVyB2d19leGVjdXRpdmVfc3VtbWFyeSBBUwpTRUxFQ1QKICAgIChTRUxFQ1QgQ09VTlQoKikgRlJPTSBvdXRsZXRzKSAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICBBUyB0b3RhbF9vdXRsZXRzLAogICAgKFNFTEVDVCBST1VORChTVU0odG90YWxfcmV2ZW51ZV82bW8pLCAwKSBGUk9NIHZ3X291dGxldF9wcm9maWxlKSAgICAgICAgICAgIEFTIHRvdGFsX3JldmVudWVfNm1vLAogICAgKFNFTEVDVCBDT1VOVCgqKSBGUk9NIHZ3X291dGxldF9zZWdtZW50cwogICAgICAgIFdIRVJFIGJ1c2luZXNzX3NlZ21lbnQgTElLRSAnU3RyYXRlZ2ljJScpICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgQVMgaGlnaF92YWx1ZV9vdXRsZXRzLAogICAgKFNFTEVDVCBDT1VOVCgqKSBGUk9NIHZ3X291dGxldF9hbm9tYWxpZXMgV0hFUkUgaXNfYW5vbWFseSkgICAgICAgICAgICAgICAgIEFTIGFub21hbHlfZmxhZ3MsCiAgICAoU0VMRUNUIENPVU5UKCopIEZST00gdndfc3VwcGx5X3ByaW9yaXR5IFdIRVJFIHN1cHBseV9wcmlvcml0eV90aWVyID0gJ0hpZ2gnKSBBUyBoaWdoX3ByaW9yaXR5X291dGxldHMsCiAgICAoU0VMRUNUIE1JTihyZXZlbnVlX3JhbmspIEZST00gdndfcGFyZXRvX3JldmVudWUgV0hFUkUgY3VtdWxhdGl2ZV9wY3RfcmV2ZW51ZSA+PSA4MCkKICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgQVMgb3V0bGV0c19kcml2aW5nXzgwcGN0X3JldmVudWU7CgpTRUxFQ1QgKiBGUk9NIHZ3X2V4ZWN1dGl2ZV9zdW1tYXJ5OwoKLS0gMTAuMiAgQWN0aW9uIHF1ZXVlIC0tIG91dGxldHMgdGhhdCBhcmUgQk9USCBoaWdoIHN1cHBseSBwcmlvcml0eSBBTkQKLS0gICAgICAgY3VycmVudGx5IGZsYWdnZWQgYW5vbWFsb3VzOiB0aGUgc2hvcnRsaXN0IGFuIG9wcyBtYW5hZ2VyIHNob3VsZAotLSAgICAgICBhY3R1YWxseSBjYWxsIHRoaXMgd2Vlay4KQ1JFQVRFIE9SIFJFUExBQ0UgVklFVyB2d19hY3Rpb25fcXVldWUgQVMKU0VMRUNUCiAgICBzcC5vdXRsZXRfY29kZSwKICAgIHNwLmxvY2F0aW9uX3R5cGUsCiAgICBzcC5idXNpbmVzc19zZWdtZW50LAogICAgc3Auc3VwcGx5X3ByaW9yaXR5X3RpZXIsCiAgICBhLnNhbGVzX21vbnRoICAgICAgQVMgYW5vbWFseV9tb250aCwKICAgIGEuel9zY29yZQpGUk9NIHZ3X3N1cHBseV9wcmlvcml0eSBzcApKT0lOIHZ3X291dGxldF9hbm9tYWxpZXMgYSBPTiBhLm91dGxldF9pZCA9IHNwLm91dGxldF9pZCBBTkQgYS5pc19hbm9tYWx5CldIRVJFIHNwLnN1cHBseV9wcmlvcml0eV90aWVyIElOICgnSGlnaCcsICdNZWRpdW0nKQpPUkRFUiBCWSBzcC5zdXBwbHlfcHJpb3JpdHlfdGllciwgQUJTKGEuel9zY29yZSkgREVTQzsKClNFTEVDVCAqIEZST00gdndfYWN0aW9uX3F1ZXVlOwoKCi0tID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09Ci0tIFNFQ1RJT04gMTEgLS0gQlVTSU5FU1MgUkVDT01NRU5EQVRJT05TCi0tID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09Ci0tICAxLiBQUk9URUNUIFRIRSBDT1JFOiAiU3RyYXRlZ2ljIEhpZ2gtVm9sdW1lIiBvdXRsZXRzIChTZWN0aW9uIDUvNikgZHJpdmUgYQotLSAgICAgZGlzcHJvcG9ydGlvbmF0ZSBzaGFyZSBvZiByZXZlbnVlIChzZWUgU2VjdGlvbiA3IFBhcmV0byBvdXRwdXQpIC0tIGxvY2sKLS0gICAgIGluIGd1YXJhbnRlZWQgbW9udGhseSBhbGxvY2F0aW9uIGFuZCBwcmlvcml0aXplIHRoZW0gaW4gYW55IHNob3J0YWdlLgotLSAgMi4gREUtUklTSyBMT0dJU1RJQ1MgQ09SUklET1JTOiAiRGllc2VsLUhlYXZ5IExvZ2lzdGljcyBIdWIiIGFuZAotLSAgICAgIlN0cmF0ZWdpYyBIaWdoLVZvbHVtZSAoRGllc2VsL0xvZ2lzdGljcykiIG91dGxldHMgc2l0IG9uIGhpZ2h3YXkKLS0gICAgIGNvcnJpZG9ycyB3aXRoIGhpZ2ggZGllc2VsX3JhdGlvIC0tIHRoZXNlIHN0b2NrIG91dCBmYXN0ZXN0IHVuZGVyCi0tICAgICBkaXNydXB0aW9uIGFuZCBzaG91bGQgZ2V0IHNob3J0ZXIgcmVwbGVuaXNobWVudCBjeWNsZXMsIG5vdCB0aGUgc2FtZQotLSAgICAgY2FkZW5jZSBhcyByZXRhaWwgb3V0bGV0cy4KLS0gIDMuIEdST1csIERPTidUIEpVU1QgTUFJTlRBSU4sIEhJR0gtRlJFUVVFTkNZIFJFVEFJTDogIkhpZ2gtRnJlcXVlbmN5IC8KLS0gICAgIExvdy1UaWNrZXQgUmV0YWlsIiBvdXRsZXRzIGhhdmUgdm9sdW1lIHVwc2lkZSB2aWEgbG95YWx0eS9zdWJzY3JpcHRpb24KLS0gICAgIHN0eWxlIHByb2dyYW1zIGV2ZW4gdGhvdWdoIGF2ZXJhZ2UgdGlja2V0IHNpemUgaXMgbG93LgotLSAgNC4gSU5WRVNUSUdBVEUsIERPTidUIElHTk9SRSwgVU5ERVJQRVJGT1JNRVJTOiAiVW5kZXJwZXJmb3JtaW5nIC8gTmVlZHMKLS0gICAgIFJldmlldyIgb3V0bGV0cyB3YXJyYW50IGEgc2l0ZSB2aXNpdCBiZWZvcmUgYXNzdW1pbmcgbG93IGRlbWFuZCBpcwotLSAgICAgcGVybWFuZW50IC0tIGNvdWxkIGJlIGEgbG9jYWwgY29tcGV0aXRpdmUgb3Igb3BlcmF0aW9uYWwgaXNzdWUuCi0tICA1LiBVU0UgVEhFIEFDVElPTiBRVUVVRSBPUEVSQVRJT05BTExZOiBTZWN0aW9uIDEwLjIgaXMgZGVzaWduZWQgdG8gYmUgcnVuCi0tICAgICBtb250aGx5IGFuZCBoYW5kZWQgZGlyZWN0bHkgdG8gdGhlIHN1cHBseSBwbGFubmluZyB0ZWFtIC0tIGl0IGlzIHRoZQotLSAgICAgb25lIHRhYmxlIHRoYXQgdHVybnMgdGhpcyBlbnRpcmUgcGlwZWxpbmUgaW50byBhIHJlcGVhdGFibGUgcHJvY2VzcwotLSAgICAgcmF0aGVyIHRoYW4gYSBvbmUtb2ZmIGFuYWx5c2lzLgotLSA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PQoKCi0tID09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09Ci0tIEFQUEVORElYIEEgLS0gT1BUSU9OQUwgQk9OVVM6IFBVUkUtU1FMIEstTUVBTlMgQVBQUk9YSU1BVElPTiAoaz00KQotLSA9PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PT09PQotLSAgTm90IHBhcnQgb2YgdGhlIGNvcmUgcGlwZWxpbmUgKHNlZSBTZWN0aW9uIDUgZm9yIHRoZSBwcm9kdWN0aW9uIGFwcHJvYWNoKS4KLS0gIEluY2x1ZGVkIG9ubHkgdG8gZGVtb25zdHJhdGUgdGhhdCB0aGUgdW5kZXJseWluZyBNTCBsb2dpYyBjYW4gYWxzbyBiZQotLSAgcmVwcm9kdWNlZCBuYXRpdmVseSBpbiBTUUwgdmlhIGl0ZXJhdGl2ZSBjZW50cm9pZCBhc3NpZ25tZW50IG9uCi0tICBtaW4tbWF4IG5vcm1hbGl6ZWQgZmVhdHVyZXMuIEZvdXIgZml4ZWQgc3RhcnRpbmcgY2VudHJvaWRzICsgb25lCi0tICByZWFzc2lnbm1lbnQgcGFzcyBpcyBlbm91Z2ggdG8gc2hvdyB0aGUgdGVjaG5pcXVlOyBhIHJlYWwgaW1wbGVtZW50YXRpb24KLS0gIHdvdWxkIGxvb3AgdGhpcyB0byBjb252ZXJnZW5jZSBpbiBhIHByb2NlZHVyYWwgYmxvY2sgKFBML3BnU1FMKSBvciBhbgotLSAgb3JjaGVzdHJhdGlvbiBsYXllci4KV0lUSCBub3JtYWxpemVkIEFTICgKICAgIFNFTEVDVAogICAgICAgIG91dGxldF9pZCwKICAgICAgICAoYXZnX3RvdGFsX2Z1ZWwgICAgIC0gTUlOKGF2Z190b3RhbF9mdWVsKSAgICAgT1ZFUiAoKSkgLyBOVUxMSUYoTUFYKGF2Z190b3RhbF9mdWVsKSAgICAgT1ZFUiAoKSAtIE1JTihhdmdfdG90YWxfZnVlbCkgICAgIE9WRVIgKCksIDApIEFTIG5fZnVlbCwKICAgICAgICAoYXZnX21vbnRobHlfdmlzaXRzIC0gTUlOKGF2Z19tb250aGx5X3Zpc2l0cykgT1ZFUiAoKSkgLyBOVUxMSUYoTUFYKGF2Z19tb250aGx5X3Zpc2l0cykgT1ZFUiAoKSAtIE1JTihhdmdfbW9udGhseV92aXNpdHMpIE9WRVIgKCksIDApIEFTIG5fdmlzaXRzCiAgICBGUk9NIHZ3X291dGxldF9wcm9maWxlCiksCmNlbnRyb2lkcyAoY2VudHJvaWRfaWQsIGNfZnVlbCwgY192aXNpdHMpIEFTICgKICAgIFZBTFVFUyAoMSwgMC45LCAwLjIpLCAoMiwgMC4yLCAwLjkpLCAoMywgMC41LCAwLjUpLCAoNCwgMC4xLCAwLjEpCiksCmRpc3RhbmNlcyBBUyAoCiAgICBTRUxFQ1QKICAgICAgICBuLm91dGxldF9pZCwKICAgICAgICBjLmNlbnRyb2lkX2lkLAogICAgICAgIFNRUlQoUE9XRVIobi5uX2Z1ZWwgLSBjLmNfZnVlbCwgMikgKyBQT1dFUihuLm5fdmlzaXRzIC0gYy5jX3Zpc2l0cywgMikpIEFTIGRpc3RhbmNlLAogICAgICAgIFJPV19OVU1CRVIoKSBPVkVSIChQQVJUSVRJT04gQlkgbi5vdXRsZXRfaWQgT1JERVIgQlkgU1FSVChQT1dFUihuLm5fZnVlbCAtIGMuY19mdWVsLCAyKSArIFBPV0VSKG4ubl92aXNpdHMgLSBjLmNfdmlzaXRzLCAyKSkpIEFTIHJuCiAgICBGUk9NIG5vcm1hbGl6ZWQgbgogICAgQ1JPU1MgSk9JTiBjZW50cm9pZHMgYwopClNFTEVDVCBvdXRsZXRfaWQsIGNlbnRyb2lkX2lkIEFTIGFwcHJveF9rbWVhbnNfY2x1c3RlciwgUk9VTkQoZGlzdGFuY2UsIDQpIEFTIGRpc3RhbmNlX3RvX2NlbnRyb2lkCkZST00gZGlzdGFuY2VzCldIRVJFIHJuID0gMQpPUkRFUiBCWSBvdXRsZXRfaWQKTElNSVQgMTU7Ci0tIE9ubHkgdGhlIGZpcnN0IDE1IHJvd3MgYXJlIHNob3duIGhlcmUgc2luY2UgdGhpcyBpcyBhIGRlbW8gb2YgdGVjaG5pcXVlLAotLSBub3QgdGhlIHByb2R1Y3Rpb24gc2VnbWVudGF0aW9uIC0tIHNlZSB2d19vdXRsZXRfc2VnbWVudHMgKFNlY3Rpb24gNSkgZm9yCi0tIHRoZSBhY3R1YWwsIGZ1bGx5LWV4cGxhaW5hYmxlIGxvZ2ljIHVzZWQgdGhyb3VnaG91dCB0aGUgcmVzdCBvZiB0aGlzIGZpbGUuCg==-- =============================================================================
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
