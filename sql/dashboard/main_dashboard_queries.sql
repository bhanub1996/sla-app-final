-- Replace ${catalog} and ${schema_prefix}.
-- KPI cards
SELECT * FROM ${catalog}.${schema_prefix}_gold.main_dashboard_kpis ORDER BY timestamp DESC LIMIT 1;

-- Demand vs stock movement
SELECT m.timestamp, AVG(m.demand_index) AS demand_index, SUM(s.closing_stock) AS stock_on_hand
FROM ${catalog}.${schema_prefix}_silver.fact_marketing_performance_hourly m
JOIN ${catalog}.${schema_prefix}_silver.fact_stock_movement_hourly s
  ON m.hour_key = s.hour_key AND m.sku = s.sku
GROUP BY m.timestamp ORDER BY m.timestamp;

-- Category demand lift
SELECT * FROM ${catalog}.${schema_prefix}_gold.category_demand_lift_current ORDER BY demand_lift_pct DESC;

-- Products to push now
SELECT priority_rank, sku, product_name, category, demand_spike_pct, cvr_pct, roas, stock_cover_hrs, push_score, action_type, decision
FROM ${catalog}.${schema_prefix}_gold.product_pushnow_current
ORDER BY priority_rank;

-- Dynamic action center
SELECT * FROM ${catalog}.${schema_prefix}_gold.recommended_actions_current_hour ORDER BY priority_rank;
