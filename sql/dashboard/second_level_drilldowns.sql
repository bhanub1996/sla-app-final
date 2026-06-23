-- Revenue drilldown
SELECT product_name, category, gross_revenue, margin_pct, roas, stock_cover_hrs, push_score, action_type, decision
FROM ${catalog}.${schema_prefix}_gold.product_pushnow_current ORDER BY gross_revenue DESC, push_score DESC;

-- Orders drilldown
SELECT product_name, category, orders, units_sold, cvr_pct, demand_spike_pct, stock_cover_hrs, push_score, action_type, decision
FROM ${catalog}.${schema_prefix}_gold.product_pushnow_current ORDER BY orders DESC, push_score DESC;

-- Conversion drilldown
SELECT product_name, category, cvr_pct, roas, demand_spike_pct, stock_cover_hrs, push_score, action_type, decision
FROM ${catalog}.${schema_prefix}_gold.product_pushnow_current ORDER BY cvr_pct DESC, push_score DESC;

-- ROAS drilldown
SELECT product_name, category, roas, gross_revenue, ad_spend, demand_spike_pct, stock_cover_hrs, push_score, action_type, decision
FROM ${catalog}.${schema_prefix}_gold.product_pushnow_current ORDER BY roas DESC, push_score DESC;

-- Stock drilldown
SELECT product_name, category, closing_stock, stock_cover_hrs, stock_risk_flag, demand_spike_pct, push_score, action_type, decision
FROM ${catalog}.${schema_prefix}_gold.product_pushnow_current ORDER BY stock_cover_hrs DESC, push_score DESC;

-- Budget shift drilldown
SELECT * FROM ${catalog}.${schema_prefix}_gold.channel_budget_shift_current ORDER BY roas DESC;
