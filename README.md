# 🇰🇪 Kenya Food Price Data Platform

This project tracks food prices across different Kenyan regions, detects inflation patterns, and generates actionable insights for consumers, farmers, and policymakers. It combines scalable data engineering, dimensional modeling, and rich visual analytics using PySpark, PostgreSQL, and Grafana.

---

## 🧹 Project Highlights

- ⏱️ **ETL with PySpark**: Processes monthly/weekly food prices from open data sources
- ✅ **Data Cleaning & Normalization**: Handles unit mismatches, missing values, and date formatting
- ✨ **Star Schema Modeling**: Fact/dimension design for OLAP-style queries
- 📊 **Grafana Dashboards**: Filterable and interactive dashboards with geospatial mapping
- ⚡ **Inflation Detection**: Highlights anomalies and price shifts over time

---

## 🛠️ Tech Stack

| Layer        | Tools/Tech                         |
|--------------|------------------------------------|
| Ingestion    | PySpark (ETL), Python              |
| Storage      | PostgreSQL (schema: `food_prices`) |
| Modeling     | Star Schema (Fact + 4 Dimensions)  |
| Visualization| Grafana (Maps, Trends, Filters)    |

---

## 📁 Star Schema Structure

```plaintext
                  ┌───────────────┐
                  │    raw data   │
                  └───────────────┘
                         │
                  ┌────────────────────┐
                  │     fact_table     │
                  └────────────────────┘
                           │
         ┌──────────┐────────────────────┐───────────────┐
         │          │                    │               │
   ┌───────────┐ ┌────────────┐   ┌───────────┐ ┌─────────┐
   │date_dim   │ │location_dim│   │price_type │ │product_dim  │
   └───────────┘ └────────────┘   └───────────┘ └─────────┘
```

---

## 📊 Example Dashboards in Grafana

1. **National Average Prices by Commodity** 
2. **Map of Prices by County** 
3. **Retail vs Wholesale Trends** 
4. **Price Inflation (Month-over-Month %)** 
5. **Price Comparison by Region**

## Sample Grafana Visuals
  <img width="1345" height="575" alt="grafana" src="https://github.com/user-attachments/assets/f41d08f1-d5d8-41cd-a297-cc635f345da1" />

---

## Sample Query: National Avg Prices (Latest Month)



```sql
WITH latest_date AS (
  SELECT MAX(date) AS max_date FROM food_prices.date_dim
)
SELECT
  p.commodity,
  pt.price_type,
  ROUND(AVG(f.price)::numeric, 2) AS avg_price
FROM food_prices.fact_table f
JOIN food_prices.date_dim d ON f.date_id = d.date_id
JOIN food_prices.product_dim p ON f.product_id = p.product_id
JOIN food_prices.price_type_dim pt ON f.price_type_id = pt.price_type_id
JOIN latest_date ld ON d.date = ld.max_date
GROUP BY p.commodity, pt.price_type
ORDER BY avg_price DESC;
```

---

## 📖 How to Run

1. Clone this repo
2. Configure your `.env` file with PostgreSQL credentials
3. Run ETL pipeline with:

```bash
spark-submit food_price_etl.py
```

4. In Grafana:
   - Add PostgreSQL data source
   - Import dashboards from `grafana/`
---

## Potential Enhancements

- ✅ Integrate Debezium + Kafka for real-time CDC
- ✅ Add weather/NDVI datasets for richer correlation
- ✅ Forecast commodity prices using ML models (e.g., Prophet, XGBoost)
- ✅ Push inflation alerts to users via SMS or email

---

## 📄 License

MIT License
─────
