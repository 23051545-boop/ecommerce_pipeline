# 🛒 E-Commerce Data Analytics Pipeline

> A production-style ETL pipeline that transforms raw messy CSV data into a structured SQLite data warehouse, served through an interactive Streamlit dashboard.

**Author:** Danyal Ahmad · Roll No. 2305534  
**Course:** Data Analytics (Industry Elective) · Semester 6 · KIIT University

---

## 📐 Architecture

```
Raw CSVs  →  Ingestion  →  Cleaning  →  Transformation  →  SQLite Warehouse  →  Dashboard
               Stage 1       Stage 2        Stage 3             Stage 4          Streamlit
```

### Star Schema

```
         dim_customers
         ─────────────
         customer_id PK
         name, city, segment
         age_group, tenure_days
               │ FK
               ▼
         fact_orders ────FK──▶  dim_products
         ───────────              ────────────
         order_id PK              product_id PK
         customer_id FK           product_name
         product_id  FK           category
         order_date               price_tier
         revenue, quantity        margin_pct
         status, channel
         is_outlier
```

---

## 🚀 Quick Start

### 1. Clone & install

```bash
git clone https://github.com/<your-username>/ecommerce-pipeline.git
cd ecommerce-pipeline
pip install -r requirements.txt
```

### 2. Run the pipeline (generates data + builds warehouse)

```bash
python main.py
```

### 3. Launch the dashboard

```bash
streamlit run dashboard/app.py
```

Open [http://localhost:8501](http://localhost:8501) in your browser.

---

## 📁 Project Structure

```
ecommerce-pipeline/
├── main.py                   # Orchestrator — run this
├── generate_data.py          # Synthetic dataset generator
├── data_ingestion.py         # Stage 1: Load & validate CSVs
├── data_cleaning.py          # Stage 2: Clean, deduplicate, impute
├── data_transformation.py    # Stage 3: Star schema + aggregations
├── data_loading.py           # Stage 4: SQLite warehouse + SQL reports
├── dashboard/
│   └── app.py                # Streamlit 6-tab dashboard
├── data/
│   ├── raw/                  # Auto-generated: customers, products, orders CSVs
│   ├── processed/            # Cleaned CSVs (audit trail)
│   ├── reports/              # 10 SQL report CSVs
│   └── ecommerce_warehouse.db  # SQLite warehouse (auto-created)
├── logs/
│   └── pipeline.log          # Full run logs
├── requirements.txt
└── README.md
```

---

## ⚙️ Pipeline Stages

| Stage | File | What it does |
|-------|------|-------------|
| 1 – Ingestion | `data_ingestion.py` | Loads CSVs, validates required columns, logs null counts |
| 2 – Cleaning | `data_cleaning.py` | Removes duplicates, median imputation, email validation, IQR outlier flagging |
| 3 – Transformation | `data_transformation.py` | Builds star schema with date parts, age groups, price tiers, denormalised fields |
| 4 – Loading | `data_loading.py` | Loads warehouse, runs 10 analytical SQL queries, exports CSV reports |

---

## 📊 Dashboard Tabs

| Tab | Content |
|-----|---------|
| 📊 KPIs | 6 metric cards + order status + payment breakdown |
| 📈 Revenue Trends | Monthly area chart, quarterly bars, day-of-week heatmap |
| 🏆 Top Performers | Top 10 products & top 10 customers |
| 📦 Category Analysis | Revenue share donut, grouped bar, treemap by price tier |
| 👥 Customer Insights | Segment, age group, top cities by revenue |
| 🔍 Data Explorer | Paginated table view + custom live SQL query box |

---

## 🛠️ Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Python 3.10+ |
| Data manipulation | Pandas, NumPy |
| Database | SQLite |
| Dashboard | Streamlit + Plotly |
| Scheduler | schedule (pip) |
| Logging | Python `logging` |

---

## 🔁 Scheduled Runs

Run the pipeline on a daily schedule (blocks):

```bash
python main.py --schedule
```

The pipeline runs immediately on start, then every day at 06:00.

---

## 📋 SQL Reports Generated

After running, find 10 CSV reports in `data/reports/`:

1. `01_total_kpis.csv` — Overall KPIs
2. `02_monthly_revenue.csv` — Monthly revenue trend
3. `03_top_10_customers.csv` — VIP customers
4. `04_best_selling_products.csv` — Top products
5. `05_category_analysis.csv` — Category breakdown
6. `06_quarterly_growth.csv` — QoQ growth
7. `07_status_breakdown.csv` — Order status split
8. `08_channel_performance.csv` — Sales channel analysis
9. `09_customer_segments.csv` — Segment revenue
10. `10_price_tier_revenue.csv` — Price tier performance

---

## 📝 License

MIT — free to use, modify, and distribute.
