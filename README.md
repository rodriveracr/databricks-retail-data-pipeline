Markdown# Enterprise Retail Lakehouse: Automated Medallion Pipeline Architecture via Databricks Auto Loader, Delta Live Tables (DLT) & AI/BI Engine 🛒

[![Databricks](https://img.shields.io/badge/Platform-Databricks%20Lakehouse-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com/)
[![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake%20%2F%20Parquet-00ADD8?style=for-the-badge&logo=apachespark&logoColor=white)](https://delta.io/)
[![Unity Catalog](https://img.shields.io/badge/Governance-Unity%20Catalog-0052CC?style=for-the-badge)](https://docs.databricks.com/data-governance/unity-catalog/)
[![PySpark](https://img.shields.io/badge/Engine-PySpark%20%7C%20Structured%20Streaming-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://spark.apache.org/)
[![Orchestration](https://img.shields.io/badge/Orchestration-Lakeflow%20Jobs-success?style=for-the-badge)](https://docs.databricks.com/workflows/)

**Architected & Implemented by:** [Rodolfo Villarreal Rivera](https://github.com/rodriveracr)

---

## 📌 Executive & Technical Abstract

A production-grade, distributed retail analytics platform and lakehouse pipeline engineered on **Databricks**, implementing the **Medallion Architecture (Bronze -> Silver -> Gold)** to process transactional data from the *Online Retail II* enterprise repository (UCI Machine Learning Repository).

The platform continuously processes batch and streaming micro-batches using **Databricks Auto Loader** (`cloudFiles`) for automated file discovery and schema evolution. It enforces schema conformance and strict **Delta Expectations (Data Quality Contracts)** in the Silver layer, constructs multi-dimensional business marts in the Gold layer, orchestrates pipeline tasks end-to-end via **Databricks Jobs / Lakeflow Workflows** with automated SLA failure alerts, and exposes interactive business intelligence via serverless **AI/BI Executive Dashboards**.

---

## 🏗️ End-to-End System Architecture

```text
                  [ Partitioned Batch CSV Ingestion ]
                    (retail_batch_01.csv, 02.csv)
                                   │
                                   ▼  Auto Loader (cloudFiles / schema evolution)
   ┌──────────────────────────────────────────────────────────────┐
   │                         BRONZE LAYER                         │
   │  Raw Transaction Stream: bronze_retail_transactions          │
   │  - Unity Catalog Volume: /Volumes/.../retail_source/         │
   │  - Non-destructive append-only storage                       │
   │  - System metadata injection (_ingestion_timestamp)          │
   └──────────────────────────────────────────────────────────────┘
                                   │
                                   ▼  Structured Streaming / DLT Engine
   ┌──────────────────────────────────────────────────────────────┐
   │                         SILVER LAYER                         │
   │  Cleansed, Typed & Conformed Domain Tables                   │
   │  - silver_transactions: Type casting, 'C' prefix segregation │
   │  - Quality Constraints: Quantity > 0, UnitPrice > 0, PK NOT NULL
   │  - Derived metrics: revenue = Quantity * UnitPrice           │
   │  - Domain Disaggregation: silver_customers, silver_products  │
   └──────────────────────────────────────────────────────────────┘
                                   │
                                   ▼  Micro-batch Aggregations & Semantic Modeling
   ┌──────────────────────────────────────────────────────────────┐
   │                          GOLD LAYER                          │
   │  Curated Star-Schema Marts & Business Aggregations           │
   │  - gold_sales_summary: High-level executive performance KPIs │
   │  - gold_sales_by_country: Geographic sales concentration     │
   │  - gold_sales_by_product: Inventory velocity & SKU analytics │
   │  - gold_sales_by_customer: LTV & purchase cadence tracking   │
   │  - gold_sales_by_month: Run-rates & seasonality pacing       │
   └──────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
        [ Automated Databricks Lakeflow Job & AI/BI Dashboards ]
⚙️ Data Engineering Specifications by Medallion Layer1. Ingestion Layer (Bronze)Ingestion Engine: Databricks Auto Loader (spark.readStream.format("cloudFiles")).Source Path: Managed Unity Catalog Volume: /Volumes/workspace/default/retail_source/.Strategy: Incremental arrival detection across partition files (retail_batch_01.csv, retail_batch_02.csv), enforcing schema inference while preserving original raw records for end-to-end auditability:Pythonfrom pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp

@dp.table(
    name="bronze_retail_transactions",
    comment="Raw retail transactions ingested via Auto Loader from Unity Catalog Volume"
)
def bronze_retail_transactions():
    source_path = "/Volumes/workspace/default/retail_source/"
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(source_path)
        .withColumn("_ingestion_timestamp", current_timestamp())
    )
2. Conformation & Quality Layer (Silver)Financial Metric Derivation: Gross revenue computed at line-item granularity:revenue = Quantity * UnitPriceData Contracts & Expectations:Structural integrity: Quantity > 0 and UnitPrice > 0.Mandatory keys: InvoiceNo IS NOT NULL and StockCode IS NOT NULL.Segregation of canceled transactions: Systematic identification and flagging of reversal invoices prefixed with 'C'.Domain Disaggregation: Creation of cleaned fact and dimension foundations:silver_transactions: Fact table with sanitized data types, dates, and order status flags.silver_customers: Dimension table tracking active CustomerID entities and geographic origins.silver_products: Catalog dimension with canonical StockCode and standard descriptions.3. Analytics & Semantic Layer (Gold)gold_sales_summary: Executive summary rolling up Total Revenue, Total Orders, Unique Active Customers, and Units Sold.gold_sales_by_country: Geographical sales contribution and international revenue breakdown.gold_sales_by_product: High-velocity SKU ranking and underperforming inventory alerts.gold_sales_by_customer: Customer Lifetime Value (LTV) cohorts and transaction frequency metrics.gold_sales_by_month: Longitudinal time-series aggregates highlighting seasonality curves and monthly revenue progression.📂 Repository LayoutPlaintextdatabricks-retail-data-pipeline/
│
├── README.md                              # Enterprise system architecture documentation
├── .gitignore                             # Data files, checkpoints & local environment exclusions
│
├── transformations/                       # Medallion PySpark / Delta Live Tables modules
│   ├── bronze/
│   │   └── 01_bronze_ingestion.py         # Auto Loader stream ingestion
│   ├── silver/
│   │   ├── 02_silver_transactions.py      # Cleansing, typing, derived revenue & DQ rules
│   │   ├── 03_silver_customers.py         # Customer master domain model
│   │   └── 04_silver_products.py          # Product catalog domain model
│   └── gold/
│       ├── 05_gold_summary.py             # Enterprise executive summary KPI mart
│       ├── 06_gold_country.py             # Geographic sales distribution mart
│       ├── 07_gold_product.py             # Product performance & volume mart
│       ├── 08_gold_customer.py            # Customer LTV & frequency mart
│       └── 09_gold_month.py               # Monthly revenue run-rate & seasonality mart
│
├── Queries/
│   └── Global_Retail_Analytics.sql        # High-performance analytics & validation queries
│
├── Notebooks/
│   └── 00_bronze_exploration.sql         # Exploratory data profiling & source validation
│
├── Documentation/                         # Architecture blueprints and technical assets
│
└── docs/                                  # Execution traces and dashboard reporting
    ├── pipeline_graph.png                 # Databricks DAG execution trace
    └── executive_dashboard.png            # AI/BI executive reporting interface
🔍 Analytical SQL Queries (Global_Retail_Analytics.sql)SQL-- 1. Top Revenue Markets
SELECT 
    country,
    total_revenue,
    total_orders,
    ROUND(total_revenue / total_orders, 2) AS average_order_value
FROM gold_sales_by_country
ORDER BY total_revenue DESC;

-- 2. Top 10 SKU Performers by Sales Volume and Revenue
SELECT 
    stock_code,
    description,
    units_sold,
    total_revenue
FROM gold_sales_by_product
ORDER BY total_revenue DESC
LIMIT 10;
🔄 Orchestration, Scheduling & SLA AlertsOrchestrator: Databricks Workflows (Lakeflow Jobs)
 via Global_Retail_ETL_Job.Execution Paradigm: Scheduled micro-batch trigger executing the
 upstream DLT pipeline and downstream table refreshes.Observability & Operational SLAs: Native
alert hooks configured for execution status transitions:On-Failure: Immediate alert dispatch
 containing task run IDs and runtime exception traces.On-Success: Confirmation digest
 indicating successful processing, latency duration, and record throughput.📊 Analytics
 & Serving Layer: Global Executive DashboardConnected directly to Databricks Serverless
SQL Warehouses, the Global Executive Dashboard surfaces near-real-time business KPIs:Dashboard
 WidgetUnderlying TableBusiness InsightKPI Counter: Total Revenuegold_sales_summaryEnterprise
 gross financial revenueKPI Counter: Total Ordersgold_sales_summaryAggregate fulfilled order
volumeKPI Counter: Active Customersgold_sales_summaryDistinct purchasing customer countKPI
 Counter: Units Soldgold_sales_summaryTotal physical unit inventory turnoverBar Chart:
 Revenue by Countrygold_sales_by_countryMarket penetration and regional revenue distributionBar
 Chart: Top 10 Productsgold_sales_by_productRevenue driver SKU identificationLine Chart:
Monthly Pacinggold_sales_by_monthSeasonality trends and month-over-month run-rate🚀
Deployment & Replication StepsPrerequisitesA Databricks Workspace enabled with Unity
Catalog.Compute permissions to create Delta Live Tables / Lakeflow Pipelines and
Serverless SQL Warehouses.Step-by-Step DeploymentClone Repository:In Databricks,
navigate to Workspace -> Git Folders and clone:Bashhttps://github.com/rodriveracr/databricks-retail-
data-pipeline.git
Provision Storage Volume:In Catalog Explorer, create a Managed Volume named retail_source under your schema (e.g., workspace.default.retail_source).Stage Data Ingestion:Upload retail_batch_01.csv into the Volume path /Volumes/workspace/default/retail_source/.Deploy ETL Pipeline:Go to Delta Live Tables -> Create Pipeline.Target the repository's transformations/ directory as the pipeline source.Set the target schema to your Unity Catalog workspace destination.Trigger Initial Run:Execute Run pipeline and monitor DAG state progression across Bronze, Silver, and Gold.Simulate Incremental Streaming:Upload retail_batch_02.csv to /Volumes/workspace/default/retail_source/ and trigger an incremental run. Auto Loader will detect new files without reprocessing existing records.
