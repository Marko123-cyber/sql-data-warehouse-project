# SQL Data Warehouse Project: CRM & ERP Integration

A practical implementation of a modern SQL Data Warehouse using **Microsoft SQL Server Express**. This project follows the **Medallion Architecture** (Bronze, Silver, Gold) and is based on the "Data with Baraa SQL Mastery Course", featuring **significant independent extensions**, advanced data quality testing, and custom analytics.

## 🏗️ Architecture Overview

The project implements a multi-layered data architecture designed for reliability and clean data flow:

| Layer | Purpose | Description |
| :--- | :--- | :--- |
| **Bronze** | Raw Ingestion | Land raw CSV data from CRM and ERP sources without transformation. |
| **Silver** | Cleaning & Standardization | **Independent ETL Logic**: Data type casting, null handling, deduplication, and schema enforcement. |
| **Gold** | Business Analysis | Aggregated views and reporting-ready tables for end-user analytics. |

sql-data-warehouse-project/
├── datasets/                 # Raw data sources (CSV)
│   ├── crm_source/          # Customer, Product, and Sales data
│   └── erp_source/          # Regional, Localization, and Category data
├── scripts/                  # SQL scripts for pipeline stages
│   ├── bronze/              # DDL and Data Loading for Bronze layer
│   ├── silver/              # DDL and ETL for Silver layer (Cleaning)
│   ├── gold/                # DDL for Gold layer (Business Views)
│   └── *.sql                # Analysis, Initialization and independent EDA scripts
├── tests/                    # Enhanced Data Quality and Integrity checks
│   ├── data_quality_inspection_*.sql  # In-depth validation beyond course curriculum
│   └── tables_overview_*.sql
└── Customer & Product Analytics.twbx  # Local Tableau Visualization Workbook

---

## 🚀 Getting Started

### 1. Prerequisites
- **Microsoft SQL Server Express** (installed locally).
- **SQL Server Management Studio (SSMS)**.
- **Tableau Desktop/Public** (for visualization).

### 2. Initialization
Run the [innit_database.sql](file:///home/marko/Desktop/data_warehouse_project/sql-data-warehouse-project/scripts/innit_database.sql) to set up the database structure and schemas.

### 3. Pipeline Execution
1.  **Bronze**: Ingest raw data using scripts in [scripts/bronze/](file:///home/marko/Desktop/data_warehouse_project/sql-data-warehouse-project/scripts/bronze/).
    > [!IMPORTANT]
    > Before running `data_loading_bronze.sql`, you **must** update the file paths within the script to match the location of the `datasets/` folder on your local machine.
2.  **Silver**: Perform cleaning using [scripts/silver/](file:///home/marko/Desktop/data_warehouse_project/sql-data-warehouse-project/scripts/silver/).
3.  **Gold**: Create business views using [scripts/gold/](file:///home/marko/Desktop/data_warehouse_project/sql-data-warehouse-project/scripts/gold/).

---

## 🔍 Independent Work & Data Quality
This project extends beyond the standard course material with:
- **Advanced Testing**: Comprehensive scripts in `tests/` for deep data inspection and quality assurance, ensuring higher integrity than standard exercises.
- **Custom ETL Logic**: Refined scripts for handling edge cases in CRM and ERP data integration.
- **Independent Problem Solving**: Tackled complex data consistency issues discovered during the EDA phase.

## 📊 Analytics & Interactive Dashboard
In addition to the SQL implementation, I developed a custom **Customer & Product Analytics** dashboard. This phase was done entirely independently to provide visual insights into sales trends and customer demographics.

🔗 **View the Interactive Dashboard here:**  
[Tableau Public - Customer & Product Analytics](https://public.tableau.com/app/profile/marko.pavlovic/viz/CustomerProductAnalytics/CustomerAnalytics)

---
> [!NOTE]
> This project was developed as part of a learning journey following the "Data with Baraa SQL Mastery Course", featuring independent exercises, advanced testing, and custom visualizations not included in the original curriculum.
