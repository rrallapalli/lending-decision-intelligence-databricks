# Databricks notebook source
# MAGIC %md
# MAGIC # 🧱 Architecture: Lending Analytics Lakehouse
# MAGIC
# MAGIC ## 🧭 Overview
# MAGIC
# MAGIC The solution follows the Databricks **Medallion Architecture**:
# MAGIC
# MAGIC 👉 Bronze → Silver → Gold → Consumption (Dashboard + AI)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🥉 Bronze Layer (Raw Data)
# MAGIC
# MAGIC ### Purpose
# MAGIC - Store raw synthetic datasets
# MAGIC - Mimic ingestion from source systems
# MAGIC
# MAGIC ### Tables
# MAGIC - `bronze_customers`
# MAGIC - `bronze_loans`
# MAGIC - `bronze_macro_signals`
# MAGIC
# MAGIC ### Characteristics
# MAGIC - Minimal transformation
# MAGIC - Raw structure preserved
# MAGIC - Used as source of truth
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🥈 Silver Layer (Enriched Data)
# MAGIC
# MAGIC ### Purpose
# MAGIC - Join and clean datasets
# MAGIC - Create derived features
# MAGIC - Prepare data for analytics
# MAGIC
# MAGIC ### Table
# MAGIC - `silver_lending`
# MAGIC
# MAGIC ### Key Transformations
# MAGIC
# MAGIC - Join customers + loans + macro data
# MAGIC - Feature engineering:
# MAGIC   - `loan_to_income_ratio`
# MAGIC   - `score_band`
# MAGIC   - `risk_band`
# MAGIC   - `ticket_size_segment`
# MAGIC   - `tenure_band`
# MAGIC   - `dpd_bucket`
# MAGIC - Synthetic default generation using risk logic
# MAGIC
# MAGIC ### Output
# MAGIC A **feature-rich, analytics-ready dataset**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🥇 Gold Layer (Business Metrics)
# MAGIC
# MAGIC ### Purpose
# MAGIC - Provide aggregated business KPIs
# MAGIC - Enable dashboard consumption
# MAGIC
# MAGIC ### Tables
# MAGIC
# MAGIC #### `gold_portfolio_kpis`
# MAGIC - Multi-dimensional aggregation
# MAGIC - Metrics:
# MAGIC   - total loans
# MAGIC   - disbursed amount
# MAGIC   - outstanding balance
# MAGIC   - defaults
# MAGIC   - default exposure
# MAGIC   - default rate
# MAGIC
# MAGIC #### `gold_strategy_simulation`
# MAGIC - Strategy-level aggregated results
# MAGIC - Metrics:
# MAGIC   - approval rate
# MAGIC   - expected default rate
# MAGIC   - disbursed amount
# MAGIC   - expected loss proxy
# MAGIC
# MAGIC #### `gold_ai_insights`
# MAGIC - AI-generated narrative summaries
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 Strategy Simulation Layer
# MAGIC
# MAGIC ### Purpose
# MAGIC Simulate underwriting decisions using business rules.
# MAGIC
# MAGIC ### Strategies
# MAGIC
# MAGIC - Baseline
# MAGIC - Conservative
# MAGIC - Growth
# MAGIC - Risk Adjusted
# MAGIC
# MAGIC ### Inputs
# MAGIC - Credit score
# MAGIC - Loan-to-income ratio
# MAGIC - Employment type
# MAGIC - Product type
# MAGIC - Loan amount
# MAGIC - Risk band
# MAGIC
# MAGIC ### Outputs
# MAGIC - Approval counts
# MAGIC - Approval rates
# MAGIC - Default rates
# MAGIC - Expected loss
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Consumption Layer
# MAGIC
# MAGIC ### Dashboard
# MAGIC
# MAGIC Built using Databricks native dashboards with:
# MAGIC
# MAGIC - `gold_portfolio_kpis`
# MAGIC - `gold_strategy_simulation`
# MAGIC - `gold_ai_insights`
# MAGIC
# MAGIC ### Characteristics
# MAGIC - Dataset-driven (no SQL required)
# MAGIC - Interactive filters
# MAGIC - Multi-section layout
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🤖 AI Insight Layer
# MAGIC
# MAGIC ### Purpose
# MAGIC Convert analytics into narratives.
# MAGIC
# MAGIC ### Examples
# MAGIC - Highest risk segment
# MAGIC - Best growth strategy
# MAGIC - Lowest risk strategy
# MAGIC - Portfolio trend
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Data Flow Summary
# MAGIC
# MAGIC Bronze → Silver → Gold → Dashboard → Strategy Simulation → AI Insights

# COMMAND ----------

