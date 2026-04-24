# Databricks notebook source
# MAGIC %md
# MAGIC # 📘 Data Dictionary
# MAGIC
# MAGIC ## 🥉 Bronze Layer Data Dictionary
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC The Bronze layer contains **raw, minimally processed data** that simulates ingestion from upstream systems.
# MAGIC
# MAGIC These tables act as:
# MAGIC - source-of-truth inputs
# MAGIC - reproducible raw datasets
# MAGIC - foundation for downstream transformations
# MAGIC
# MAGIC No business logic or aggregation is applied at this stage.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📄 Table: `bronze_customers`
# MAGIC
# MAGIC ### Description
# MAGIC Customer master dataset containing demographic, financial, and acquisition attributes.
# MAGIC
# MAGIC ### Columns
# MAGIC
# MAGIC | Column Name | Description |
# MAGIC |------------|------------|
# MAGIC | customer_id | Unique identifier for each customer |
# MAGIC | age | Customer age |
# MAGIC | income | Annual income |
# MAGIC | employment_type | Employment category (Salaried / Self-Employed / Other) |
# MAGIC | geography | Customer region (North / South / East / West) |
# MAGIC | acquisition_channel | Channel through which customer was acquired |
# MAGIC | bureau_score | Credit score of the customer |
# MAGIC | customer_segment | Income-based segmentation (Mass / Affluent / Premium) |
# MAGIC
# MAGIC ### Notes
# MAGIC - Used to derive risk and affordability features in Silver layer
# MAGIC - No transformations applied at Bronze stage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📄 Table: `bronze_loans`
# MAGIC
# MAGIC ### Description
# MAGIC Loan-level dataset capturing product, amount, and origination details.
# MAGIC
# MAGIC ### Columns
# MAGIC
# MAGIC | Column Name | Description |
# MAGIC |------------|------------|
# MAGIC | loan_id | Unique identifier for each loan |
# MAGIC | customer_id | Foreign key linking to customers |
# MAGIC | product_type | Type of loan (Personal Loan / Credit Line / Consumer Durable) |
# MAGIC | loan_amount | Total loan amount disbursed |
# MAGIC | interest_rate | Interest rate applied to the loan |
# MAGIC | tenure_months | Loan tenure in months |
# MAGIC | origination_date | Date of loan origination |
# MAGIC
# MAGIC ### Notes
# MAGIC - Represents loan origination system
# MAGIC - Used for exposure and portfolio analysis
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📄 Table: `bronze_macro_signals`
# MAGIC
# MAGIC ### Description
# MAGIC Synthetic macroeconomic dataset used to simulate external economic conditions.
# MAGIC
# MAGIC ### Columns
# MAGIC
# MAGIC | Column Name | Description |
# MAGIC |------------|------------|
# MAGIC | reporting_month | Monthly time dimension |
# MAGIC | inflation_rate | Simulated inflation rate |
# MAGIC | policy_rate | Simulated central bank policy rate |
# MAGIC | unemployment_proxy | Simulated unemployment indicator |
# MAGIC
# MAGIC ### Notes
# MAGIC - Joined in Silver layer to enrich loan data
# MAGIC - Used to influence synthetic risk generation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 Design Considerations
# MAGIC
# MAGIC - Bronze layer mimics ingestion from:
# MAGIC   - Customer systems
# MAGIC   - Loan origination systems
# MAGIC   - External macroeconomic feeds
# MAGIC
# MAGIC - Data is:
# MAGIC   - unvalidated
# MAGIC   - unaggregated
# MAGIC   - minimally transformed
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔄 Role in Pipeline
# MAGIC bronze_loans JOIN bronze_customers JOIN bronze_macro_signals → silver_lending
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC ## 🥈 Silver Layer
# MAGIC ---
# MAGIC ### Table: `silver_lending`
# MAGIC
# MAGIC #### Identifiers
# MAGIC - `loan_id` – Unique loan identifier
# MAGIC - `customer_id` – Unique customer identifier
# MAGIC
# MAGIC #### Customer Attributes
# MAGIC - `age` – Customer age
# MAGIC - `customer_segment` – Mass / Affluent / Premium
# MAGIC - `customer_age_segment` – Age bucket
# MAGIC - `employment_type` – Salaried / Self-employed / Other
# MAGIC - `geography` – Region
# MAGIC - `acquisition_channel` – Digital / Branch / Partner
# MAGIC - `bureau_score` – Credit score
# MAGIC
# MAGIC #### Loan Attributes
# MAGIC - `product_type` – Loan product
# MAGIC - `loan_amount` – Loan value
# MAGIC - `tenure_months` – Loan duration
# MAGIC - `interest_rate` – Interest rate
# MAGIC
# MAGIC #### Derived Features
# MAGIC - `loan_to_income_ratio`
# MAGIC - `score_band` – Credit score bucket
# MAGIC - `risk_band` – Derived risk category
# MAGIC - `ticket_size_segment`
# MAGIC - `tenure_band`
# MAGIC - `dpd_bucket`
# MAGIC
# MAGIC #### Outcome Variables
# MAGIC - `default_flag` – 1 if default
# MAGIC - `delinquency_days`
# MAGIC - `outstanding_balance`
# MAGIC
# MAGIC #### Time Dimension
# MAGIC - `reporting_month`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🥇 Gold Layer
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Table: `gold_portfolio_kpis`
# MAGIC
# MAGIC #### Dimensions
# MAGIC - reporting_month
# MAGIC - customer_segment
# MAGIC - customer_age_segment
# MAGIC - employment_type
# MAGIC - geography
# MAGIC - acquisition_channel
# MAGIC - score_band
# MAGIC - risk_band
# MAGIC - product_type
# MAGIC - ticket_size_segment
# MAGIC - tenure_band
# MAGIC - dpd_bucket
# MAGIC
# MAGIC #### Measures
# MAGIC - total_loans
# MAGIC - total_disbursed
# MAGIC - total_outstanding_balance
# MAGIC - total_defaults
# MAGIC - total_default_amount
# MAGIC - total_default_outstanding_balance
# MAGIC - avg_default_rate
# MAGIC - avg_bureau_score
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Table: `gold_strategy_simulation`
# MAGIC
# MAGIC #### Dimensions
# MAGIC - reporting_month
# MAGIC - strategy_name
# MAGIC
# MAGIC #### Measures
# MAGIC - total_applications
# MAGIC - approved_count
# MAGIC - approval_rate
# MAGIC - approved_disbursed
# MAGIC - approved_outstanding_balance
# MAGIC - approved_defaults
# MAGIC - approved_default_amount
# MAGIC - approved_default_outstanding_balance
# MAGIC - expected_default_rate
# MAGIC - avg_loan_amount
# MAGIC - avg_bureau_score
# MAGIC - expected_loss_proxy
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Table: `gold_ai_insights`
# MAGIC
# MAGIC #### Columns
# MAGIC - insight_category
# MAGIC - insight_text
# MAGIC
# MAGIC #### Description
# MAGIC Narrative summaries generated from portfolio and strategy data.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📊 Key Metrics Explained
# MAGIC
# MAGIC - **Default Rate**
# MAGIC   = total_defaults / total_loans
# MAGIC
# MAGIC - **Approval Rate**
# MAGIC   = approved_count / total_applications
# MAGIC
# MAGIC - **Expected Default Rate**
# MAGIC   = approved_defaults / approved_count
# MAGIC
# MAGIC - **Expected Loss Proxy**
# MAGIC   = approved_disbursed × expected_default_rate
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Notes
# MAGIC
# MAGIC - All metrics are aggregated from Silver layer
# MAGIC - Default rates should be interpreted as synthetic but realistic
# MAGIC - Strategy simulation reflects rule-based underwriting logic

# COMMAND ----------

