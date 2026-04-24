# ▶️ How to Run & Extend This Project  
### Lending Decision Intelligence – Databricks Lakehouse

---

## 🧭 Overview

This project is designed as both:

- a **reproducible demo** on Databricks Free Edition  
- a **blueprint for enterprise-scale implementation**

It follows a modular pipeline built on the Medallion Architecture.

---

## 🔄 End-to-End Workflow

The solution executes in the following sequence:

```text
01_ingest_bronze       → Raw data generation
02_transform_silver    → Feature engineering & enrichment
03_build_gold          → Aggregated business metrics
05_strategy_simulation → Underwriting strategy evaluation
04_ai_insight_layer    → Business insights with AI-styled narratives
06_validation          → Data quality & sanity checks
```

---

## 🔁 How to Replicate (Databricks Free Edition)

### Step 1 — Setup Environment

- Create a Databricks workspace (Free Edition is sufficient)  
- Use a default cluster / compute  

---

### Step 2 — Import Project

- Import notebooks into a workspace folder  
- Ensure all notebooks are in the same directory  

---

### Step 3 — Execute Pipeline

Run notebooks in the following order:

#### 1. `01_ingest_bronze`
- Generates synthetic datasets  
- Creates Bronze tables  

#### 2. `02_transform_silver`
- Joins datasets  
- Creates derived features:
  - score_band  
  - risk_band  
  - tenure_band  
  - ticket_size_segment  

#### 3. `03_build_gold`
- Builds aggregated portfolio KPIs  
- Creates `gold_portfolio_kpis`  

#### 4. `05_strategy_simulation`
- Applies underwriting strategies  
- Generates:
  - approval rate  
  - expected default rate  
  - expected loss proxy  

#### 5. `04_ai_insight_layer`
- Generates **business insights with AI-styled narratives**  
- Creates `gold_ai_insights`  

#### 6. `06_validation`
- Validates data across layers  
- Performs sanity checks  

---

### Step 4 — Build Dashboard

- Create a Databricks dashboard  
- Add datasets:
  - `gold_portfolio_kpis`
  - `gold_strategy_simulation`
  - `gold_ai_insights`  

- Configure visuals:
  - KPIs  
  - Trends  
  - Risk segmentation  
  - Strategy comparison  

---

## 📊 Dashboard JSON

A Databricks dashboard JSON export is included in the `dashboard/` folder.

This can be used as a reference/template to recreate the dashboard in another Databricks workspace. Depending on the target workspace, users may need to update:

- catalog and schema names  
- dataset/table references  
- dashboard source mappings  
- permissions and workspace-specific IDs  

### Required Tables

The dashboard expects the following Gold tables to exist:

- `gold_portfolio_kpis`
- `gold_strategy_simulation`
- `gold_ai_insights`

---

### Option B — Use Included Dashboard JSON

If supported in your Databricks environment, import the dashboard JSON from:

```text
dashboard/dashboard.json
```

After import:
- Validate dataset mappings  
- Update table references if needed  

👉 Treat the JSON as a **template**, not a guaranteed one-click deployment.
