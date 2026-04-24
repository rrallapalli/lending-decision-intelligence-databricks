# 📊 Dashboard Walkthrough  
### Lending Decision Intelligence – Databricks AI/BI

---

## 🧭 Overview

This dashboard provides a comprehensive view of a lending portfolio, enabling users to:

- Monitor portfolio health  
- Identify risk concentration  
- Analyze exposure across segments  
- Evaluate underwriting strategies  
- Interpret insights through AI-styled narratives  

The dashboard is built using curated Gold layer datasets in Databricks.

---

## 🗂️ Data Sources

- `gold_portfolio_kpis` → Portfolio metrics & segmentation  
- `gold_strategy_simulation` → Strategy-level outcomes  
- `gold_ai_insights` → Narrative insights  

---

## 🎯 Target Users

- Risk Analysts  
- Credit Strategy Teams  
- Product Managers  
- Business Stakeholders  

---

## 🧩 Dashboard Structure & Business Value

---

## 1️⃣ Executive Overview

### What it shows
- Total loans  
- Total disbursed  
- Outstanding balance  
- Portfolio default rate  

### Why it matters
- Quick snapshot of portfolio scale and risk  
- Helps leadership assess overall health  

---

## 2️⃣ Portfolio Trends

### What it shows
- Disbursed trend over time  
- Default rate trend  
- Outstanding exposure trend  

### Why it matters
- Identifies growth vs risk patterns  
- Detects early signals of portfolio deterioration  

---

## 3️⃣ Risk Segmentation

### What it shows
- Default rate by score band  
- Default rate by risk band  
- Segment-level risk variations  

### Why it matters
- Highlights high-risk customer segments  
- Validates effectiveness of risk segmentation  
- Enables targeted interventions  

---

## 4️⃣ Portfolio Composition

### What it shows
- Product distribution  
- Geography exposure  
- Channel contribution  
- Ticket size mix  

### Why it matters
- Identifies concentration risk  
- Helps optimize portfolio mix  
- Supports business expansion decisions  

---

## 5️⃣ Strategy Simulation (Core Section)

### What it shows
- Approval rate by strategy  
- Expected default rate  
- Expected loss proxy  
- Growth vs risk trade-off  

### Why it matters
- Compares alternative underwriting strategies  
- Helps balance growth and risk  
- Enables data-driven policy decisions  

---

## 6️⃣ Business Insights with AI-styled narratives

### What it shows
- Key portfolio observations  
- Strategy insights  
- Trend summaries  

### Why it matters
- Converts data into business-friendly narratives  
- Reduces manual interpretation effort  
- Improves stakeholder communication  

---

## 🔍 Filters & Interactivity

Users can filter by:

- Reporting month  
- Product type  
- Geography  
- Customer segment  
- Score band  
- Risk band  

### Why it matters
- Enables slice-and-dice analysis  
- Supports exploratory workflows  
- Allows targeted investigation  

---

## 🧠 Suggested Demo Flow

1. Start with Executive KPIs  
2. Analyze trends to understand portfolio trajectory  
3. Drill into segmentation to identify risk hotspots  
4. Review portfolio composition  
5. Compare strategies for decision-making  
6. Summarize insights using AI narratives  

---

## 💡 Key Takeaway

This dashboard enables a transition from:

👉 **Descriptive Analytics (What happened)**  
to  
👉 **Decision Intelligence (What should we do next)**  

---

## ⚠️ Notes

- Built on Databricks Free Edition  
- Uses synthetic data  
- AI insights are lightweight narrative generation  

---

## 🚀 Extension (Enterprise Setup)

In a production Databricks environment, this can be extended with:

- Unity Catalog (governance)  
- Row-level security  
- Mosaic AI / Genie for conversational analytics  
- Real-time data pipelines  

---