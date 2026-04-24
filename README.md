# 📊 Lending Decision Intelligence on Databricks  
### Lakehouse Architecture + Strategy Simulation + AI Insights

---

## 🚀 What This Project Demonstrates

This project simulates how a financial institution can move from:

👉 **Raw lending data → Portfolio insights → Strategy decisions**

Built on the Databricks Lakehouse, it demonstrates:

- End-to-end **Medallion Architecture (Bronze → Silver → Gold)**
- **PySpark-based data pipelines & feature engineering**
- Portfolio analytics & risk segmentation
- **Underwriting strategy simulation (decision intelligence)**
- AI-generated business insights
- Dataset-driven dashboards (Databricks AI/BI)

---

## 🎯 Business Problem

Lenders must continuously balance:

- 📈 Growth → Increase approvals and disbursed volume  
- ⚠️ Risk → Control defaults and delinquency  
- 💰 Profitability → Optimize expected loss vs exposure  

Most analytics solutions answer:

> “What happened?”

But business stakeholders need:

> “What happens if we change our underwriting strategy?”

---

## 🧠 Key Differentiator: Strategy Simulation

This project introduces a **strategy simulation layer** to evaluate trade-offs between growth and risk.

### Underwriting Strategy Definitions

![Strategy Definitions](../images/Underwriting%Strategies.png)

---

## 🧱 Architecture (Lakehouse Design)

Bronze → Silver → Gold → Dashboard  
                     → Strategy Simulation  
                     → AI Insights  

---

## 📊 Dashboard (Databricks AI/BI)

The dashboard is built using **Gold tables as datasets (no SQL layer)**.

### Data Sources
- `gold_portfolio_kpis`
- `gold_strategy_simulation`
- `gold_ai_insights`

---

## 🧠 Demo Flow (Customer-Facing)

1. Portfolio health (KPIs + trends)  
2. Risk segmentation  
3. Exposure analysis  
4. Strategy comparison  
5. Recommendation  

---

## 🛠️ Tech Stack

- Databricks Lakehouse  
- PySpark  
- Delta Tables  
- Databricks Dashboards (AI/BI)  
- Python (AI insight layer)  

---

## ▶️ How to Run

01_ingest_bronze  
02_transform_silver  
03_build_gold  
05_strategy_simulation  
04_ai_insight_layer  
06_validation  

---

## 💡 Key Insight

> This project moves from **descriptive analytics → decision intelligence**, enabling organizations to evaluate *what-if scenarios before changing lending policies*.

---

## 📬 Contact

Add your LinkedIn here
