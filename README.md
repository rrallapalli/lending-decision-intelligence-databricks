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

![Strategy Definitions](images/Underwriting%20Strategies.png)

---

## 🧱 Architecture (Lakehouse Design)

![Medallion Architecture](images/Lending%20Decision%20Intelligence%20-%20Databricks%20Medallion%20Architecture.png)

## 📊 Dashboard (Databricks AI/BI)

The dashboard is built using **Gold tables as datasets (no SQL layer)**.

### Data Sources
- `gold_portfolio_kpis`
- `gold_strategy_simulation`
- `gold_ai_insights`

---

## 🧠 Demo Flow (Customer-Facing)

1. Portfolio health (KPIs + trends)  
![Executive Summary](images/Dashboard%20-%201.%20Executive%20Summary.png)

2. Monthly Portfolio Trends
![Portfolio Trends](images/Dashboard%20-%202.%20Portfolio%20Trends.png)

3. Risk segmentation
![Risk segmentation](images/Dashboard%20-%203.%20Risk%20Segmentation.png)

4. Exposure Analysis
![Portfolio Composition & Exposure](images/Dashboard%20-%204.%20Portfolio%20Composition%20&%20Exposure.png)

5. Strategy Comparison
![Strategy Comparison](images/Dashboard%20-%205.%20Strategy%20Simulation.png)

---

## 🛠️ Tech Stack

- Databricks Lakehouse  
- PySpark  
- Delta Tables  
- Databricks Dashboards (AI/BI)  
- Python (AI insight layer)  

---

## ▶️ How to Run

Refer [![How To Run]](docs/how_to_run.md)

---

## 💡 Key Insight

> This project moves from **descriptive analytics → decision intelligence**, enabling organizations to evaluate *what-if scenarios before changing lending policies*.

---

## 📬 Contact

If you're interested in discussing Data, AI, or Databricks solutions, feel free to connect.

Rakesh Rallapalli  
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue?logo=linkedin)](https://www.linkedin.com/in/rakesh-rallapalli/)
