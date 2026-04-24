# Databricks notebook source
# MAGIC %md
# MAGIC # KPI Layer
# MAGIC ## Build Gold Tables in Pyspark
# MAGIC ### Goals:
# MAGIC #### 1 - create business facing aggregate tables
# MAGIC #### 2 - each KPI is associated with one or more specific business questions

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# COMMAND ----------

silver = spark.table("silver_lending")

# COMMAND ----------

silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - Portfolio KPIs

# COMMAND ----------

# DBTITLE 1,Cell 6
gold_portfolio_kpis = (
    silver.groupBy("reporting_month", "customer_segment", "customer_age_segment", "employment_type", "geography", "acquisition_channel", "score_band", "risk_band", "product_type", "ticket_size_segment", "tenure_band", "dpd_bucket")
    .agg(
        F.count("*").alias("total_loans"),
        F.sum("loan_amount").alias("total_disbursed"),
        F.sum("outstanding_balance").alias("total_outstanding_balance"),
        F.sum(F.when(F.col("default_flag") == 1, 1).otherwise(0)).alias("total_defaults"),
        F.sum(F.when(F.col("default_flag") == 1, F.col("loan_amount")).otherwise(0)).alias("total_default_amount"),
        F.sum(F.when(F.col("default_flag") == 1, F.col("outstanding_balance")).otherwise(0)).alias("total_default_outstanding_balance"),
        F.avg("default_flag").alias("avg_default_rate"),
        F.avg("bureau_score").alias("avg_bureau_score")
    )
)

gold_portfolio_kpis.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable("gold_portfolio_kpis")

# COMMAND ----------

gold_portfolio_kpis.display()

# COMMAND ----------

