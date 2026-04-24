# Databricks notebook source
# MAGIC %md
# MAGIC # Transformation Layer
# MAGIC
# MAGIC ## Transform Silver layer in PySpark
# MAGIC
# MAGIC ### Goals: 
# MAGIC #### 1 - clean and enrich raw tables
# MAGIC #### 2 - create derived risk fields
# MAGIC #### 3 - produce one analytics-ready Silver table

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# COMMAND ----------

customers = spark.table("bronze_customers")
loans = spark.table("bronze_loans")
repayments = spark.table("bronze_repayments")
macro = spark.table("bronze_macro_signals")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join tables

# COMMAND ----------

# DBTITLE 1,Cell 5
silver = (
    loans.alias("l")
    .join(customers.alias("c"), on="customer_id", how="left")
    .join(repayments.alias("r"), on=["loan_id", "customer_id"], how="left")
    .select(
        F.col("l.loan_id"),
        F.col("l.customer_id"),
        F.col("l.product_type"),
        F.col("l.loan_amount"),
        F.col("l.interest_rate"),
        F.col("l.tenure_months"),
        F.col("l.origination_date"),

        F.col("c.age"),
        F.col("c.income"),
        F.col("c.employment_type"),
        F.col("c.geography"),
        F.col("c.acquisition_channel"),
        F.col("c.bureau_score"),
        F.col("c.customer_segment"),

        F.coalesce(F.col("r.delinquency_days"), F.lit(0)).cast("int").alias("delinquency_days"),
        F.coalesce(F.col("r.default_flag"), F.lit(0)).cast("int").alias("default_flag"),
        F.coalesce(F.col("r.outstanding_balance"), F.lit(0.0)).cast("double").alias("outstanding_balance")
    )
    .withColumn("reporting_month", F.trunc(F.col("origination_date"), "month"))
)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Add derived features
# MAGIC ### Add:
# MAGIC #### score_band
# MAGIC #### loan_to_income_ratio
# MAGIC #### risk_band
# MAGIC #### dpd_bucket

# COMMAND ----------

silver = (
    silver
    .withColumn(
        "ticket_size_segment",
        F.when(F.col("loan_amount") <= 5000, "upto 5K")
         .when(F.col("loan_amount") <= 10000, "5k - 10K")
         .when(F.col("loan_amount") <= 20000, "10K - 20K")
         .when(F.col("loan_amount") <= 50000, "20K - 50K")
         .otherwise("More than 50K")
    )
    .withColumn(
        "tenure_band",
        F.when(F.col("tenure_months") <= 12, "upto 12M")
         .when(F.col("tenure_months") <= 24, "12M - 24M")  
         .when(F.col("tenure_months") <= 36, "24M - 36M")
         .when(F.col("tenure_months") <= 60, "36M - 60M")
         .otherwise("More than 60M")
    )
    .withColumn(
        "customer_age_segment",
        F.when(F.col("age") <= 25, "upto 25")
         .when(F.col("age") <= 30, "26 - 30")
         .when(F.col("age") <= 35, "31 - 35")
         .when(F.col("age") <= 40, "36 - 40")
         .when(F.col("age") <= 45, "41 - 45")
         .when(F.col("age") <= 50, "46 - 50")
         .when(F.col("age") <= 55, "51 - 55")
         .when(F.col("age") <= 60, "56 - 60")
         .when(F.col("age") <= 65, "61 - 65")
         .otherwise("More than 65")
    )
    .withColumn(
        "score_band",
        F.when(F.col("bureau_score") < 580, "Very Low")
         .when(F.col("bureau_score") < 650, "Low")
         .when(F.col("bureau_score") < 720, "Medium")
         .otherwise("High")
    )
    .withColumn(
        "loan_to_income_ratio",
        F.round(F.col("loan_amount") / F.col("income"), 3)
    )
    .withColumn(
        "dpd_bucket",
        F.when(F.col("delinquency_days") < 30, "Current")
         .when(F.col("delinquency_days") < 60, "30-59")
         .when(F.col("delinquency_days") < 90, "60-89")
         .otherwise("90+")
    )
    .withColumn(
        "risk_band",
        F.when((F.col("bureau_score") >= 720) & (F.col("loan_to_income_ratio") < 0.30), "Low Risk")
         .when((F.col("bureau_score") >= 650) & (F.col("loan_to_income_ratio") < 0.45), "Medium Risk")
         .otherwise("High Risk")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Macro Data

# COMMAND ----------

silver = (
    silver.join(macro, on="reporting_month", how="left")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Silver Table

# COMMAND ----------

silver.write.mode("overwrite").format("delta").saveAsTable("silver_lending")

# COMMAND ----------

silver.summary().display()

# COMMAND ----------

# Query to Check Data

spark.table("silver_lending").select(
    F.count("*").alias("total_rows"),
    F.count("default_flag").alias("non_null_default_flag"),
    F.avg("default_flag").alias("overall_default_rate")
).show()

spark.table("silver_lending").groupBy("score_band").agg(
    F.count("*").alias("loans"),
    F.avg("default_flag").alias("default_rate")
).orderBy("score_band").show(truncate=False)

# COMMAND ----------

