# Databricks notebook source
# MAGIC %md
# MAGIC # Raw Data Ingestion Layer 
# MAGIC
# MAGIC ## Build Bronze layer in PySpark
# MAGIC
# MAGIC ### Create Synthetic Dataset (Bronze Layer)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# COMMAND ----------

# Generate Customer Data & Create Customers Bronze Table

n_customers = 3000

customers = (
    spark.range(1, n_customers + 1)
    .withColumnRenamed("id", "customer_id")
    .withColumn("age", (F.rand(seed=1) * 35 + 21).cast("int"))
    .withColumn("income", (F.rand(seed=2) * 90000 + 10000).cast("int"))
    .withColumn(
        "employment_type",
        F.when(F.rand(seed=3) < 0.60, "Salaried")
         .when(F.rand(seed=3) < 0.85, "Self-Employed")
         .otherwise("Other")
    )
    .withColumn(
        "geography",
        F.when(F.rand(seed=4) < 0.25, "North")
         .when(F.rand(seed=4) < 0.50, "South")
         .when(F.rand(seed=4) < 0.75, "West")
         .otherwise("East")
    )
    .withColumn(
        "acquisition_channel",
        F.when(F.rand(seed=5) < 0.40, "Digital")
         .when(F.rand(seed=5) < 0.75, "Branch")
         .otherwise("Partner")
    )
    .withColumn("bureau_score", (F.rand(seed=6) * 350 + 450).cast("int"))
    .withColumn(
        "customer_segment",
        F.when(F.col("income") < 30000, "Mass")
         .when(F.col("income") < 70000, "Affluent")
         .otherwise("Premium")
    )
)

customers.write.mode("overwrite").format("delta").saveAsTable("bronze_customers")


# COMMAND ----------

# Generate Loans Data & Create Loans Bronze Table

n_loans = 7000

loans = (
    spark.range(1, n_loans + 1)
    .withColumnRenamed("id", "loan_id")
    .withColumn("customer_id", (F.rand(seed=10) * n_customers + 1).cast("int"))
    .withColumn(
        "product_type",
        F.when(F.rand(seed=11) < 0.50, "Personal Loan")
         .when(F.rand(seed=11) < 0.80, "Credit Line")
         .otherwise("Consumer Durable")
    )
    .withColumn("loan_amount", (F.rand(seed=12) * 45000 + 5000).cast("int"))
    .withColumn("interest_rate", F.round(F.rand(seed=13) * 10 + 8, 2))
    .withColumn("tenure_months", (F.rand(seed=14) * 48 + 12).cast("int"))
    .withColumn(
        "origination_date",
        F.expr("date_add(to_date('2023-01-01'), cast(rand(15) * 820 as int))")
    )
)

loans.write.mode("overwrite").format("delta").saveAsTable("bronze_loans")


# COMMAND ----------

# Generate Repayments Data & Create Repayments Bronze Table

loan_risk_base = (
    loans.alias("l")
    .join(customers.alias("c"), on="customer_id", how="left")
    .select(
        F.col("l.loan_id"),
        F.col("l.customer_id"),
        F.col("l.loan_amount"),
        F.col("l.interest_rate"),
        F.col("l.origination_date"),
        F.col("c.income"),
        F.col("c.bureau_score"),
        F.col("c.employment_type")
    )
    .withColumn("loan_to_income_ratio_raw", F.col("loan_amount") / F.col("income"))
    .withColumn(
        "score_risk_component",
        F.when(F.col("bureau_score") < 580, 0.18)
         .when(F.col("bureau_score") < 650, 0.10)
         .when(F.col("bureau_score") < 720, 0.05)
         .otherwise(0.02)
    )
    .withColumn(
        "lti_risk_component",
        F.when(F.col("loan_to_income_ratio_raw") >= 0.60, 0.16)
         .when(F.col("loan_to_income_ratio_raw") >= 0.45, 0.10)
         .when(F.col("loan_to_income_ratio_raw") >= 0.30, 0.05)
         .otherwise(0.01)
    )
    .withColumn(
        "employment_risk_component",
        F.when(F.col("employment_type") == "Self-Employed", 0.04)
         .when(F.col("employment_type") == "Other", 0.03)
         .otherwise(0.01)
    )
    .withColumn(
        "rate_risk_component",
        F.when(F.col("interest_rate") >= 15, 0.05)
         .when(F.col("interest_rate") >= 12, 0.03)
         .otherwise(0.01)
    )
    .withColumn("random_risk_component", F.rand(seed=40) * 0.04)
    .withColumn(
        "default_probability",
        F.least(
            F.lit(0.75),
            F.col("score_risk_component")
            + F.col("lti_risk_component")
            + F.col("employment_risk_component")
            + F.col("rate_risk_component")
            + F.col("random_risk_component")
        )
    )
    .withColumn(
        "default_flag",
        F.when(F.rand(seed=41) < F.col("default_probability"), 1).otherwise(0)
    )
    .withColumn(
        "delinquency_days",
        F.when(F.col("default_flag") == 1, (F.rand(seed=42) * 90 + 90).cast("int"))
         .when(F.col("default_probability") > 0.18, (F.rand(seed=43) * 60 + 20).cast("int"))
         .otherwise((F.rand(seed=44) * 25).cast("int"))
    )
    .withColumn(
        "outstanding_balance",
        F.round(
            F.col("loan_amount") * (
                F.when(F.col("default_flag") == 1, F.rand(seed=45) * 0.50 + 0.35)
                 .otherwise(F.rand(seed=46) * 0.50 + 0.10)
            ),
            2
        )
    )
    .select(
        "loan_id",
        "customer_id",
        "delinquency_days",
        "default_flag",
        "outstanding_balance"
    )
)

loan_risk_base.write.mode("overwrite").format("delta").saveAsTable("bronze_repayments")


# COMMAND ----------

# Generate Macro Signals Data & Create Macro Signals Bronze Table

macro = (
    spark.sql("SELECT sequence(to_date('2023-01-01'), to_date('2025-03-01'), interval 1 month) as months")
    .select(F.explode("months").alias("reporting_month"))
    .withColumn("inflation_rate", F.round(F.rand(seed=30) * 4 + 3, 2))
    .withColumn("policy_rate", F.round(F.rand(seed=31) * 3 + 5, 2))
    .withColumn("unemployment_proxy", F.round(F.rand(seed=32) * 3 + 4, 2))
)

macro.write.mode("overwrite").format("delta").saveAsTable("bronze_macro_signals")


# COMMAND ----------

