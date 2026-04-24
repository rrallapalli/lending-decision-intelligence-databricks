# Databricks notebook source
# MAGIC %md
# MAGIC # Strategy Simulation in Pyspark
# MAGIC
# MAGIC ## Goal:
# MAGIC ### 1- simulate alternate underwriting rules
# MAGIC ### 2 - compare growth vs risk tradeoff

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# COMMAND ----------

silver = spark.table("silver_lending")

# COMMAND ----------

# Optional quick validation
print("Total applications in silver_lending:", silver.count())

silver.select(
    F.count("*").alias("total_rows"),
    F.count("default_flag").alias("non_null_default_flag"),
    F.sum(F.when(F.col("default_flag").isNull(), 1).otherwise(0)).alias("null_default_flag_rows")
).show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Enhanced Approval Rules
# MAGIC ### Baseline:
# MAGIC #### - Core credit quality and affordability checks
# MAGIC #### - Slight tightening for self-employed borrowers
# MAGIC
# MAGIC ### Conservative:
# MAGIC #### - Higher score threshold
# MAGIC #### - Lower LTI
# MAGIC #### - Prefer salaried borrowers
# MAGIC #### - Avoid large ticket / long-tenure risk
# MAGIC
# MAGIC ### Growth:
# MAGIC #### - Lower score threshold for expansion
# MAGIC #### - Slightly higher LTI tolerance
# MAGIC #### - But still avoids weak combinations in risky products
# MAGIC
# MAGIC ### Risk Adjusted:
# MAGIC #### - Uses derived risk band directly
# MAGIC #### - Selectively allows growth while excluding clearly risky profiles

# COMMAND ----------

sim = (
    silver
    .withColumn(
        "approve_baseline",
        F.when(
            (F.col("bureau_score") >= 650) &
            (F.col("loan_to_income_ratio") < 0.45) &
            ~(
                (F.col("employment_type") == "Self-Employed") &
                (F.col("bureau_score") < 680)
            ),
            1
        ).otherwise(0)
    )
    .withColumn(
        "approve_conservative",
        F.when(
            (F.col("bureau_score") >= 700) &
            (F.col("loan_to_income_ratio") < 0.40) &
            (F.col("employment_type") == "Salaried") &
            (F.col("loan_amount") < 30000) &
            (F.col("tenure_months") > 36),
            1
        ).otherwise(0)
    )
    .withColumn(
        "approve_growth",
        F.when(
            (F.col("bureau_score") >= 620) &
            (F.col("loan_to_income_ratio") < 0.50) &
            ~(
                (F.col("product_type") == "Credit Line") &
                (F.col("bureau_score") < 650)
            ) &
            ~(
                (F.col("customer_segment") == "Mass") &
                (F.col("loan_to_income_ratio") > 0.50)
            ),
            1
        ).otherwise(0)
    )
    .withColumn(
        "approve_risk_adjusted",
        F.when(
            (F.col("risk_band") != "High Risk") &
            (F.col("loan_to_income_ratio") < 0.50) &
            ~(
                (F.col("employment_type") == "Self-Employed") &
                (F.col("bureau_score") < 660)
            ) &
            ~(
                (F.col("loan_amount") > 35000) &
                (F.col("bureau_score") < 700)
            ),
            1
        ).otherwise(0)
    )
)


# COMMAND ----------

# Quick approval diagnostics
sim.select(
    F.sum("approve_baseline").alias("baseline_approvals"),
    F.sum("approve_conservative").alias("conservative_approvals"),
    F.sum("approve_growth").alias("growth_approvals"),
    F.sum("approve_risk_adjusted").alias("risk_adjusted_approvals")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reshape to long format

# COMMAND ----------

baseline_df = (
    sim.filter(F.col("approve_baseline") == 1)
    .withColumn("strategy_name", F.lit("Baseline"))
)

conservative_df = (
    sim.filter(F.col("approve_conservative") == 1)
    .withColumn("strategy_name", F.lit("Conservative"))
)

growth_df = (
    sim.filter(F.col("approve_growth") == 1)
    .withColumn("strategy_name", F.lit("Growth"))
)

risk_adjusted_df = (
    sim.filter(F.col("approve_risk_adjusted") == 1)
    .withColumn("strategy_name", F.lit("Risk Adjusted"))
)

sim_long = (
    baseline_df
    .unionByName(conservative_df)
    .unionByName(growth_df)
    .unionByName(risk_adjusted_df)
)

print("Approved rows across all strategies:", sim_long.count())


# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregate by Reporting Month & Strategy

# COMMAND ----------

gold_strategy_simulation = (
    sim_long
    .groupBy("reporting_month", "strategy_name")
    .agg(
        F.count("*").alias("approved_count"),
        F.sum("loan_amount").alias("approved_disbursed"),
        F.sum("outstanding_balance").alias("approved_outstanding_balance"),
        F.sum(F.when(F.col("default_flag") == 1, 1).otherwise(0)).alias("approved_defaults"),
        F.sum(F.when(F.col("default_flag") == 1, F.col("loan_amount")).otherwise(0)).alias("approved_default_amount"),
        F.sum(F.when(F.col("default_flag") == 1, F.col("outstanding_balance")).otherwise(0)).alias("approved_default_outstanding_balance"),
        F.avg("default_flag").alias("expected_default_rate"),
        F.avg("loan_amount").alias("avg_loan_amount"),
        F.avg("bureau_score").alias("avg_bureau_score"),
        F.avg("loan_to_income_ratio").alias("avg_loan_to_income_ratio")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add monthly denominator from silver

# COMMAND ----------

monthly_apps = (
    silver.groupBy("reporting_month")
    .agg(F.count("*").alias("total_applications"))
)

gold_strategy_simulation = (
    gold_strategy_simulation
    .join(monthly_apps, on="reporting_month", how="left")
    .withColumn(
        "approval_rate",
        F.round(F.col("approved_count") / F.col("total_applications"), 4)
    )
    .withColumn(
        "expected_loss_proxy",
        F.round(F.col("approved_disbursed") * F.col("expected_default_rate"), 2)
    )
    .select(
        "reporting_month",
        "strategy_name",
        "total_applications",
        "approved_count",
        "approval_rate",
        "approved_disbursed",
        "approved_outstanding_balance",
        "approved_defaults",
        "approved_default_amount",
        "approved_default_outstanding_balance",
        "expected_default_rate",
        "avg_loan_amount",
        "avg_bureau_score",
        "avg_loan_to_income_ratio",
        "expected_loss_proxy"
    )
    .orderBy("reporting_month", "strategy_name")
)

# COMMAND ----------

# Save output
gold_strategy_simulation.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable("gold_strategy_simulation")

print("Saved table: gold_strategy_simulation")
spark.table("gold_strategy_simulation").show(100, truncate=False)

# COMMAND ----------

# Strategy summary across all months (for quick review)

strategy_summary = (
    spark.table("gold_strategy_simulation")
    .groupBy("strategy_name")
    .agg(
        F.sum("total_applications").alias("total_applications_sum"),
        F.sum("approved_count").alias("approved_count"),
        F.round(F.sum("approved_count") / F.sum("total_applications"), 4).alias("overall_approval_rate"),
        F.round(F.sum("approved_disbursed"), 2).alias("approved_disbursed"),
        F.sum("approved_defaults").alias("approved_defaults"),
        F.round(F.sum("approved_defaults") / F.sum("approved_count"), 4).alias("overall_expected_default_rate"),
        F.round(F.sum("approved_default_amount"), 2).alias("approved_default_amount"),
        F.round(F.sum("expected_loss_proxy"), 2).alias("expected_loss_proxy")
    )
    .orderBy(F.col("approved_count").desc())
)

print("Strategy summary across all months:")
strategy_summary.show(truncate=False)

# COMMAND ----------

spark.table("gold_strategy_simulation").groupBy("strategy_name").agg(
    F.sum("approved_count"),
    F.avg("approval_rate"),
    F.avg("expected_default_rate")
).show()

# COMMAND ----------

