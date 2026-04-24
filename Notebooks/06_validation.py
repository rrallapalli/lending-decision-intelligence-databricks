# Databricks notebook source
# MAGIC %md
# MAGIC # Validation notebook in PySpark
# MAGIC ## Goal
# MAGIC ### - row counts across Bronze / Silver / Gold
# MAGIC ### - nulls in critical Silver fields
# MAGIC ### - uniqueness of IDs
# MAGIC ### - whether default rates look sensible
# MAGIC ### - whether Gold totals reconcile back to Silver
# MAGIC ### - whether strategy outputs exist and vary meaningfully
# MAGIC ### - whether AI insight rows were created

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# COMMAND ----------

# Row Counts

bronze_customers = spark.table("bronze_customers")
bronze_loans = spark.table("bronze_loans")
bronze_macro_signals = spark.table("bronze_macro_signals")
silver_lending = spark.table("silver_lending")
gold_portfolio_kpis = spark.table("gold_portfolio_kpis")
gold_strategy_simulation = spark.table("gold_strategy_simulation")
gold_ai_insights = spark.table("gold_ai_insights")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Row Counts

# COMMAND ----------

print("\n=== ROW COUNTS ===")

row_counts = [
    ("bronze_customers", bronze_customers.count()),
    ("bronze_loans", bronze_loans.count()),
    ("bronze_macro_signals", bronze_macro_signals.count()),
    ("silver_lending", silver_lending.count()),
    ("gold_portfolio_kpis", gold_portfolio_kpis.count()),
    ("gold_strategy_simulation", gold_strategy_simulation.count()),
    ("gold_ai_insights", gold_ai_insights.count())
]

for table_name, row_count in row_counts:
    print(f"{table_name}: {row_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Null checks on key silver columns

# COMMAND ----------

print("\n=== NULL CHECKS: silver_lending ===")

silver_nulls = silver_lending.select(
    F.count("*").alias("total_rows"),
    F.sum(F.when(F.col("loan_id").isNull(), 1).otherwise(0)).alias("null_loan_id"),
    F.sum(F.when(F.col("customer_id").isNull(), 1).otherwise(0)).alias("null_customer_id"),
    F.sum(F.when(F.col("loan_amount").isNull(), 1).otherwise(0)).alias("null_loan_amount"),
    F.sum(F.when(F.col("outstanding_balance").isNull(), 1).otherwise(0)).alias("null_outstanding_balance"),
    F.sum(F.when(F.col("bureau_score").isNull(), 1).otherwise(0)).alias("null_bureau_score"),
    F.sum(F.when(F.col("default_flag").isNull(), 1).otherwise(0)).alias("null_default_flag"),
    F.sum(F.when(F.col("reporting_month").isNull(), 1).otherwise(0)).alias("null_reporting_month"),
    F.sum(F.when(F.col("score_band").isNull(), 1).otherwise(0)).alias("null_score_band"),
    F.sum(F.when(F.col("risk_band").isNull(), 1).otherwise(0)).alias("null_risk_band")
)

silver_nulls.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Uniqueness checks

# COMMAND ----------

print("\n=== UNIQUENESS CHECKS ===")

silver_lending.select(
    F.count("*").alias("total_rows"),
    F.countDistinct("loan_id").alias("distinct_loan_id"),
    F.countDistinct("customer_id").alias("distinct_customer_id")
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Default rate sanity checks

# COMMAND ----------

print("\n=== DEFAULT RATE SANITY CHECKS ===")

silver_lending.select(
    F.count("*").alias("total_loans"),
    F.sum("default_flag").alias("total_defaults"),
    F.round(F.avg("default_flag"), 4).alias("overall_default_rate")
).show(truncate=False)

print("\nDefault rate by score band:")
(
    silver_lending.groupBy("score_band")
    .agg(
        F.count("*").alias("total_loans"),
        F.sum("default_flag").alias("total_defaults"),
        F.round(F.avg("default_flag"), 4).alias("default_rate")
    )
    .orderBy("score_band")
    .show(truncate=False)
)

print("\nDefault rate by risk band:")
(
    silver_lending.groupBy("risk_band")
    .agg(
        F.count("*").alias("total_loans"),
        F.sum("default_flag").alias("total_defaults"),
        F.round(F.avg("default_flag"), 4).alias("default_rate")
    )
    .orderBy("risk_band")
    .show(truncate=False)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exposure sanity checks

# COMMAND ----------

print("\n=== EXPOSURE SANITY CHECKS ===")

silver_lending.select(
    F.round(F.sum("loan_amount"), 2).alias("total_disbursed"),
    F.round(F.sum("outstanding_balance"), 2).alias("total_outstanding_balance"),
    F.round(F.avg("loan_amount"), 2).alias("avg_loan_amount"),
    F.round(F.avg("outstanding_balance"), 2).alias("avg_outstanding_balance")
).show(truncate=False)

# --------------------------------------------------
# 7. Gold portfolio KPI sanity checks
# --------------------------------------------------
print("\n=== GOLD PORTFOLIO KPI CHECKS ===")

gold_portfolio_kpis.select(
    F.sum("total_loans").alias("sum_total_loans"),
    F.round(F.sum("total_disbursed"), 2).alias("sum_total_disbursed"),
    F.round(F.sum("total_outstanding_balance"), 2).alias("sum_total_outstanding_balance"),
    F.sum("total_defaults").alias("sum_total_defaults"),
    F.round(F.sum("total_default_amount"), 2).alias("sum_total_default_amount"),
    F.round(F.sum("total_default_outstanding_balance"), 2).alias("sum_total_default_outstanding_balance")
).show(truncate=False)

print("\nMonthly gold KPI trend check:")
(
    gold_portfolio_kpis.groupBy("reporting_month")
    .agg(
        F.sum("total_loans").alias("total_loans"),
        F.round(F.sum("total_disbursed"), 2).alias("total_disbursed"),
        F.sum("total_defaults").alias("total_defaults"),
        F.round(F.sum("total_defaults") / F.sum("total_loans"), 4).alias("default_rate")
    )
    .orderBy("reporting_month")
    .show(50, truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Strategy simulation validation

# COMMAND ----------

print("\n=== STRATEGY SIMULATION CHECKS ===")

gold_strategy_simulation.select(
    F.count("*").alias("strategy_rows"),
    F.countDistinct("strategy_name").alias("distinct_strategies"),
    F.min("reporting_month").alias("min_reporting_month"),
    F.max("reporting_month").alias("max_reporting_month")
).show(truncate=False)

print("\nStrategy summary:")
(
    gold_strategy_simulation.groupBy("strategy_name")
    .agg(
        F.sum("total_applications").alias("total_applications"),
        F.sum("approved_count").alias("approved_count"),
        F.round(F.sum("approved_count") / F.sum("total_applications"), 4).alias("overall_approval_rate"),
        F.round(F.sum("approved_disbursed"), 2).alias("approved_disbursed"),
        F.sum("approved_defaults").alias("approved_defaults"),
        F.round(F.sum("approved_defaults") / F.sum("approved_count"), 4).alias("overall_expected_default_rate"),
        F.round(F.sum("expected_loss_proxy"), 2).alias("expected_loss_proxy")
    )
    .orderBy(F.col("approved_count").desc())
    .show(truncate=False)
)

print("\nMonthly strategy trend:")
(
    gold_strategy_simulation.groupBy("reporting_month", "strategy_name")
    .agg(
        F.sum("approved_count").alias("approved_count"),
        F.round(F.sum("approved_defaults") / F.sum("approved_count"), 4).alias("expected_default_rate")
    )
    .orderBy("reporting_month", "strategy_name")
    .show(100, truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## AI insights validation

# COMMAND ----------

print("\n=== AI INSIGHTS CHECKS ===")

gold_ai_insights.select(
    F.count("*").alias("insight_count"),
    F.countDistinct("insight_category").alias("distinct_categories")
).show(truncate=False)

gold_ai_insights.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross-check silver vs gold totals

# COMMAND ----------

print("\n=== SILVER VS GOLD CROSS-CHECK ===")

silver_totals = (
    silver_lending.agg(
        F.count("*").alias("silver_total_loans"),
        F.round(F.sum("loan_amount"), 2).alias("silver_total_disbursed"),
        F.round(F.sum("outstanding_balance"), 2).alias("silver_total_outstanding_balance"),
        F.sum("default_flag").alias("silver_total_defaults")
    )
)

gold_totals = (
    gold_portfolio_kpis.agg(
        F.sum("total_loans").alias("gold_total_loans"),
        F.round(F.sum("total_disbursed"), 2).alias("gold_total_disbursed"),
        F.round(F.sum("total_outstanding_balance"), 2).alias("gold_total_outstanding_balance"),
        F.sum("total_defaults").alias("gold_total_defaults")
    )
)

silver_totals.show(truncate=False)
gold_totals.show(truncate=False)


# COMMAND ----------

print("\n=== WARNING CHECKS ===")

default_rate_value = silver_lending.select(F.avg("default_flag").alias("default_rate")).collect()[0]["default_rate"]

if default_rate_value is not None:
    if default_rate_value < 0.01:
        print("WARNING: Overall default rate is very low. Strategy differentiation may be weak.")
    elif default_rate_value > 0.40:
        print("WARNING: Overall default rate is very high. Synthetic risk logic may be too aggressive.")
    else:
        print("Overall default rate is within a reasonable demo range.")
else:
    print("WARNING: default_flag appears to be null or missing.")

strategy_count = gold_strategy_simulation.select(F.countDistinct("strategy_name").alias("cnt")).collect()[0]["cnt"]

if strategy_count < 4:
    print("WARNING: Fewer than 4 strategies found in gold_strategy_simulation.")
else:
    print("Expected strategy count present.")

print("\nValidation complete.")

# COMMAND ----------

