# Databricks notebook source
# MAGIC %md
# MAGIC # AI-style insight layer in PySpark
# MAGIC ## Goal:
# MAGIC ### 1 - generate simple, business-friendly narrative insights from Gold tables
# MAGIC ### 2 - keep it lightweight for Free Edition

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# COMMAND ----------

# Read Gold Tables

portfolio = spark.table("gold_portfolio_kpis")
strategy = spark.table("gold_strategy_simulation")

print("gold_portfolio_kpis rows:", portfolio.count())
print("gold_strategy_simulation rows:", strategy.count())


# COMMAND ----------

# MAGIC %md
# MAGIC ## Build portfolio-level summary views

# COMMAND ----------

# Highest risk segment from portfolio slices
segment_risk = (
    portfolio
    .groupBy("customer_segment", "product_type", "score_band", "risk_band")
    .agg(
        F.sum("total_loans").alias("total_loans"),
        F.sum("total_defaults").alias("total_defaults"),
        F.sum("total_disbursed").alias("total_disbursed"),
        F.sum("total_outstanding_balance").alias("total_outstanding_balance")
    )
    .withColumn(
        "default_rate",
        F.when(F.col("total_loans") > 0, F.col("total_defaults") / F.col("total_loans"))
         .otherwise(None)
    )
)

top_risk_rows = (
    segment_risk
    .filter(F.col("default_rate").isNotNull())
    .orderBy(F.col("default_rate").desc(), F.col("total_loans").desc())
    .limit(1)
    .collect()
)

top_risk_segment = top_risk_rows[0] if top_risk_rows else None

# Monthly trend summary
monthly_summary = (
    portfolio
    .groupBy("reporting_month")
    .agg(
        F.sum("total_loans").alias("total_loans"),
        F.sum("total_defaults").alias("total_defaults"),
        F.sum("total_disbursed").alias("total_disbursed"),
        F.sum("total_outstanding_balance").alias("total_outstanding_balance")
    )
    .withColumn(
        "default_rate",
        F.when(F.col("total_loans") > 0, F.col("total_defaults") / F.col("total_loans"))
         .otherwise(None)
    )
)

latest_month_rows = (
    monthly_summary
    .filter(F.col("reporting_month").isNotNull())
    .orderBy(F.col("reporting_month").desc())
    .limit(1)
    .collect()
)

earliest_month_rows = (
    monthly_summary
    .filter(F.col("reporting_month").isNotNull())
    .orderBy(F.col("reporting_month").asc())
    .limit(1)
    .collect()
)

latest_month = latest_month_rows[0] if latest_month_rows else None
earliest_month = earliest_month_rows[0] if earliest_month_rows else None

# Product risk summary
product_summary = (
    portfolio
    .groupBy("product_type")
    .agg(
        F.sum("total_loans").alias("total_loans"),
        F.sum("total_defaults").alias("total_defaults"),
        F.sum("total_disbursed").alias("total_disbursed")
    )
    .withColumn(
        "default_rate",
        F.when(F.col("total_loans") > 0, F.col("total_defaults") / F.col("total_loans"))
         .otherwise(None)
    )
)

top_product_rows = (
    product_summary
    .filter(F.col("default_rate").isNotNull())
    .orderBy(F.col("default_rate").desc(), F.col("total_disbursed").desc())
    .limit(1)
    .collect()
)

top_product = top_product_rows[0] if top_product_rows else None

# Geography risk summary
geo_summary = (
    portfolio
    .groupBy("geography")
    .agg(
        F.sum("total_loans").alias("total_loans"),
        F.sum("total_defaults").alias("total_defaults"),
        F.sum("total_disbursed").alias("total_disbursed")
    )
    .withColumn(
        "default_rate",
        F.when(F.col("total_loans") > 0, F.col("total_defaults") / F.col("total_loans"))
         .otherwise(None)
    )
)

top_geo_rows = (
    geo_summary
    .filter(F.col("default_rate").isNotNull())
    .orderBy(F.col("default_rate").desc(), F.col("total_disbursed").desc())
    .limit(1)
    .collect()
)

top_geo = top_geo_rows[0] if top_geo_rows else None


# COMMAND ----------

# MAGIC %md
# MAGIC ## Build strategy summary across all months

# COMMAND ----------

strategy_summary = (
    strategy
    .groupBy("strategy_name")
    .agg(
        F.sum("total_applications").alias("total_applications"),
        F.sum("approved_count").alias("approved_count"),
        F.sum("approved_disbursed").alias("approved_disbursed"),
        F.sum("approved_outstanding_balance").alias("approved_outstanding_balance"),
        F.sum("approved_defaults").alias("approved_defaults"),
        F.sum("approved_default_amount").alias("approved_default_amount"),
        F.sum("approved_default_outstanding_balance").alias("approved_default_outstanding_balance")
    )
    .withColumn(
        "approval_rate",
        F.when(F.col("total_applications") > 0, F.col("approved_count") / F.col("total_applications"))
         .otherwise(None)
    )
    .withColumn(
        "expected_default_rate",
        F.when(F.col("approved_count") > 0, F.col("approved_defaults") / F.col("approved_count"))
         .otherwise(None)
    )
    .withColumn(
        "expected_loss_proxy",
        F.when(F.col("approved_disbursed").isNotNull() & F.col("expected_default_rate").isNotNull(),
               F.col("approved_disbursed") * F.col("expected_default_rate"))
         .otherwise(None)
    )
)

best_growth_rows = (
    strategy_summary
    .filter(F.col("approval_rate").isNotNull())
    .orderBy(F.col("approval_rate").desc(), F.col("approved_count").desc())
    .limit(1)
    .collect()
)

lowest_risk_rows = (
    strategy_summary
    .filter(F.col("expected_default_rate").isNotNull())
    .orderBy(F.col("expected_default_rate").asc(), F.col("approved_count").desc())
    .limit(1)
    .collect()
)

best_growth_strategy = best_growth_rows[0] if best_growth_rows else None
lowest_risk_strategy = lowest_risk_rows[0] if lowest_risk_rows else None


# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute Trend Direction

# COMMAND ----------

trend_direction = "stable"
default_rate_change = None

if latest_month is not None and earliest_month is not None:
    earliest_default = earliest_month["default_rate"]
    latest_default = latest_month["default_rate"]

    if earliest_default is not None and latest_default is not None:
        default_rate_change = latest_default - earliest_default

        if default_rate_change > 0.01:
            trend_direction = "increasing"
        elif default_rate_change < -0.01:
            trend_direction = "improving"
        else:
            trend_direction = "stable"


# COMMAND ----------

# MAGIC %md
# MAGIC ## Build narrative insights

# COMMAND ----------

insights = []

if top_risk_segment is not None:
    insights.append((
        "risk_segment",
        f"Highest risk portfolio slice is {top_risk_segment['customer_segment']} / "
        f"{top_risk_segment['product_type']} / {top_risk_segment['score_band']} / "
        f"{top_risk_segment['risk_band']} with default rate {top_risk_segment['default_rate']:.2%}."
    ))
else:
    insights.append(("risk_segment", "No valid risk segment insight available."))

if top_product is not None:
    insights.append((
        "product_risk",
        f"Highest risk product is {top_product['product_type']} with default rate "
        f"{top_product['default_rate']:.2%} on disbursed amount {top_product['total_disbursed']:.2f}."
    ))
else:
    insights.append(("product_risk", "No valid product risk insight available."))

if top_geo is not None:
    insights.append((
        "geography_risk",
        f"Highest risk geography is {top_geo['geography']} with default rate "
        f"{top_geo['default_rate']:.2%}."
    ))
else:
    insights.append(("geography_risk", "No valid geography risk insight available."))

if best_growth_strategy is not None:
    insights.append((
        "growth_strategy",
        f"Highest growth strategy is {best_growth_strategy['strategy_name']} with approval rate "
        f"{best_growth_strategy['approval_rate']:.2%} and approved count {best_growth_strategy['approved_count']}."
    ))
else:
    insights.append(("growth_strategy", "No valid growth strategy insight available."))

if lowest_risk_strategy is not None:
    insights.append((
        "lowest_risk_strategy",
        f"Lowest risk strategy is {lowest_risk_strategy['strategy_name']} with expected default rate "
        f"{lowest_risk_strategy['expected_default_rate']:.2%} and expected loss proxy "
        f"{lowest_risk_strategy['expected_loss_proxy']:.2f}."
    ))
else:
    insights.append(("lowest_risk_strategy", "No valid low-risk strategy insight available."))

if latest_month is not None:
    insights.append((
        "latest_month_summary",
        f"Latest reporting month is {latest_month['reporting_month']}, with "
        f"{latest_month['total_loans']} loans, disbursed amount {latest_month['total_disbursed']:.2f}, "
        f"outstanding balance {latest_month['total_outstanding_balance']:.2f}, and default rate "
        f"{latest_month['default_rate']:.2%}."
    ))
else:
    insights.append(("latest_month_summary", "No latest month summary available."))

if default_rate_change is not None:
    insights.append((
        "portfolio_trend",
        f"Portfolio default trend is {trend_direction}; default rate changed by "
        f"{default_rate_change:.2%} from earliest to latest month."
    ))
else:
    insights.append(("portfolio_trend", "Portfolio trend could not be computed."))


# COMMAND ----------

# Print insights cleanly

print("AI-style narrative insights:")
for category, text in insights:
    print(f"- [{category}] {text}")


# COMMAND ----------

# Save insights table
insights_schema = T.StructType([
    T.StructField("insight_category", T.StringType(), True),
    T.StructField("insight_text", T.StringType(), True)
])

insights_df = spark.createDataFrame(insights, schema=insights_schema)

insights_df.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable("gold_ai_insights")

print("Saved table: gold_ai_insights")
spark.table("gold_ai_insights").show(truncate=False)

# COMMAND ----------

