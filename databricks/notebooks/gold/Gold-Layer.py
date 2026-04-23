# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### this is the most important part of our project (Gold Layer).
# MAGIC ### Now we convert our clean Silver data into business insights (funnel + KPIs)
# MAGIC Goal
# MAGIC - How many sessions started?
# MAGIC - How many reached intent?
# MAGIC - How many converted?
# MAGIC - Where do users drop?
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 1 — READ SILVER DATA

# COMMAND ----------

df = spark.table("silver.fact_sessions")

# COMMAND ----------

df.display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 2 — DEFINE FUNNEL STAGES
# MAGIC - Stage 1 All sessions
# MAGIC - Stage 2 User actually interacted
# MAGIC - - total_events > 3
# MAGIC - Stage 3 Strong interaction
# MAGIC - - total_events > 10
# MAGIC - Stage 4 Converted
# MAGIC - - converted = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 3 — CALCULATE FUNNEL COUNTS

# COMMAND ----------

funnel_df = df.agg(
    count("*").alias("total_sessions"),
    count(when(df.total_events > 3, True)).alias("engaged_sessions"),
    count(when(df.total_events > 10, True)).alias("high_intent_sessions"),
    count(when(df.converted == 1, True)).alias("converted_sessions")
)

# COMMAND ----------

funnel_df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC ### STEP 4 — CALCULATE CONVERSION %

# COMMAND ----------

funnel_df = funnel_df.withColumn(
    "engagement_rate",
    col("engaged_sessions") / col("total_sessions")) \
    .withColumn(
    "intent_rate",
    col("high_intent_sessions") / col("engaged_sessions")) \
    .withColumn(
    "conversion_rate",
    col("converted_sessions") / col("high_intent_sessions")
)

# COMMAND ----------

funnel_df.display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 5 — CREATE STEP-WISE FUNNEL TABLE

# COMMAND ----------

funnel_table = spark.createDataFrame([
    Row(stage="Session Started", count=df.count()),
    Row(stage="Engaged", count=df.filter("total_events > 3").count()),
    Row(stage="High Intent", count=df.filter("total_events > 10").count()),
    Row(stage="Converted", count=df.filter("converted = 1").count())
])

# COMMAND ----------

funnel_table.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 6 — SAVE GOLD TABLE

# COMMAND ----------

funnel_table.write.format("delta") \
    .mode("overwrite") \
    .option("path","abfss://gold@chatbotadls.dfs.core.windows.net/funnel/") \
    .saveAsTable("gold.funnel_metrics")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from gold.funnel_metrics

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 7 — ADVANCED (VERY IMPORTANT)
# MAGIC - Funnel by dimension (powerful)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### By city

# COMMAND ----------

city_Df = df.groupBy("dealer_city").agg(
    count("*").alias("sessions"),
    count(when(df.converted == 1, True)).alias("conversions")
    ) \
    .sort(col("conversions").desc())

# COMMAND ----------

city_Df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### By product

# COMMAND ----------

product_Df = df.groupBy("product_id").agg(
    count("*").alias("sessions"),
    count(when(df.converted == 1, True)).alias("conversions")
    ) \
    .sort(col("conversions").desc())

# COMMAND ----------

product_Df.display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 8 — DROP-OFF ANALYSIS

# COMMAND ----------

funnel_df = funnel_df \
    .withColumn("drop_stage1_to_2", col("total_sessions") - col("engaged_sessions")) \
    .withColumn("drop_stage2_to_3", col("engaged_sessions") - col("high_intent_sessions")) \
    .withColumn("drop_stage3_to_4", col("high_intent_sessions") - col("converted_sessions")
)

# COMMAND ----------

funnel_df.display()

# COMMAND ----------

