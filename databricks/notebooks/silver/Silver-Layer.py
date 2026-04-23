# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 1. Read Data from adls bronze

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

chat_df = spark.read.format("parquet").load("abfss://bronze@chatbotadls.dfs.core.windows.net/chat_history/")
booking_df = spark.read.format("parquet").load("abfss://bronze@chatbotadls.dfs.core.windows.net/bookings/")

# COMMAND ----------

chat_df.display()

# COMMAND ----------

booking_df.display()

# COMMAND ----------

chat_df.printSchema()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2. Basic Cleaning for 'created'
# MAGIC - dropping columns with 'created' as null
# MAGIC -  type tasting 'created' to timestamp

# COMMAND ----------

from pyspark.sql.functions import col

# remove null timestamps
chat_df = chat_df.filter(col("created").isNotNull())
booking_df = booking_df.filter(col("created").isNotNull())

# ensure correct datatype
chat_df = chat_df.withColumn("created", col("created").cast("timestamp"))
booking_df = booking_df.withColumn("created", col("created").cast("timestamp"))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3. Sessionization (CORE LOGIC)
# MAGIC - If gap > 30 minutes → new session

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3.1 — Define Window based on user_id (i.e. create groups by user_id) and sort by created

# COMMAND ----------

window_spec = Window.partitionBy("user_id").orderBy("created")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3.2 — Previous timestamp

# COMMAND ----------

chat_df = chat_df.withColumn(
    "prev_time",
    lag("created").over(window_spec)
)

# COMMAND ----------

chat_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3.3 — Time difference between previous message and current message

# COMMAND ----------

chat_df = chat_df.withColumn(
    "time_diff",
    unix_timestamp("created") - unix_timestamp("prev_time")
)

# COMMAND ----------

chat_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3.4 — New session flag (If gap > 30 mins → new session)
# MAGIC - if true add value as 1 else 0
# MAGIC - where 1 means new session, and 0 means currently runnung session

# COMMAND ----------

chat_df = chat_df.withColumn(
    "new_session_flag",
    when((col("time_diff") > 1800) | col("time_diff").isNull(), 1).otherwise(0)
)

# COMMAND ----------

chat_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3.5 — create Session ID based on sum of new_session_flag values

# COMMAND ----------

from pyspark.sql.functions import sum as spark_sum

chat_df = chat_df.withColumn(
    "session_id",
    sum("new_session_flag").over(window_spec)
)

# COMMAND ----------

chat_df.display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 4 — Add Step Order (Funnel LOGIC)
# MAGIC - To track:
# MAGIC - - user journey → step1 → step2 → step3
# MAGIC - - Step should be inside session, not across all data

# COMMAND ----------

step_window = Window.partitionBy("user_id", "session_id").orderBy("created")

chat_df = chat_df.withColumn(
    "step_number",
    row_number().over(step_window)
)

# COMMAND ----------

chat_df.display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 5 — Create Session-Level Table
# MAGIC - We don’t analyze event-level always → we need session summary.
# MAGIC - grouping based on user_id and session_id
# MAGIC - - then finding:
# MAGIC - - - max(created), min(created), total(sessions) for each group

# COMMAND ----------

session_df = chat_df.groupBy("user_id", "session_id").agg(
    min("created").alias("session_start"),
    max("created").alias("session_end"),
    count("*").alias("total_events")
)

# COMMAND ----------

session_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 - Add Next session start for each session

# COMMAND ----------


window_spec = Window.partitionBy("user_id").orderBy("session_start")

session_df = session_df.withColumn(
    "next_session_start",
    lead("session_start").over(window_spec)
)

# COMMAND ----------

session_df.display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 6 — Add Surrogate Key because
# MAGIC - Natural keys unreliable
# MAGIC - Needed for joins

# COMMAND ----------

session_df = session_df.withColumn(
    "session_key",
    monotonically_increasing_id()
)

# COMMAND ----------

session_df.display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 7 — Join session with Bookings
# MAGIC - To know: Did user convert?

# COMMAND ----------


joined_df = session_df.join(
    booking_df,
    (session_df.user_id == booking_df.user_id) &
    (booking_df.created >= session_df.session_start) &
    (
        (booking_df.created < session_df.next_session_start) |
        (session_df.next_session_start.isNull())
    ),
    "left"
).drop(booking_df.user_id)

# COMMAND ----------

joined_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8 Since each session can have multiple bookings 
# MAGIC - we'll Rank bookings per session
# MAGIC - we'll Pick ONLY ONE booking per session

# COMMAND ----------

window_spec = Window.partitionBy("user_id", "session_id") \
                    .orderBy(col("created").desc())

final_df = joined_df.withColumn(
    "rank",
    row_number().over(window_spec)
).filter("rank = 1")

# COMMAND ----------

display(final_df)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 9 — Create Conversion Flag
# MAGIC - Needed for:
# MAGIC - - conversion rate
# MAGIC - - funnel

# COMMAND ----------

final_df = final_df.withColumn(
    "converted",
    when(col("booking_id").isNotNull(), 1).otherwise(0)
)

# COMMAND ----------

final_df.display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 10. Now clean the data

# COMMAND ----------

# MAGIC %md
# MAGIC - 10a Drop unecessry columns

# COMMAND ----------

final_df = final_df.drop("rank")

# COMMAND ----------

# MAGIC %md
# MAGIC - 10b Handle null booking columns

# COMMAND ----------

final_df = final_df.withColumn(
    "created",
    when(col("booking_id").isNull(), None).otherwise(col("created"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC -10c Rename columns (VERY IMPORTANT)

# COMMAND ----------

final_df = final_df.withColumnRenamed("created", "booking_time")

# COMMAND ----------

# MAGIC %md
# MAGIC - 10d Keep only useful columns

# COMMAND ----------

final_df = final_df.select(
    "user_id",
    "session_id",
    "session_start",
    "session_end",
    "total_events",
    "session_key",
    "booking_id",
    "booking_time",
    "dealer_id",
    "dealer_city",
    "product_id",
    "variant_name",
    "enquiry_type",
    "converted"
)

# COMMAND ----------

final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC - 10e Add session duration (GOOD FOR ANALYSIS)

# COMMAND ----------

final_df = final_df.withColumn(
    "session_duration_minutes",
    round((unix_timestamp("session_end") - unix_timestamp("session_start"))/60,2)
)

# COMMAND ----------

# MAGIC %md
# MAGIC - 10f Add session date (for reporting)

# COMMAND ----------

from pyspark.sql.functions import to_date

final_df = final_df.withColumn(
    "session_date",
    to_date("session_start")
)

# COMMAND ----------

# MAGIC %md
# MAGIC - 10g Add session type

# COMMAND ----------

final_df = final_df.withColumn(
    "session_type",
    when(col("converted") == 1, "converted").otherwise("non_converted")
)

# COMMAND ----------

final_df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 11. Save data as Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists silver;
# MAGIC
# MAGIC create schema if not exists gold;

# COMMAND ----------

final_df.write.format("delta") \
    .mode("overwrite") \
    .option("path","abfss://silver@chatbotadls.dfs.core.windows.net/fact_tables/") \
    .saveAsTable("silver.fact_sessions")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.fact_sessions

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 12. CReate dimension tables, IMPLEMENT SCD TYPE 2 (STEP BY STEP)
# MAGIC ### STEP 1 —Create Dealer Dimension

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.dim_dealer (
# MAGIC     dealer_key BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     dealer_id STRING,
# MAGIC     dealer_city STRING,
# MAGIC     start_date DATE,
# MAGIC     end_date DATE,
# MAGIC     is_active INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@chatbotadls.dfs.core.windows.net/dim_tables/dim_dealer';

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 3 PREPARE SOURCE DATA

# COMMAND ----------

dealer_src_df = spark.table("silver.fact_sessions") \
    .select("dealer_id", "dealer_city") \
    .where("dealer_id IS NOT NULL") \
    .dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 4 — Add SCD columns

# COMMAND ----------

dealer_src_df = dealer_src_df.withColumn("start_date", current_date()) \
                             .withColumn("end_date", lit(None).cast("date")) \
                             .withColumn("is_active", lit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 5 — APPLY SCD TYPE 2 (MERGE)

# COMMAND ----------

from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, "silver.dim_dealer")

delta_table.alias("target").merge(
    dealer_src_df.alias("source"),
    "target.dealer_id = source.dealer_id AND target.is_active = 1"
) \
    .whenMatchedUpdate(
        condition="target.dealer_city <> source.dealer_city",
        set={
            "end_date": "current_date()",
            "is_active": "0"
        }
    ) \
    .whenNotMatchedInsert(
        values={
            "dealer_id": "source.dealer_id",
            "dealer_city": "source.dealer_city",
            "start_date": "source.start_date",
            "end_date": "null",
            "is_active": "1"
        }
    ) \
    .execute()

# COMMAND ----------

