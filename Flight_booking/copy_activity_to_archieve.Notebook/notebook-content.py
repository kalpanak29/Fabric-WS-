# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ece9d16b-cc54-4dd2-b6e8-8514fde143c7",
# META       "default_lakehouse_name": "Flight_lakehouse",
# META       "default_lakehouse_workspace_id": "36759dff-37d4-44a0-965a-866308fe065f",
# META       "known_lakehouses": [
# META         {
# META           "id": "ece9d16b-cc54-4dd2-b6e8-8514fde143c7"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col, current_timestamp

# Define paths
source_path = "Files/flight_booking.json"
staging_table_name = "staging_flights"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.option("multiline", "true").json(source_path)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add timestamp when data is ingested

df_staging = df.withColumn("ingestion_time", current_timestamp())

# Write to Staging Table
df_staging.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(staging_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_staging)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

archive_table_name = "archive_flights"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read from Staging
df_stage_read = spark.read.table(staging_table_name)

# Convert string date (DeptTimestamp) to actual DateType and extract Year/Month
from pyspark.sql.functions import year, month, to_date
df_archive = df_stage_read.withColumn("DepartTimestamp", to_date(col("DepartTimestamp"))) \
                          .withColumn("year", year(col("DepartTimestamp"))) \
                          .withColumn("month", month(col("DepartTimestamp")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# create partition into  archieve
df_archive.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("year", "month") \
    .saveAsTable(archive_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_archive)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check the specific details of the Delta table
spark.sql("DESCRIBE DETAIL archive_flights").select("partitionColumns").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Use Spark SQL to truncate the table
spark.sql(f"TRUNCATE TABLE {staging_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(staging_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
