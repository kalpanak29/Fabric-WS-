# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e16896e4-94f3-4bf8-889e-977cbfab1017",
# META       "default_lakehouse_name": "pharma_silver",
# META       "default_lakehouse_workspace_id": "f96006f6-40b3-4b24-a888-736a091473dd",
# META       "known_lakehouses": [
# META         {
# META           "id": "e16896e4-94f3-4bf8-889e-977cbfab1017"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df_silver = spark.read.format("parquet")\
            .option("header", "true")\
            .load("Files/silver_pharma/silver_pharama.parquet")

display(df_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Use this data to gold layer

# CELL ********************

df_gold = df_silver.select("patient", "name", "gender", "is_alive",
                            "total_encounters", "unique_enc_reasons", "total_medications",
                            "unique_medications", "active_medications", 
                            "total_procedures", "unique_procedures")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_enc = df_gold.select("patient", "name", "gender", "is_alive",
                            "total_encounters", "unique_enc_reasons")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_enc.write \
    .format("delta") \
    .mode("overwrite") \
    .save("abfss://Pharma_teamA@onelake.dfs.fabric.microsoft.com/pharma_gold.Lakehouse/Files/gold_pharma")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_med = df_silver.select("patient", "name", "gender", "is_alive",
                            "total_medications",
                            "unique_medications", "active_medications")                             

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_med.write \
    .format("delta") \
    .mode("append") \
    .save("abfss://Pharma_teamA@onelake.dfs.fabric.microsoft.com/pharma_gold.Lakehouse/Files/gold_pharma/gold_med")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_proc = df_silver.select("patient", "name", "gender", "is_alive",
                            "total_procedures", "unique_procedures")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_proc.write \
    .format("delta") \
    .mode("append") \
    .save("abfss://Pharma_teamA@onelake.dfs.fabric.microsoft.com/pharma_gold.Lakehouse/Files/gold_pharma/gold_proc")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
