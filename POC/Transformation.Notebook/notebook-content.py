# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8f16e6b1-a4de-4d41-b9f1-7f7eecf06d90",
# META       "default_lakehouse_name": "pharma_bronze",
# META       "default_lakehouse_workspace_id": "f96006f6-40b3-4b24-a888-736a091473dd",
# META       "known_lakehouses": [
# META         {
# META           "id": "8f16e6b1-a4de-4d41-b9f1-7f7eecf06d90"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# Read ingested tables as:
# 
# df_pat: Patient table
# 
# df_enc: encounter table
# 
# df_proc: procedure table
# 
# df_med: medication table

# CELL ********************

df_pat = spark.read.format("csv")\
        .option("header", "true")\
        .option("mode", "permissive")\
        .load("Files/usedfiles/patients.csv")
        
display(df_pat)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_pat.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_med = spark.read.format("csv")\
        .option("header", "true")\
        .option("mode", "permissive")\
        .load("Files/usedfiles/medications.csv")
        
display(df_med)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_med.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_enc = spark.read.format("csv")\
        .option("header", "true")\
        .option("mode", "permissive")\
        .load("Files/usedfiles/encounters.csv")
        
display(df_enc)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_enc.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_proc = spark.read.format("csv")\
        .option("header", "true")\
        .option("mode", "permissive")\
        .load("Files/usedfiles/procedures.csv")
        
display(df_proc)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_proc.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Import libraries

# CELL ********************

from pyspark.sql.types import *
from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Data cleansing 

# CELL ********************

df_pat1 = df_pat.select(
                        col("patient"),
                        col("birthdate"),
                        col("deathdate"),
                        concat(col("first"), lit(" "), col("last")).alias("name"),
                        col("gender")
                    ).filter(
                        col("patient").isNotNull()
                    ).withColumn(
                        "is_alive", when(col("deathdate").isNull(), 1).otherwise(0)
                    ).fillna({
                        "name": "Unknown Name",
                        "gender": "Unknown"
                    })
   

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, when, lit

df_enc1 = df_enc.select(
                        col("PATIENT").alias("enc_patient"),
                        col("DESCRIPTION").alias("enc_description"),
                        col("REASONCODE").alias("enc_reasoncode"),
                        col("REASONDESCRIPTION").alias("enc_reasondescription")
                    ).filter(
                        col("enc_patient").isNotNull()   
                    ).fillna({
                        "enc_description": "Unknown Diagnosis",
                        "enc_reasondescription": "Unknown Reason"
                    })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_med1 = df_med.select(
                        col("START").alias("med_startdate"),
                        col("STOP").alias("med_stopdate"),
                        col("PATIENT").alias("med_patient"),
                        col("DESCRIPTION").alias("med_description"),
                        col("REASONCODE").alias("med_reasoncode"),
                        col("REASONDESCRIPTION").alias("med_reasondescription")
                    ).filter(
                        col("med_patient").isNotNull()
                    ).withColumn(
                        "is_active", when(col("med_stopdate").isNull(), 1).otherwise(0)
                    ).fillna({
                        "med_description": "Unknown Drug",
                        "med_reasondescription": "Unknown Reason"
                    })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_proc1 = df_proc.select(
                            col("PATIENT").alias("proc_patient"),
                            col("DESCRIPTION").alias("proc_description"),
                            col("REASONCODE").alias("proc_reasoncode"),
                            col("REASONDESCRIPTION").alias("proc_reasondescription")
                        ).filter(
                            col("proc_patient").isNotNull()
                        ).fillna({
                            "proc_description": "Unknown Procedure",
                            "proc_reasondescription": "Unknown Reason"
                        })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Aggregation before join in order to avoid data explosion

# CELL ********************

from pyspark.sql.functions import count, countDistinct

df_enc_agg = df_enc1.groupBy("enc_patient").agg(
    count("*").alias("total_encounters"),
    countDistinct("enc_reasoncode").alias("unique_enc_reasons")
)

df_med_agg = df_med1.groupBy("med_patient").agg(
    count("*").alias("total_medications"),
    countDistinct("med_description").alias("unique_medications"),
    count(when(col("is_active") == 1, True)).alias("active_medications")
)

df_proc_agg = df_proc1.groupBy("proc_patient").agg(
    count("*").alias("total_procedures"),
    countDistinct("proc_description").alias("unique_procedures")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final = df_pat1 \
    .join(df_enc_agg, df_pat1["patient"] == df_enc_agg["enc_patient"], "left") \
    .join(df_med_agg, df_pat1["patient"] == df_med_agg["med_patient"], "left") \
    .join(df_proc_agg, df_pat1["patient"] == df_proc_agg["proc_patient"], "left")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final = df_final.drop(
    "enc_patient",
    "med_patient",
    "proc_patient"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final.write \
    .format("delta") \
    .mode("overwrite") \
    .save("abfss://Pharma_teamA@onelake.dfs.fabric.microsoft.com/pharma_silver.Lakehouse/Files/silver_pharma")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
