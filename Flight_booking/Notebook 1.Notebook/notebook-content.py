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

# Welcome to your new notebook
# Type here in the cell editor to add code!
df = spark.read.option("multiLine", True) \
    .json("Files/flight_booking.json")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert the DepartTimestamp string into proper timestamp
from pyspark.sql.functions import to_timestamp

df = df.withColumn(
    "DepartTimestamp",
    to_timestamp("DepartTimestamp")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Extract year and month
from pyspark.sql.functions import year, month

df_yrnmonth = df \
    .withColumn("DepartYear", year("DepartTimestamp")) \
    .withColumn("DepartMonth", month("DepartTimestamp"))

display(df_yrnmonth)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# To extract the quarter (Q1–Q4)
from pyspark.sql.functions import quarter

df_yrnmonth = df_yrnmonth.withColumn(
    "DepartQuarter",
    quarter("DepartTimestamp")
)

display(df_yrnmonth)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_yrnmonth.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import concat_ws, col

df_yrnmonth = df_yrnmonth.withColumn(
    "DepartYearQuarter",
    concat_ws("_Q", col("DepartYear"), col("DepartQuarter"))
)

display(df_yrnmonth)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_yrnmonth.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_yrnmonth.write.format("delta") \
  .mode("overwrite") \
  .partitionBy("DepartYear", "DepartMonth") \
  .save("Tables/flight_booking_month")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Find max date of the dataset

from pyspark.sql.functions import max as spark_max

max_date_df = df_yrnmonth.select(
    spark_max("DepartTimestamp").alias("max_date")
)

max_date_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filter the data by flight status as flown, passenger status flown,
# transaction type credit and the departure date is between max date and last 3 months
from pyspark.sql.functions import col, date_sub

df_filtered = df_yrnmonth.crossJoin(max_date_df).filter(
    (col("FlightStatus") == "Flown") &
    (col("PassengerFlown") == "Yes") &
    (col("TransactionType") == "Credit") &
    (col("DepartTimestamp") >= date_sub(col("max_date"), 90)) &
    (col("DepartTimestamp") <= col("max_date"))
).drop("max_date")

display(df_filtered)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filter the data with the transaction more than Rs.100000
from pyspark.sql.functions import col

df_max_trans = df_filtered.withColumn(
    "Amount",
    col("NumberOfSeats") * col("Price")
)

df_max_trans = df_max_trans.filter(
    col("Amount") > 100000
)

display(df_max_trans)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_creditdetails = spark.read.option("multiLine", True) \
    .json("Files/creditdetails.json")
    
display(df_creditdetails)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_creditdetails.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Extract the customers who used the credit cards for the flight booking

df_final = df_max_trans.join(
    df_creditdetails,
    on="TransactionID",
    how="inner"
)

display(df_final)


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

from pyspark.sql.functions import col

df1 = df_final.select(
    col("PassengerName"),
    col("bankname"),
    col("merchant"),
    col("partner")
)

display(df1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create new table of prime customers
df1.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Prime_customers")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
