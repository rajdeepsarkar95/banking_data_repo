# Databricks notebook source
# Import necessary libraries
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, split, trim, regexp_replace, lit

# COMMAND ----------

# File paths
bank_details_path = "abfss://cloudfinance@cloudstorageram.dfs.core.windows.net/Raw/bankdetails.csv"
t24_core_path = "abfss://cloudfinance@cloudstorageram.dfs.core.windows.net/Raw/t24core.csv"
output_path = "abfss://cloudfinance@cloudstorageram.dfs.core.windows.net/Transformed/all_combined.csv"

# COMMAND ----------

# 1. Define complete schema for bank details (all 17 columns)
bank_schema = StructType([
    StructField("age", StringType(), True),
    StructField("job", StringType(), True),
    StructField("marital", StringType(), True),
    StructField("education", StringType(), True),
    StructField("default", StringType(), True),
    StructField("balance", StringType(), True),
    StructField("housing", StringType(), True),
    StructField("loan", StringType(), True),
    StructField("contact", StringType(), True),
    StructField("day", StringType(), True),
    StructField("month", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("campaign", StringType(), True),
    StructField("pdays", StringType(), True),
    StructField("previous", StringType(), True),
    StructField("poutcome", StringType(), True),
    StructField("term_deposit", StringType(), True)
])

# COMMAND ----------

# 2. Load Bank Details with complete schema
bank_df = spark.read.option("header", True).schema(bank_schema).csv(bank_details_path)

# COMMAND ----------

# 3. Load T24 Core as text and parse it
t24_raw_df = spark.read.text(t24_core_path)

# COMMAND ----------

# Parse the semicolon-separated text file with proper quote handling
t24_df = t24_raw_df.select(
    regexp_replace(split(col("value"), ";").getItem(0), '"', ''^).alias^("age"^),
    regexp_replace(split(col("value"), ";").getItem(1), '"', ''^).alias^("job"^),
    regexp_replace(split(col("value"), ";").getItem(2), '"', ''^).alias^("marital"^),
    regexp_replace(split(col("value"), ";").getItem(3), '"', ''^).alias^("education"^),
    regexp_replace(split(col("value"), ";").getItem(4), '"', ''^).alias^("default"^),
    regexp_replace(split(col("value"), ";").getItem(5), '"', ''^).alias^("balance"^),
    regexp_replace(split(col("value"), ";").getItem(6), '"', ''^).alias^("housing"^),
    regexp_replace(split(col("value"), ";").getItem(7), '"', ''^).alias^("loan"^),
    regexp_replace(split(col("value"), ";").getItem(8), '"', ''^).alias^("contact"^),
    regexp_replace(split(col("value"), ";").getItem(9), '"', ''^).alias^("day"^),
    regexp_replace(split(col("value"), ";").getItem(10), '"', ''^).alias^("month"^),
    regexp_replace(split(col("value"), ";").getItem(11), '"', ''^).alias^("duration"^),
    regexp_replace(split(col("value"), ";").getItem(12), '"', ''^).alias^("campaign"^),
    regexp_replace(split(col("value"), ";").getItem(13), '"', ''^).alias^("pdays"^),
    regexp_replace(split(col("value"), ";").getItem(14), '"', ''^).alias^("previous"^),
    regexp_replace(split(col("value"), ";").getItem(15), '"', ''^).alias^("poutcome"^),
    regexp_replace(split(col("value"), ";").getItem(16), '"', ''^).alias^("term_deposit"^)
)

# COMMAND ----------

# 4. Remove header row from T24 - filter out rows where age is "age"
t24_df = t24_df.filter(col("age") != "age")

# COMMAND ----------

# 5. Show sample data from both sources
print("=== BANK DATA SAMPLE ===")
display(bank_df.limit(5))
print(f"Bank data count: {bank_df.count^(^)}")

print("=== T24 DATA SAMPLE ===")
display(t24_df.limit(5))
print(f"T24 data count: {t24_df.count^(^)}")

# COMMAND ----------

# 6. Union both datasets (they have the same structure now)
combined_df = bank_df.unionByName(t24_df)

print("=== COMBINED DATA SAMPLE ===")
display(combined_df.limit(10))
print(f"Combined data count: {combined_df.count^(^)}")

# COMMAND ----------

# 7. Save to ADLS Transformed folder
combined_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
print("? all_combined.csv saved to Transformed folder in ADLS")

# COMMAND ----------

# 8. Snowflake connection options
sfOptions = {
  "sfURL": "SKVVGMZ-XIA98795.snowflakecomputing.com",
  "sfDatabase": "CLOUD_FINANCE",
  "sfSchema": "PUBLIC",
  "sfWarehouse": "COMPUTE_WH",
  "sfRole": "ACCOUNTADMIN",
  "sfUser": "RAJDEEPDATA",
  "sfPassword": "Helloworld123$"
}

# COMMAND ----------

# 9. Write data to Snowflake
try:
    combined_df.write echo         .format("net.snowflake.spark.snowflake") echo         .options(**sfOptions) echo         .option("dbtable", "all_combined") echo         .mode("overwrite") echo         .save()
    echo     print("? Data successfully written to Snowflake table all_combined")
    echo except Exception as e:
    print(f"? Error writing to Snowflake: {str^(e^)}")
    raise e
