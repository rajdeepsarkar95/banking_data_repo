# Databricks notebook source 
# Import necessary libraries 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType 
from pyspark.sql.functions import col, split, trim, regexp_replace, lit 
 
# COMMAND ---------- 
 
# File paths 
bank_details_path = "abfss://cloudfinance@cloudstorageram.dfs.core.windows.net/Raw/bankdetails.csv" 
t24_core_path = "abfss://cloudfinance@cloudstorageram.dfs.core.windows.net/Raw/t24core.csv" 
output_path = "abfss://cloudfinance@cloudstorageram.dfs.core.windows.net/Transformed/all_combined.csv" 
