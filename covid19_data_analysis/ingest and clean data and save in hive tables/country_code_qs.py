from pyspark.sql import SparkSession
#from pyspark.sql.functions import col, lit, when


# Initialize Spark Session
spark = SparkSession.builder.appName("AWS Project Covid-19 Analysis").enableHiveSupport().getOrCreate()


# Read CountryCodeQS Data from local
df = spark.read.csv("file:///home/takeo/data/aws_project/data_files/CountryCodeQS.csv", header=True, inferSchema=True)




# Change column names from Alpha-2 code and Alpha-3 code to Alpha_2_code and Alpha_3_code
# in order to follow rules for hive column names


df = (df.withColumnRenamed("Alpha-2 code", "Alpha_2_code") \
     .withColumnRenamed("Alpha-3 code", "Alpha_3_code") \
     .withColumnRenamed("Numeric code","Numeric_code"))


# drop duplicates
df = df.dropDuplicates()




# Handle Null Values: Replace NULLs with Default Values
df = df.fillna({
   "Country": "Unknown",
   "Alpha_2_code": "Unknown",
   "Alpha_3_code": "Unknown",
   "Numeric_code": 0.0,
   "Latitude": 0.0,
   "Longitude": 0.0
})




# Write Cleaned Data to Hive Tables
df.write.format("parquet").mode("overwrite").saveAsTable("aws_project.country_code_qs_hive_table")


# Query the table in Hive and display results
spark.sql ("select * from aws_project.country_code_qs_hive_table limit 5").show()
