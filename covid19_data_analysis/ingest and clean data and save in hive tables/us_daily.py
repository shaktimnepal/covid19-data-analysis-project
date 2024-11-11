from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# Initialize Spark Session
spark = SparkSession.builder.appName("AWS Project Covid-19 Analysis").enableHiveSupport().getOrCreate()


# Read CSV file into DataFrame
df = spark.read.csv("file:///home/takeo/data/aws_project/data_files/us_daily.csv",header=True, inferSchema=True)




# Drop duplicates
df = df.dropDuplicates()




# Assuming 'column_name' is the column I want to check if it has no records at all
# Checking for the column "recovered"


column_name = 'recovered'


# Check if the column has all nulls, and if so, drop it
if df.filter(F.col(column_name).isNotNull()).count() == 0:
   df = df.drop(column_name)


# Handle null values by removing all rows with at least one null value
df = df.dropna()




# Write DataFrame to Hive as a Parquet table
df.write.format("parquet").mode("overwrite").saveAsTable("aws_project.us_daily_hive_table")


# Query the table in Hive and display results
spark.sql("select * from aws_project.us_daily_hive_table limit 5").show()
