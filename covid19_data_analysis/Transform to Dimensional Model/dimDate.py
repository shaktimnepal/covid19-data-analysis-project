from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, month, year, dayofweek


# Initialize Spark session
spark = SparkSession.builder.appName("DimDateCreation").enableHiveSupport().getOrCreate()


#Load table from Hive into DataFrame


us_states_df = spark.table("aws_project.us_states_hive_table")


# Add month and year columns
dimDate_df = us_states_df.withColumn("month", month(col("date"))).withColumn("year", year(col("date")))


# Add is_weekend column
# Using dayofweek (1 = Sunday, 7 = Saturday) to mark weekends
dimDate_df = dimDate_df.withColumn(
   "is_weekend",
   (dayofweek(col("date")).isin([1, 7])).cast("boolean")
)


# Create final dataFrame with only the required columns to match dimDate table schema
dimDate_df = dimDate_df.select(
   col("fips"),
   col("date"),
   col("month"),
   col("year"),
   col("is_weekend")
)


# Write the new dataFrame as table back to Hive


dimDate_df.write.mode("overwrite").saveAsTable("aws_project.dim_Date")


# Query the table in Hive and display results
spark.sql("select * from aws_project.dim_Date limit 5").show()


#Stop the Spark Session
spark.stop()
