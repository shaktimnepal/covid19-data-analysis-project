from pyspark.sql import SparkSession


# Initialize Spark Session
spark = SparkSession.builder.appName("AWS Project Covid-19 Analysis").enableHiveSupport().getOrCreate()


# Read CSV file into DataFrame
df = spark.read.csv("file:///home/takeo/data/aws_project/data_files/us_county.csv",header=True, inferSchema=True)


# Drop duplicates
df = df.dropDuplicates()


# Handle null values with default replacements
df = df.fillna({
   "date": "unknown",
   "county": "unknown",
   "state": "unknown",
   "fips": 0,
   "cases": 0,
   "deaths": 0
})


# Write DataFrame to Hive as a Parquet table
df.write.format("parquet").mode("overwrite").saveAsTable("aws_project.us_county_hive_table")


# Query the table in Hive and display results
spark.sql("select * from aws_project.us_county_hive_table limit 5").show()