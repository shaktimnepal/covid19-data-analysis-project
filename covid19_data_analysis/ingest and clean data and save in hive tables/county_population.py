from pyspark.sql import SparkSession


# Initialize Spark Session
spark = SparkSession.builder.appName("AWS Project Covid-19 Analysis").enableHiveSupport().getOrCreate()


# Read CSV file into DataFrame
df = spark.read.csv("file:///home/takeo/data/aws_project/data_files/County_Population.csv",header=True, inferSchema=True)


# Rename columns to Hive-compatible names
df = df.withColumnRenamed("Population Estimate 2018", "population_estimate_2018")


# Drop duplicates
df = df.dropDuplicates()


# Handle null values with default replacements
df = df.fillna({
   "Id": 0.0,
   "Id2": 0.0,
   "County": "Unknown",
   "State": "Unknown",
   "population_estimate_2018": 0.0
})


# Write DataFrame to Hive as a Parquet table
df.write.format("parquet").mode("overwrite").saveAsTable("aws_project.county_population_hive_table")


# Query the table in Hive and display results
spark.sql("select * from aws_project.county_population_hive_table limit 5").show()
