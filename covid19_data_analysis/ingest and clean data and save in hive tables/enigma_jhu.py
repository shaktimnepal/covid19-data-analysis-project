from pyspark.sql import SparkSession


# Initialize Spark Session
spark = SparkSession.builder.appName("AWS Project Covid-19 Analysis").enableHiveSupport().getOrCreate()


# Read CSV file into DataFrame
df = spark.read.csv("file:///home/takeo/data/aws_project/data_files/Enigma-JHU.csv",header=True, inferSchema=True)


# Drop duplicates
df = df.dropDuplicates()


# Handle null values with default replacements
df = df.fillna({
   "fips": 0,
   "admin2": "Unknown",
   "province_state": "Unknown",
   "country_region": "Unknown",
   "last_update": "Unknown",
   "latitude": 0.0,
   "longitude": 0.0,
   "confirmed": 0,
   "deaths": 0,
   "recovered": 0,
   "active": 0,
   "combined_key": "Unknown"
})


# Write DataFrame to Hive as a Parquet table
df.write.format("parquet").mode("overwrite").saveAsTable("aws_project.enigma_jhu_hive_table")


# Query the table in Hive and display results
spark.sql("select * from aws_project.enigma_jhu_hive_table limit 5").show()
