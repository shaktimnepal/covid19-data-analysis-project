from pyspark.sql import SparkSession


# Initialize Spark Session
spark = SparkSession.builder.appName("AWS Project Covid-19 Analysis").enableHiveSupport().getOrCreate()


# Read json file into DataFrame
df = spark.read.json("file:///home/takeo/data/aws_project/data_files/usa-hospital-beds.geojson")


# Drop duplicates
df = df.dropDuplicates()


# Write DataFrame to Hive as a Parquet table
df.write.format("parquet").mode("overwrite").saveAsTable("aws_project.usa_hospital_beds_hive_table")


# Query the table in Hive and display results
spark.sql("select * from aws_project.usa_hospital_beds_hive_table limit 5").show()
