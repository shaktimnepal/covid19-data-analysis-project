from pyspark.sql import SparkSession
from pyspark.sql.functions import col




#Initialize Sparksession with Hive support
spark = SparkSession.builder.appName("DimHospitalCreation").enableHiveSupport().getOrCreate()


#Load table from Hive into DataFrame


usa_hospital_beds_df = spark.table("aws_project.usa_hospital_beds_hive_table")


# Select only the needed columns from usa_hospital_beds_df and rename them to match requirement for final table


dimHospital_df = usa_hospital_beds_df.select(
   col("FIPS").alias("fips"),
   col("STATE_NAME").alias("state"),
   col("latitude").alias("hos_lat"),
   col("longtitude").alias("hos_lang"),
   col("HQ_ADDRESS").alias("hq_address"),
   col("HOSPITAL_TYPE").alias("hospital_type"),
   col("HOSPITAL_NAME").alias("hospital_name"),
   col("HQ_CITY").alias("hq_city"),
   col("HQ_STATE").alias("hq_state")
)


# Write the new dataFrame as table back to Hive


dimHospital_df.write.mode("overwrite").saveAsTable("aws_project.dim_Hospital")


# Query the table in Hive and display results
spark.sql("select * from aws_project.dim_Hospital limit 5").show()


#Stop the Spark Session
spark.stop()
