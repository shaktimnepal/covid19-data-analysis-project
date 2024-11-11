from pyspark.sql import SparkSession
from pyspark.sql.functions import col


#Initialize Sparksession with Hive support
spark = SparkSession.builder.appName("DimRegionCreation").enableHiveSupport().getOrCreate()


#Load tables from Hive into DataFrames


country_code_df = spark.table("aws_project.country_code_qs_hive_table")
county_population_df = spark.table("aws_project.county_population_hive_table")
enigma_jhu_df = spark.table("aws_project.enigma_jhu_hive_table")
states_abv_df = spark.table("aws_project.states_abv_hive_table")
states_daily_df = spark.table("aws_project.states_daily_hive_table")
us_county_df = spark.table("aws_project.us_county_hive_table")
us_daily_df = spark.table("aws_project.us_daily_hive_table")
us_df = spark.table("aws_project.us_hive_table")
us_states_df = spark.table("aws_project.us_states_hive_table")
usa_hospital_beds_df = spark.table("aws_project.usa_hospital_beds_hive_table")


# Select only the needed columns from enigma_jhu_df and rename them to match requirement for final table


enigma_jhu_selected_df = enigma_jhu_df.select(
   col("fips"),
   col("country_region").alias("region"),
   col("latitude").alias("lat"),
   col("longitude").alias("lang"),
   col("province_state").alias("state"),
   col("admin2").alias("county")
)


# Select columns from states_abv_df and rename them to match requirement for final table


states_abv_selected_df = states_abv_df.select(
   col("State").alias("state"),
   col("abbreviation").alias("state_abb")
)


#Join DataFrames to create dimRegion table


dimRegion_df = enigma_jhu_selected_df.join(states_abv_selected_df, on = "state", how = "inner")


# Write the new dataFrame as table back to Hive


dimRegion_df.write.mode("overwrite").saveAsTable("aws_project.dim_Region")


# Query the table in Hive and display results
spark.sql("select * from aws_project.dim_Region limit ").show()


#Stop the Spark Session
spark.stop()
