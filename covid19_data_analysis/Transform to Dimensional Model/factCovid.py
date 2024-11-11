from pyspark.sql import SparkSession
from pyspark.sql.functions import col


#Initialize Sparksession with Hive support
spark = SparkSession.builder.appName("FactCovidCreation").enableHiveSupport().getOrCreate()


#Load tables from Hive into DataFrames


enigma_jhu_df = spark.table("aws_project.enigma_jhu_hive_table")


states_daily_df = spark.table("aws_project.states_daily_hive_table")




# Select only the needed columns from enigma_jhu_df and rename them to match requirement for final table


enigma_jhu_selected_df = enigma_jhu_df.select(
   col("last_update").alias("date"),
   col("fips").alias("FIPS"),
   col("province_state").alias("states"),
   col("country_region").alias("region"),
   col("confirmed"),
   col("deaths").alias("death"),
   col("recovered"),
   col("active")
)


# Select columns from states_daily_df and rename them to match requirement for final table


states_daily_selected_df = states_daily_df.select(
   col("date"),
   col("positive"),
   col("negative"),
   col("hospitalizedCurrently"),
   col("hospitalized"),
   col("hospitalizedDischarged").alias("hospitalizeddischarged")
)


#Join DataFrames to create factCovid dataframe


factCovid_df = enigma_jhu_selected_df.join(states_daily_selected_df, on = "date", how = "inner")


# Create final dataframe without date column to match the requirement schema


factCovid_final_df = factCovid_df.select(
   col("FIPS"),
   col("states"),
   col("region"),
   col("confirmed"),
   col("death"),
   col("recovered"),
   col("active"),
   col("positive"),
   col("negative"),
   col("hospitalizedCurrently"),
   col("hospitalized"),
   col("hospitalizeddischarged")
)


# Write the final dataFrame as table back to Hive


factCovid_final_df.write.mode("overwrite").saveAsTable("aws_project.fact_Covid")


# Query the table in Hive and display results
spark.sql("select * from aws_project.fact_Covid limit 5").show()


#Stop the Spark Session
spark.stop()
