from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import to_date
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType


# Initialize Spark Session
spark = SparkSession.builder.appName("AWS Project Covid-19 Analysis").enableHiveSupport().getOrCreate()


# Read CSV file into DataFrame
df = spark.read.csv("file:///home/takeo/data/aws_project/data_files/states_daily.csv",header=True, inferSchema=True)


#change dataType of date column from integer to timestamp to make it consistent
df = df.withColumn("date", F.to_timestamp(F.col("date").cast("string"), "yyyyMMdd"))


# Drop duplicates
df = df.dropDuplicates()


# Handle null values by filling with default values for specific columns
df = df.fillna({
   "date": "unknown",
   "state": "unknown",
   "positive": 0,
   "probableCases": 0,
   "negative": 0,
   "pending": 0,
   "totalTestResultsSource": "unknown",
   "totalTestResults": 0,
   "hospitalizedCurrently": 0,
   "hospitalizedCumulative": 0,
   "inIcuCurrently": 0,
   "inIcuCumulative": 0,
   "onVentilatorCurrently": 0,
   "onVentilatorCumulative": 0,
   "recovered": 0,
   "lastUpdateEt": "unknown",
   "dateModified": "unknown",
   "checkTimeEt": "unknown",
   "death": 0,
   "hospitalized": 0,
   "hospitalizedDischarged": 0,
   "dateChecked": "unknown",
   "totalTestsViral": 0,
   "positiveTestsViral": 0,
   "negativeTestsViral": 0,
   "positiveCasesViral": 0,
   "deathConfirmed": 0,
   "deathProbable": 0,
   "totalTestEncountersViral": 0,
   "totalTestsPeopleViral": 0,
   "totalTestsAntibody": 0,
   "positiveTestsAntibody": 0,
   "negativeTestsAntibody": 0,
   "totalTestsPeopleAntibody": 0,
   "positiveTestsPeopleAntibody": 0,
   "negativeTestsPeopleAntibody": 0,
   "totalTestsPeopleAntigen": 0,
   "positiveTestsPeopleAntigen": 0,
   "totalTestsAntigen": 0,
   "positiveTestsAntigen": 0,
   "fips": 0,
   "positiveIncrease": 0,
   "negativeIncrease": 0,
   "total": 0,
   "totalTestResultsIncrease": 0,
   "posNeg": 0,
   "dataQualityGrade": "unknown",
   "deathIncrease": 0,
   "hospitalizedIncrease": 0,
   "hash": "unknown",
   "commercialScore": 0,
   "negativeRegularScore": 0,
   "negativeScore": 0,
   "positiveScore": 0,
   "score": 0,
   "grade": "unknown"
})


# Write DataFrame to Hive as a Parquet table
df.write.format("parquet").mode("overwrite").saveAsTable("aws_project.states_daily_hive_table")


# Query the table in Hive and display results
spark.sql("select * from aws_project.states_daily_hive_table limit 1").show()
