from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, current_timestamp

# Authentication
# This file must exist and contain valid service account credentials.
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key.json"

#Create Spark Session
spark = SparkSession.builder.appName('EmployeeETL').getOrCreate()

#Input or Output paths
input_path = "gs://raw-data-bucket01/employee_data.csv"
output_path = "gs://processed-data-bucket01"

#Read csv
employee_df = spark.read.option("header", True) \
                    .option("inferSchema", True) \
                    .csv(input_path)

print("===========RAW DATA=================")
employee_df.show()

#Transformations
transformed_df = employee_df \ 
                .withColumn("department", upper(col("department"))) \
                .withColumn("city", upper(col("city"))) \
                .withColumn("bonus", col("salary") * 0.10) \
                .withColumn("processing_timestamp", current_timestamp())

print("===========TRANSFORMED DATA=================")    
transformed_df.show()

#write output
transformed_df.write \ 
            .mode("overwrite") \
            .option("header", True) \
            .parquet(output_path)           

print("===========DATA WRITTEN SUCCESSFULLY =================")    
spark.stop()