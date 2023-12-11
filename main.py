from pyspark.sql import SparkSession
from pyspark.sql.functions import count

# Initialize Spark Session
spark = SparkSession.builder.appName("BirdDataProcessing").getOrCreate()

file_path = "dbfs:/FileStore/tables/your-output-directory/part-00000-tid-6687512951801856408-77285f10-1c38-4edc-92e1-f3f07f756634-9-1-c000.snappy.parquet"

# Read data from the Parquet file into a DataFrame
birds_df = spark.read.parquet(file_path)

# Group by 'scientific name' and count the occurrences
transformed_df = birds_df.groupBy("scientific_name").agg(count("class_id").alias("image_count"))

# Write the transformed data to the sink
output_path = "dbfs:/FileStore/tables/your-output-directory/transformed/"
transformed_df.write.mode("overwrite").parquet(output_path)
