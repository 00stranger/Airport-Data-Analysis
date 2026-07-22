from pyspark.sql import SparkSession
from pyspark.sql.functions import col

#creating spark session
spark = SparkSession.builder \
    .appName("Airport_Data_Analysis") \
    .enableHiveSupport() \
    .getOrCreate()

#Loading source data
df_airports = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("hdfs:///home/takeon/warehouse/raw/airports/airports.csv")

df_airports.cache()

HDFS_OUTPUT_BASE = "hdfs:///home/takeon/warehouse/output/airports"

#function to find southeast part
#Airports in the South East part of the world
#Assumption: South = negative Latitude, East = positive Longitude
#Change the filter below if "South East" refers to a specific
# region like Southeast Asia instead of a hemisphere quadrant.
def southeast_airports(df):
    df_se = df.filter((col("Latitude") < 0) & (col("Longitude") > 0))

    print("Number of airports in the South East:", df_se.count())

    df_se.write.mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{HDFS_OUTPUT_BASE}/southeast_airports")
    return df_se

#Driver
if __name__ == "__main__":
    #calling function to find airports in southeast part
    southeast_airports(df_airports)

    spark.stop()
