from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, count, avg

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

#function to find Unique cities with airports, per country
def unique_cities_per_country(df):
    df_result = df.groupBy("Country") \
        .agg(countDistinct("City").alias("unique_city_count")) \
        .orderBy(col("unique_city_count").desc())

    df_result.show(20, truncate=False)

    df_result.write.mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{HDFS_OUTPUT_BASE}/unique_cities_per_country")

    return df_result

#funtion to find average altitude (feet) of airports, per country
def avg_altitude_per_country(df):
    df_result = df.groupBy("Country") \
        .agg(avg("Altitude").alias("avg_altitude_ft")) \
        .orderBy(col("avg_altitude_ft").desc())

    df_result.show(20, truncate=False)

    df_result.write.mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{HDFS_OUTPUT_BASE}/avg_altitude_per_country")

    return df_result

#function to return Number of airports operating per timezone
def airports_per_timezone(df):
    df_result = df.groupBy("Timezone") \
        .agg(count("AirportID").alias("num_airports")) \
        .orderBy(col("Timezone"))

    df_result.show(30, truncate=False)

    df_result.write.mode("overwrite") \
        .format("csv") \
        .option("header", "true") \
        .save(f"{HDFS_OUTPUT_BASE}/airports_per_timezone")

    return df_result


#Driver
if __name__ == "__main__":
    #calling function to find airports in southeast part
    southeast_airports(df_airports)
    unique_cities_per_country(df_airports)
    avg_altitude_per_country(df_airports)
    airports_per_timezone(df_airports)
    spark.stop()
