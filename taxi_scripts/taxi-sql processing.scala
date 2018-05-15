import org.apache.spark.sql.SparkSession
import spark.implicits._
import magellan.{Point, Polygon, PolyLine}
import magellan.coord.NAD83
import org.apache.spark.sql.magellan.MagellanContext
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._



//Load Spark Session
val spark = SparkSession.builder().
  appName("Spark SQL basic example").
  config("spark.some.config.option", "some-value").
  getOrCreate()


// Load Data Into A DataFrame
print("Loading Data")
val data = spark.read.
  format("csv").
  option("header", true).
  option("inferSchema", true).
  load(("/user/tp823/data/2013/yellow_tripdata_2013-12.csv")).
  withColumn("pickup_point", point($"pickup_longitude",$"pickup_latitude")).
  withColumn("dropoff_point", point($"dropoff_longitude",$"dropoff_latitude")).cache()


var df = data
for(col <- data.columns){
  df = df.withColumnRenamed(col,col.replaceAll("\\s", ""))
}


// Load NYC Mapping Data
val neighborhoods = spark.read.
  format("magellan").option("type", "geojson").
  load("/user/tp823/data/neighborhoods.geojson").
  select($"polygon",$"metadata"("neighborhood").as("neighborhood")).cache()


// Map GPS Coordinates to NYC Zone
print("Map GPS Coordinates to NYC Zone ")
val joined_df = df.join(neighborhoods).
  where($"pickup_point" within $"polygon")

joined_df.createOrReplaceTempView("joined_df")
val trip_data = spark.sql("SELECT pickup_datetime,dropoff_datetime,trip_distance,neighborhood,dropoff_point" +
  " from joined_df").withColumnRenamed("neighborhood", "pickup_zone")


val new_df = trip_data.join(neighborhoods).
  where($"dropoff_point" within $"polygon").
  withColumnRenamed("neighborhood", "dropoff_zone").
  drop("polygon","dropoff_point")


val trips_2016=spark.sql("SELECT pickup_datetime, dropoff_datetime, trip_distance, pickup_zone, dropoff_zone FROM parquet. `2016trips.parquet`")
//val trips=spark.sql("SELECT pickup_datetime, dropoff_datetime, trip_distance, pickup_zone, dropoff_zone FROM parquet. `trips_zones.parquet`")
