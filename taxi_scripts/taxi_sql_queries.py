#!/usr/bin/env python
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

spark = SparkSession.builder.master("local").appName("taxi-sql")\
    .config("spark.some.config.option", "some-value").getOrCreate()


#Calculate Total Travel Time(In Minutes)
timeFmt = "yyyy-MM-dd' 'HH:mm:ss"
timeDiff = (F.unix_timestamp('dropoff_datetime', format=timeFmt)-F.unix_timestamp('pickup_datetime', format=timeFmt)/60)

dateFMT = 'MM-yyyy'
fiscalYear=F.date_format('pickup_datetime', format=dateFMT)



#Load Data
data = spark.sql("SELECT pickup_datetime, dropoff_datetime, trip_distance,pickup_zone, dropoff_zone \
                 FROM parquet. `nyctaxizones.parquet` ")  #tripsbyzones
#Convet Data to Fiscal Year
data.createOrReplaceTempView("data")
data =spark.sql("SELECT *, CASE WHEN date_format(pickup_datetime,'yyyy-MM') between '2012-07' and '2013-06' Then 'Year0' \
          WHEN date_format(pickup_datetime,'yyyy-MM') between '2013-07' and '2014-06' Then 'Year1' \
          WHEN date_format(pickup_datetime,'yyyy-MM') between '2014-07' and '2015-06' Then 'Year2' \
          WHEN date_format(pickup_datetime,'yyyy-MM') between '2015-07' and '2016-06' Then 'Year3' \
          Else NULL End AS FY FROM data")
data.createOrReplaceTempView("data")


# Add Columns
data= data.withColumn("duration",timeDiff) \
          .withColumn("avg_speed",F.col("trip_distance")/(F.col("duration")))

#Create a Temporary View
data.createOrReplaceTempView("data")

# Order by Dropouts
dropoffs = spark.sql("SELECT dropoff_zone, Count(*) as counts FROM data \
                        GROUP BY dropoff_zone ORDER BY counts DESC")
dropoffs.show()

# Order number of Pickups
pickups = spark.sql("SELECT pickup_zone, Count(*) as counts FROM data \
                        GROUP BY pickup_zone ORDER BY counts DESC")
pickups.show()

# Popular Trip Strings
trip_string = spark.sql("SELECT CONCAT(pickup_zone, '-',dropoff_zone ) as trip_string, avg(trip_distance) as avg \
                        Count(*) as counts FROM data \
                        GROUP BY trip_string ORDER BY counts DESC")
trip_string.show()

# Find Top Pickups by Year
pickups_year = spark.sql("SELECT FY, pickup_zone, count(pickup_zone) as total_trips \
                         FROM data GROUP BY FY, pickup_zone").show()
pickups_year.createOrReplaceTempView("pickups_year")

#Total Number of Riders by Hour
rides_by_year = spark.sql("SELECT FY,date_format(pickup_datetime,'h-a') as hour, count(*) as total_trips \
                         FROM data GROUP BY FY,hour Order by FY, hour ASC")

#Total Number of Trips by Month
rides_by_month = spark.sql("SELECT FY,date_format(pickup_datetime,'MMM') as Month, count(*) as total_trips \
                          FROM data GROUP BY FY,month")


# Popular Trip Strings
trip_string = spark.sql("SELECT FY, CONCAT(pickup_zone, '-',dropoff_zone ) as trip_string,Count(*) as total_trips \
                        FROM data GROUP BY FY, trip_string")

trip_string.show()

#Total Counts by Fiscal Year
countsbyyear = spark.sql("SELECT FY, Count(*) as counts FROM trips ORDER BY counts DESC")




