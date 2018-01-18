package com.antoniocachuan.geospark

import org.apache.spark.sql.SparkSession
import magellan._
import org.apache.spark.sql.magellan.dsl.expressions._

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object magellan {
  
  def main(args: Array[String]) {
   
    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
   
    val spark = SparkSession
                   .builder()
                   .master("local")
                   .appName("SparkDemo")
                   .getOrCreate()
    
    val schema = StructType(Array(
    StructField("vendorId", StringType, false),
    StructField("pickup_datetime", StringType, false),
    StructField("dropoff_datetime", StringType, false),
    StructField("passenger_count", IntegerType, false),
    StructField("trip_distance", DoubleType, false),
    StructField("pickup_longitude", DoubleType, false),
    StructField("pickup_latitude", DoubleType, false),
    StructField("rateCodeId", StringType, false),
    StructField("store_fwd", StringType, false),
    StructField("dropoff_longitude", DoubleType, false),
    StructField("dropoff_latitude", DoubleType, false),
    StructField("payment_type", StringType, false),
    StructField("fare_amount", StringType, false),
    StructField("extra", StringType, false),
    StructField("mta_tax", StringType, false),
    StructField("tip_amount", StringType, false),
    StructField("tolls_amount", StringType, false),
    StructField("improvement_surcharge", StringType, false),
    StructField("total_amount", DoubleType, false)))
    
 
     import spark.implicits._
       
       val trips = spark.sqlContext.read
                   .format("csv")
                   .option("mode", "DROPMALFORMED")
                   .option("comment", "V")
                   .schema(schema)
                   .load("../TheDefinitiveGuide/geo/yellow_tripdata_2015-02.csv")
                   .withColumn("point", point($"pickup_longitude",$"pickup_latitude"))
                   .cache()
                  
       
       trips.show(10)
       
      
      val neighborhoods = spark.sqlContext.read
            .format("magellan")
            .option("type", "geojson")
            .load("../TheDefinitiveGuide/geo/geojson/")
            .select($"polygon", 
             $"metadata"("neighborhood").as("neighborhood"))
            .cache()
     
      print("final :" + neighborhoods.count())    
      
      neighborhoods.show(10)
      
       val joined = trips.join(neighborhoods)
        .where($"point" within $"polygon")
        
        
      joined.show(200)
      
      print("Total de punto: " + trips.count()) 
  }
}