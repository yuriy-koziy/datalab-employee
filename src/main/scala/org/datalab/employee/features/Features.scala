package org.datalab.employee.features

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.{DenseVector => MLDenseVector, Vector => SparkVector}
import org.apache.spark.ml.clustering.{GaussianMixture, KMeans}

/**
  * Created by Yuriy Koziy on 12/7/17.
  */
object Features {
    def main(args: Array[String]): Unit = {
        val appConf = ConfigFactory.load()
        val sparkConf = new SparkConf()
                .setAppName("datalabEmployeeFeatures")
//                .setMaster("local[*]")
                .set("spark.executor.extraJavaOptions", "-Duser.timezone=" + appConf.getString("app.sparkTimezone"))
                .set("spark.debug.maxToStringFields", "10000")

        val options = parseArguments(args)

        Logger.getLogger("org.apache.spark")
                .setLevel(Level.toLevel(appConf.getString("app.features.sparkDebugLevel")))
        val spark = SparkSession.builder.config(sparkConf).getOrCreate()
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._

        import java.time.format.DateTimeFormatter

        try {
            val maxSpeed = appConf.getDouble("app.features.maxSpeed")

            val abonentWindow = Window.partitionBy($"hash_number_A").orderBy($"datetime")
            val previousActionTime = lag($"unix_timestamp", 1).over(abonentWindow)
            val previousLat = lag($"LAT", 1).over(abonentWindow)
            val previousLon = lag($"LON", 1).over(abonentWindow)

            val calls = spark.read.option("header", "true").csv(options.inputPath)
              .filter(isDoubleUDF($"LAT") && isDoubleUDF($"LON"))
              .withColumn("lat", when(isDoubleUDF($"LAT"), toDoubleUDF($"LAT")).otherwise(0.0d))
              .withColumn("lon", when(isDoubleUDF($"LON"), toDoubleUDF($"LON")).otherwise(0.0d))
//              .withColumn("datetime", timestampUDF($"event_start_date"))
              .withColumn("unix_timestamp", unix_timestamp(col("datetime")))
//              .withColumn("weekday", weekdayUDF($"datetime"))
//              .drop("event_start_date")
//                    .filter($"weekday" =!= 6 && $"weekday" =!= 7)
              .withColumn("time_delta", $"unix_timestamp" - when(isnull(previousActionTime), 0).otherwise(previousActionTime))
              .withColumn("lat_previous", previousLat)
              .withColumn("lon_previous", previousLon)
              .withColumn("dist_delta", distanceBetweenUDF($"lat_previous", $"lon_previous", $"LAT", $"LON"))
              .withColumn("is_valid_coord", $"dist_delta" <= $"time_delta" * lit(maxSpeed))
                    .drop($"unix_timestamp")

            //calls.coalesce(1).write.option("header", "true").csv(options.inputPath + "-workdays")

            val work = spark.read.option("header", "true").csv(options.inputPath)
                    .withColumn("lat", when(isDoubleUDF($"LAT"), toDoubleUDF($"LAT")).otherwise(0.0d))
                    .withColumn("lon", when(isDoubleUDF($"LON"), toDoubleUDF($"LON")).otherwise(0.0d))
                    .filter($"phone_price_category" =!= "4")
                    .select($"hash_number_A", $"lat", $"lon")
                    .repartition($"hash_number_A").cache()

            val numbers = work.select($"hash_number_A").distinct().collect().map(_.getAs[String](0))

            val toDense = udf((vec: SparkVector) => vec.toDense)

            var geoClusters: Seq[(String, Int, Double)] = Seq()
            var i = 0
            for(aNumber <- numbers) {
                i = i + 1
                println(s"$i. Processing $aNumber")
                val aNumberCalls = work.filter($"hash_number_A" === aNumber)
                val featured = new VectorAssembler()
                        .setInputCols(Array("lat", "lon"))
                        .setOutputCol("features")
                        .transform(aNumberCalls).cache()

                val nClusters = featureClusters(featured)
                geoClusters = geoClusters :+ (aNumber, nClusters._1, nClusters._2 )
                println(geoClusters.size + s". Number: $aNumber, clusters: " + nClusters._1 + ", error: " + nClusters._2)
                featured.unpersist()
            }

            spark.sparkContext.parallelize(geoClusters).toDF("hash_number_A", "number_geo_clusters, geo_cluster_error")
                    .coalesce(1).write.option("header", "true").csv(options.outputPath)
        } finally spark.close()
    }

    def numbers(spark: SparkSession, options: Arguments) = {
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        spark.read.option("header", "true").csv(options.inputPath)
    }
}
