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
                .setMaster("local[*]")
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
              .withColumn("datetime", timestampUDF($"event_start_date"))
              .withColumn("unix_timestamp", unix_timestamp(col("datetime")))
              .withColumn("weekday", weekdayUDF($"datetime"))
              .drop("event_start_date")
              .withColumn("time_delta", $"unix_timestamp" - when(isnull(previousActionTime), 0).otherwise(previousActionTime))
              .withColumn("lat_previous", previousLat)
              .withColumn("lon_previous", previousLon)
              .withColumn("dist_delta", distanceBetweenUDF($"lat_previous", $"lon_previous", $"LAT", $"LON"))
              .withColumn("is_valid_coord", $"dist_delta" <= $"time_delta" * lit(maxSpeed))

            val toDense = udf((vec: SparkVector) => vec.toDense)

            val aNumber = calls.select("hash_number_A").distinct().takeAsList(1).get(0).getAs[String](0)
            val aNumberCalls = calls.filter($"hash_number_A" === aNumber)
            val featured = new VectorAssembler()
              .setInputCols(Array("lat", "lon"))
              .setOutputCol("features")
              .transform(aNumberCalls)

            val clustersModel = new KMeans()
                    .setK(3)
                    .setFeaturesCol("features")
                    .setMaxIter(100)
                    .fit(featured)

            val WSSSE = clustersModel.computeCost(featured)
            println("Within Set Sum of Squared Errors = " + WSSSE)

        } finally spark.close()
    }
}
