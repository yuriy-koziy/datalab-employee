package org.datalab.employee.features

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{DenseVector => MLDenseVector, Vector => SparkVector}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by Yuriy Koziy on 12/7/17.
  */
object BehavioralClusters {
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

        try {
            val weekOfYearAgg = new WeeklyActivityAggregator

            val work = spark.read.option("header", "true").csv(options.inputPath)
              .filter(isDoubleUDF($"LAT") && isDoubleUDF($"LON"))
                    .withColumn("datetime", timestampUDF($"event_start_date"))
                    .withColumn("weekday", weekdayUDF($"datetime"))
                    .drop("event_start_date")
                    .filter($"weekday" =!= 6 && $"weekday" =!= 7)
                    .withColumn("year_week", yearWeekUDF($"datetime"))
              .groupBy($"hash_number_A", $"year_week")
              .agg(weekOfYearAgg($"weekday", $"network_service_direction").as("week_activity"))
                    .withColumn("1_in", $"week_activity".getItem(0).getItem(1))
                    .withColumn("2_in", $"week_activity".getItem(0).getItem(2))
                    .withColumn("3_in", $"week_activity".getItem(0).getItem(3))
                    .withColumn("4_in", $"week_activity".getItem(0).getItem(4))
                    .withColumn("5_in", $"week_activity".getItem(0).getItem(5))
                    .withColumn("1_out", $"week_activity".getItem(1).getItem(1))
                    .withColumn("2_out", $"week_activity".getItem(1).getItem(2))
                    .withColumn("3_out", $"week_activity".getItem(1).getItem(3))
                    .withColumn("4_out", $"week_activity".getItem(1).getItem(4))
                    .withColumn("5_out", $"week_activity".getItem(1).getItem(5))
                    .withColumn("1_total", $"1_in" + $"1_out")
                    .withColumn("2_total", $"2_in" + $"2_out")
                    .withColumn("3_total", $"3_in" + $"3_out")
                    .withColumn("4_total", $"4_in" + $"4_out")
                    .withColumn("5_total", $"5_in" + $"5_out")
                    .withColumn("in_total", $"1_in" + $"2_in" + $"3_in" + $"4_in" + $"5_in")
                    .withColumn("out_total", $"1_out" + $"2_out" + $"3_out" + $"4_out" + $"5_out")
                    .withColumn("total", $"in_total" + $"out_total")
                    .drop($"week_activity")
                    .cache()

                val numbers = work.select($"hash_number_A").distinct().collect().map(_.getAs[String](0))

                val toDense = udf((vec: SparkVector) => vec.toDense)

                var behaviourClusters: Seq[(String, Int, Double)] = Seq()
                var i = 0
                for (aNumber <- numbers) {
                    i = i + 1
                    println(s"$i. Processing $aNumber")
                    val aNumberCalls = work.filter($"hash_number_A" === aNumber).cache()
                    val featured = new VectorAssembler()
                            .setInputCols(Array(
                                "1_in",
                                "2_in",
                                "3_in",
                                "4_in",
                                "5_in",
                                "1_out",
                                "2_out",
                                "3_out",
                                "4_out",
                                "5_out",
                                "1_total",
                                "2_total",
                                "3_total",
                                "4_total",
                                "5_total",
                                "in_total",
                                "out_total",
                                "total"
                            ))
                            .setOutputCol("features")
                            .transform(aNumberCalls)

                    val nClusters = featureClusters(featured)
                    behaviourClusters :+ (aNumber, nClusters._1, nClusters._2)
                    aNumberCalls.unpersist()
                }

                spark.sparkContext.parallelize(behaviourClusters).toDF("hash_number_A", "number_beh_clusters, beh_cluster_error")
                        .coalesce(1).write.option("header", "true").csv(options.outputPath)
        } finally spark.close()
    }
}
