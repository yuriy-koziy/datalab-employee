package org.datalab.employee.features

import java.time.LocalDateTime

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

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
        val dateFormaterFrom = DateTimeFormatter.ofPattern("ddMMMyy:HH:mm:ss")
        val dateFormaterTo = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

        try {
            val timestampUDF = udf((dt: String) => {
                val suffixMonth = dt.substring(3, 5)
                LocalDateTime.parse(dt.replace(suffixMonth, suffixMonth.toLowerCase()), DateTimeFormatter.ofPattern("ddMMMyy:HH:mm:ss"))
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
            })

            val weekdayUDF = udf((dt: String) => LocalDateTime.parse(dt, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).getDayOfWeek)

            val calls = spark.read.option("header", "true").csv(options.inputPath)
                    .withColumn("datetime", timestampUDF($"event_start_date"))
                    .withColumn("weekday", weekdayUDF($"datetime"))


            calls.take(2).map(println)
        } finally spark.close()
    }
}
