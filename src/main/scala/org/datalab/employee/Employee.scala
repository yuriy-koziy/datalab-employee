package org.datalab.employee

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf

/**
  * Created by Yuriy Koziy on 12/7/17.
  */
object Employee {
    def main(args: Array[String]): Unit = {
        val appConf = ConfigFactory.load()
        val sparkConf = new SparkConf()
                .setAppName("datalabEmployee")
                .set("spark.executor.extraJavaOptions", "-Duser.timezone=" + appConf.getString("app.sparkTimezone"))
                .set("spark.debug.maxToStringFields", "10000")
    }
}
