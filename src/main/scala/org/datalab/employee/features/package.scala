package org.datalab.employee

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.functions.udf
import scopt.OptionParser

/**
  * Created by Yuriy Koziy on 12/7/17.
  */
package object features {

    private[features] final case class Arguments(inputPath: String = "",
                             outputPath: String = "",
                             tempFilesPath: String = "hdfs:///tmp",
                             outlierDetectionMethod: String = "GaussianMultivariate")

    private[features] def parseArguments(args: Array[String]): Arguments = {
        val p: Package = getClass.getPackage
        val appName = p.getImplementationTitle
        val appVersion = p.getImplementationVersion
        val parser = new OptionParser[Arguments](appName) {
            head(appName, appVersion)

            opt[String]('i', "input")
                    .required()
                    .valueName("<path>")
                    .action((x, c) => c.copy(inputPath = x))
                    .text("Path to the input files with device profiles generated by generation module (S3, hdfs or local)")

            opt[String]('o', "output")
                    .required()
                    .valueName("<path>")
                    .action((x, c) => c.copy(outputPath = x))
                    .text("Path to the output directory to save device features (S3, hdfs or local)")

            opt[String]('t', "temp")
                    .optional()
                    .valueName("<path>")
                    .action((x, c) => c.copy(tempFilesPath = x))
                    .text("[optional] Path to a directory where temporary data files will be saved (default 'hdfs:///tmp')")

            opt[String]('m', "method")
                    .optional()
                    .valueName("<str>")
                    .action((x, c) => c.copy(outlierDetectionMethod = x))
                    .text("[optional] Method of outlier detection. One of: GaussianVectorized, GaussianMultivariate. Default: GaussianMultivariate")

            note(
                """
                  |     ---Feature preprocessing module---
                  |""".stripMargin)

            help("help") text "Print this usage text\n"
        }
        parser.parse(args, Arguments()) match {
            case Some(config) => config
            case None =>
                System.exit(1)
                Arguments()
        }
    }

    val timestampUDF = udf((dt: String) => {
        val suffixMonth = dt.substring(3, 5)
        LocalDateTime.parse(dt.replace(suffixMonth, suffixMonth.toLowerCase()), DateTimeFormatter.ofPattern("ddMMMyy:HH:mm:ss"))
          .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    })

    val isDouble = (str: String) => str != null && str.matches("^[\\+\\-]{0,1}[0-9]+[\\.\\,][0-9]+$")
    val weekdayUDF = udf((dt: String) => LocalDateTime.parse(dt, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).getDayOfWeek.getValue)
    val isDoubleUDF = udf((str: String) => isDouble(str))
    val toDoubleUDF = udf((str: String) => str.toDouble)
    val distanceBetweenUDF = udf((lat1: String,lon1: String,lat2: String, lon2: String) => {
        if (isDouble(lat1) && isDouble(lat2) && isDouble(lon1) && isDouble(lon2)) {
            val dx = lat1.toDouble - lat2.toDouble
            val dy = lon1.toDouble - lon2.toDouble
            Math.sqrt(dx * dx + dy * dy)
        } else 0
    })


}
