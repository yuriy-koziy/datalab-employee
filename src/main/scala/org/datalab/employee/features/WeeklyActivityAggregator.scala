package org.datalab.employee.features

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * Created by Yuriy Koziy on 12/9/17.
  */
class WeeklyActivityAggregator extends UserDefinedAggregateFunction{
    /** UDAF input schema */
    def inputSchema: StructType =
        new StructType().add("weekday", StringType).add("network_service_direction", StringType)

    /** UDAF buffer schema */
    def bufferSchema: StructType =
        new StructType().add("in", MapType(StringType, IntegerType)).add("out", MapType(StringType, IntegerType))

    /** UDAF output data type */
    def dataType: DataType = ArrayType(MapType(StringType, IntegerType))

    /** UDAF flag to produce deterministic output */
    def deterministic: Boolean = true

    /** Initialize aggregation buffer */
    def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer.update(0, Map[String, Int]())
        buffer.update(1, Map[String, Int]())
    }

    /**
      * Updates aggregation buffer with input Rows
      *
      * Sequential execution on a single worker level to accumulate local features
      *
      * @param buffer aggregation buffer that corresponds to bufferSchema schema
      * @param input data Row that contains inputSchema columns
      */
    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        val weekDayOpt = Option(input.getAs[String](0))
        val directionOpt = Option(input.getAs[String](1))

        var inboundAccumulatorMap = buffer.getAs[Map[String, Int]](0)
        var outboundAccumulatorMap = buffer.getAs[Map[String, Int]](1)
        (weekDayOpt, directionOpt) match {
            case (Some(weekDay), Some(direction)) =>
                if ("Incoming".equalsIgnoreCase(direction)) {
                    val value = inboundAccumulatorMap.getOrElse(weekDay, 0) + 1
                    inboundAccumulatorMap = inboundAccumulatorMap.updated(weekDay, value)
                } else if ("Outgoing".equalsIgnoreCase(direction)) {
                    val value = outboundAccumulatorMap.getOrElse(weekDay, 0) + 1
                    outboundAccumulatorMap = outboundAccumulatorMap.updated(weekDay, value)
                }
                buffer.update(0, inboundAccumulatorMap)
                buffer.update(1, outboundAccumulatorMap)
            case _ =>
        }
    }

    /**
      * Merges two aggregator buffers into one
      *
      * Non-local, merges aggregator buffers from two workers
      *
      * @param buffer1 first mutable buffer (after method execution contains merged results)
      * @param buffer2 second buffer
      */
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        val in1 = buffer1.getAs[Map[String, Int]](0)
        val in2 = buffer2.getAs[Map[String, Int]](0)
        val mergedIn = in1 ++ in2.map{ case (k, v) => k -> (v + in1.getOrElse(k, 0)) }
        buffer1.update(0, mergedIn)

        val out1 = buffer1.getAs[Map[String, Int]](1)
        val out2 = buffer2.getAs[Map[String, Int]](1)
        val mergedOut = out1 ++ out2.map{ case (k, v) => k -> (v + out1.getOrElse(k, 0)) }
        buffer1.update(1, mergedOut)
    }

    /** Generates output */
    def evaluate(buffer: Row): Any = {
        Array(buffer.getAs[Map[String, Int]](0), buffer.getAs[Map[String, Int]](1))
    }
}
