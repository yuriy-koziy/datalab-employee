package org.datalab.employee.features

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}

/**
  * "hash_number_A","hash_tariff","event","event_sub","network_service_direction","event_start_date",
  * "LAT","LON","cost","hash_number_B","number_B_category","call_duration_minutes",
  * "data_volume_mb","hash_accum_code","device_type","phone_price_category",
  * "interest_1","interest_2","interest_3","interest_4","interest_5"
  * Created by Yuriy Koziy on 12/7/17.
  */
object Schema {
    val schema: StructType = new StructType()
            .add("numberA", StringType)
            .add("tariff", StringType)
            .add("event", StringType)
            .add("subEvent", StringType)
            .add("direction", StringType)
            .add("eventTimestamp", StringType)
            .add("lat", DoubleType)
            .add("lon", DoubleType)
            .add("cost", DoubleType)
            .add("numberB", StringType)
            .add("numberBCategory", StringType)
            .add("duration", DoubleType)
            .add("dataVolume", DoubleType)
            .add("accumCode", StringType)
            .add("deviceType", StringType)
            .add("devicePriceCategory", IntegerType)
            .add("interest1", StringType)
            .add("interest2", StringType)
            .add("interest3", StringType)
            .add("interest4", StringType)
            .add("interest5", StringType)
}
