package com.jdreyesp.learningspark.chapter3

import com.jdreyesp.learningspark.SparkSessionInitializer

case class DeviceIoTData(battery_level: Long, c02_level: Long, cca2: String, cca3: String,
                         cn: String, device_id: Long, device_name: String, humidity: Long,
                         ip: String, latitude: Double, lcd: String, longitude: Double,
                         scale: String, temp: Long, timestamp: Long)

case class DeviceTempByCountry(temp: Long, device_name: String, device_id: Long, cca3: String)

object DeviceIoTData extends App with SparkSessionInitializer {

  import spark.implicits._

  val deviceIoTDS = spark.read
    .option("multiLine", true)
    .json(getClass.getClassLoader.getResource("chapter3/device-iot-data.json").getPath)
    .as[DeviceIoTData]

  println("Filtering by using Scala function")
  val filterTempDS = deviceIoTDS.filter({ d => {d.temp > 30 && d.humidity > 70}})
  filterTempDS.show(5, false)

  println("Using DeviceTempByCountry case class")
  val dsTemp = deviceIoTDS
    .filter(d => d.temp > 25)
    .map(d => (d.temp, d.device_name, d.device_id, d.cca3))
    .toDF("temp", "device_name", "device_id", "cca3") // If we don't do this step, column names would be _1, _2, _3, _4 and .as[DeviceTempByCountry] won't succeed
    .as[DeviceTempByCountry]

  dsTemp.show(5, false)

  println("Alternative solution")
  val dsTemp2 = deviceIoTDS
    .select($"temp", $"device_name", $"device_id", $"cca3")
    .where("temp > 25")
    .as[DeviceTempByCountry]

  dsTemp2.show(5, false)
}
