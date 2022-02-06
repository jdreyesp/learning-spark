package com.jdreyesp.learningspark.chapter6

import com.jdreyesp.learningspark.SparkSessionInitializer
import org.apache.spark.sql.functions.desc

import scala.util.Random

case class Usage(uid: Int, uname: String, usage: Int)

object Usage extends App with SparkSessionInitializer {

  import spark.implicits._

  val r = new Random(42)

  // Create 1000 instances of scala Usage class
  // This generates data on the fly
  val data = for (i <- 0 to 1000)
    yield Usage(i, "user-" + r.alphanumeric.take(5).mkString(""), r.nextInt(1000))

  // Create a Dataset of Usage typed data (encoding/decoding is implicit in Scala)
  val dsUsage = spark.createDataset(data) // We could have used spark.read.format("...").load().as[Usage] from external file data
  dsUsage.show(10)

  // Filtering
  dsUsage.filter(d => d.usage > 900)
    .orderBy(desc("usage"))
    .show(5, false)

  // Map
  dsUsage.map(u => {if (u.usage > 750) u.usage * .15 else u.usage * .50})
    .show(5, false)

  // Mapping to a different case class (encoding/decoding is implicit in Scala)
  case class UsageCost(uid: Int, uname: String, usage: Int, cost: Double)

  def computeUserCostUsage(u: Usage): UsageCost = {
    val v = if (u.usage > 750) u.usage * .15 else u.usage * .50
    UsageCost(u.uid, u.uname, u.usage, v)
  }

  dsUsage.map(computeUserCostUsage(_)).show(5)


  // Project Tungsten serialization / deserialization performance comparison

  // Bad performance (due that Spark Catalyst does not know how to handle lambda functions)
  dsUsage
    .filter(u => u.usage > 750) // Deserialize / Serialize from / to Tungsten
    .filter($"uid" < 500)
    .filter(u => u.uname.contains("C")) // Deserialize / Serialize from / to Tungsten
    .show(10, false)

  // Good performance (avoiding lambda functions)
  dsUsage
    .filter($"usage" > 750) // Deserialize / Serialize from / to Tungsten
    .filter($"uid" < 500)
    .filter($"uname".contains("C")) // Deserialize / Serialize from / to Tungsten
    .show(10, false)
}
