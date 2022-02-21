package com.jdreyesp.learningspark.examples.cogrouping

import com.jdreyesp.learningspark.SparkSessionInitializer
import org.apache.spark.sql.KeyValueGroupedDataset

/**
 * This app applies a custom function when deciding how to deduplicate rows from two different grouped Datasets.
 *
 * The 'custom function' duplicated item will appear in the final Dataset in order of appearance in the combined groups list. That is,
 * if value "30" comes first, and value "29" (the next one) is considered a duplicate, then "30" will be included in the final Dataset,
 * and "29" will be discarded.
 */
object CustomDeduplicateGroupedDatasets extends App with SparkSessionInitializer {

  import spark.implicits._

  case class Example(name: String, note: Int, age: Int) {
    override def equals(obj: Any): Boolean = Math.abs(this.age - obj.asInstanceOf[Example].age) < 5
  }

  val df1 = Seq(
    ("bob", 13, 30),
    ("bob", 13, 29),
    ("bob", 13, 25),
    ("bob", 13, 5),
    ("alice", 13, 25)).toDF("name", "note", "age")

  val df2 = Seq(
    ("bob", 13, 20),
    ("bob", 15, 21),
    ("alice", 13, 25)).toDF("name", "note", "age")

  val groupingByKeyFunction = (example: Example) => example.name

  val deduplicateInGroupFunction: (String, Iterator[Example], Iterator[Example]) => TraversableOnce[Example]
  = (_: String, df1Examples: Iterator[Example], df2Examples: Iterator[Example]) => {
    (df1Examples ++ df2Examples).toSet
  }

  val df2GroupedDataset: KeyValueGroupedDataset[String, Example] = df2.as[Example].groupByKey[String](groupingByKeyFunction)

  df1.as[Example]
    .groupByKey(groupingByKeyFunction)
    .cogroup(df2GroupedDataset)(deduplicateInGroupFunction)
    .show(false)

}
