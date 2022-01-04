package com.jdreyesp.sparkexamples

import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class Chapter2 extends AnyFlatSpec with SparkProvider {

  val readme_source = getClass.getClassLoader.getResource("README.md").getPath

  it should "count lines from a file" in {

    //Given
    val df: DataFrame = spark.read.text(readme_source)

    //When
    df.show(10, false)
    val result: Int = df.count().toInt

    //Then
    result shouldBe 109
  }

  it should "filter and count lines that contains 'Spark'" in {
    import org.apache.spark.sql.functions._

    //Given
    val strings = spark.read.text(readme_source)

    //When
    val filtered = strings.filter(col("value").contains("Spark"))
    filtered.show(10, false)
    val result: Int = filtered.count().toInt

    //Then
    result shouldBe 20
  }

//  it should "count M&Ms for the cookie monster" {
//
//  }
}
