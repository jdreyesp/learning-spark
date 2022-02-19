package com.jdreyesp.learningspark.chapter5

import com.jdreyesp.learningspark.SparkSessionInitializer

object UDF extends App with SparkSessionInitializer {

  println("Defining and calling UDF from Spark SQL")
  val cubed = (s: Long) => s * s * s

  spark.udf.register("cubed", cubed)
  spark.range(1, 9).createOrReplaceTempView("udf_test")

  spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show(10, false)

  // Important note if defining PySpark UDFs: Pandas UDF (or vectorized UDFs) have been introduced from Spark 2.3
  // so that rows aren't processed one by one (known issue in Spark on slowness on data movement between JVM -> Python)
  // but processed as Pandas Dataframes. This speeds up and distribute data nicely.
  //
  // Just define the udf using `pandas_udf(...)`
  // More info: https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html

  // It does not have to be a primitive data type that can be processed / returned by an UDF. It can also accept / return complex types
  // Let's see this in an example:

  case class PersonalChar(charisma: Double, bravery: Double)
  case class PhysicalChar(strength: Double, dexterity: Double)
  case class Human(personalChar: PersonalChar, physicalChar: PhysicalChar)
  case class Elf(personalChar: PersonalChar, physicalChar: PhysicalChar)

  val humanToElfPersonalChar = (humanPersonalChar: PersonalChar) => PersonalChar(humanPersonalChar.charisma * 2, humanPersonalChar.bravery / 2)
  val humanToElfPhysicalChar = (humanPhysicalChar: PhysicalChar) => PhysicalChar(humanPhysicalChar.strength / 2, humanPhysicalChar.dexterity * 3)

  spark.udf.register("humanToElfPersonalChar", humanToElfPersonalChar)
  spark.udf.register("humanToElfPhysicalChar", humanToElfPhysicalChar)

  import spark.implicits._

  val humanDS = Seq(Human(PersonalChar(12, 15), PhysicalChar(10, 12)))
    .toDF
    .as[Human]

  humanDS.createOrReplaceTempView("fantasyPeople")

  val elfDS = spark.sql(
    """
      SELECT humanToElfPersonalChar(personalChar) AS personalChar,
             humanToElfPhysicalChar(physicalChar) AS physicalChar
      FROM fantasyPeople""")
    .as[Elf]

  elfDS.show(false)

}
