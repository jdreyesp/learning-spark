Learning spark exercises
========================

Exercises based on the book [Learning Spark 2nd edition](https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/).

> Official Learning Spark Github repo with solved exercises can be found [here](https://github.com/databricks/LearningSparkV2)

There are two types of exercises in this project:

- Under src/main/scala => Spark submit exercises (most suitable for exercises that will be exported to an "uber" jar)
- Under src/test/scala => Scalatest runs for spark exercises

### Running examples with spark-submit

Prerequisites:

- Have JDK 8+ installed in your machine
- Have SBT installed in your machine
- Have JAVA_HOME and SPARK_HOME correctly configured in PATH

In order to package the "uber" jar, run:

`sbt clean package`

Then run:

```bash
spark-submit --class com.jdreyesp.learningspark.chapter<X>.<APP_NAME> target/scala-2.12/learning-spark_2.12-0.1.jar <PARAMS>
```

### Running all tests

Some exercises are in form of tests, and they can all executed as follows:

`sbt test`



