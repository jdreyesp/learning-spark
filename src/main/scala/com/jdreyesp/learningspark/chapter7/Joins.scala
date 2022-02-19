package com.jdreyesp.learningspark.chapter7

import com.jdreyesp.learningspark.SparkSessionInitializer
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.asc

import java.io.File
import scala.util.Random

object Joins extends App with SparkSessionInitializer {

  import spark.implicits._

  FileUtils.deleteDirectory(new File("spark-warehouse"))

  /** BROADCAST HASH JOIN
   *
   * Creates a hash table based on join_key of smaller relation
   * =>
   * Loop over Large relation and match the hashed join_key with the Hash Table created above
   *
   * */

  // Main benefits:
  // - It saves shuffling costs

  // Good to use when:
  // - Joining a small dataset with a big dataset (default small dataset size = 10MB)
  // - When each key in the small dataset and each in the big one are hashed to the same partition in Spark
  // - When you only want to perform an equi-join, to combine two datasets based on matching unsorted keys
  // - When you are not worried by excessive network bandwith usage or OOM errors, because the smaller data set will be broadcast to all Spark executors

  // Generate some sample data for two data sets
  var states = scala.collection.mutable.Map[Int, String]()
  var items = scala.collection.mutable.Map[Int, String]()
  val rnd = new Random(42)

  // Initialize states and items purchased
  states += (0 -> "AZ", 1 -> "CO", 2 -> "CA", 3 -> "TX", 4 -> "NY", 5 -> "MI")
  items += (0 -> "SKU-0", 1 -> "SKU-1", 2 -> "SKU-2", 3 -> "SKU-3", 4 -> "SKU-4", 5 -> "SKU-5")

  // Create Dataframes
  var usersDF = (0 to 100).map(id => (id, s"user_${id}", s"user_${id}@databricks.com", states(rnd.nextInt(5))))
    .toDF("uid", "login", "email", "user_state")
  var ordersDF = (0 to 1000000).map(r => (r, r, rnd.nextInt(10000), 10 * r * 0.2d, states(rnd.nextInt(5)), items(rnd.nextInt(5))))
    .toDF("transaction_id", "quantity", "users_id", "amount", "state", "items")

  // Do the join
  val usersOrdersBroadcastDF = ordersDF.join(usersDF, $"users_id" === $"uid")

  // Show the joined results
  usersOrdersBroadcastDF.show(false)

  // See "SortMergeJoin" and "hashpartitioning" steps in physical plan. A shuffle sort merge join is happening
  //== Physical Plan ==
  //AdaptiveSparkPlan isFinalPlan=false
  //+- BroadcastHashJoin [users_id#42], [uid#13], Inner, BuildRight, false
  //   :- LocalTableScan [transaction_id#40, quantity#41, users_id#42, amount#43, state#44, items#45]
  //   +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [id=#58]
  //      +- LocalTableScan [uid#13, login#14, email#15, user_state#16]
  usersOrdersBroadcastDF.explain()

  /** SORT MERGE JOIN
   *
   * Shuffles data per join_key to all worker nodes
   * * ((bucketing can be performed here to avoid exchange task in spark physical plan and improve performance))
   * =>
   * A sort merge join operation performed in each worker node
   *
   * */

  // Benefits:
  // - Performs better than Broadcast hashjoin since it's better distributed when both joining datasets are big, and no hash tables are created
  //

  // Drawbacks:
  // - join_keys need to be sortable (obviously)

  // Good join when:
  // - When each key within two large data sets can be sorted and hashed to the same partition by Spark
  // - When you want to perform only equi-joins to combine two data sets based on matching sorted keys
  // - When you want to prevent Exchange and Sort operations to save large shuffles across the network (using bucketing, see below)
  //


  // Show preference over other joins for large data sets
  // Disable broadcast join
  spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

  usersDF = (0 to 100000).map(id => (id, s"user_${id}", s"user_${id}@databricks.com", states(rnd.nextInt(5))))
    .toDF("uid", "login", "email", "user_state")

  val usersOrdersSortMergeDF = ordersDF.join(usersDF, $"users_id" === $"uid")

  // Show the joined results
  usersOrdersSortMergeDF.show(false)

  // See "SortMergeJoin" and "hashpartitioning" steps in physical plan. A shuffle sort merge join is happening
  //== Physical Plan ==
  //AdaptiveSparkPlan isFinalPlan=false
  //+- SortMergeJoin [users_id#42], [uid#126], Inner
  //   :- Sort [users_id#42 ASC NULLS FIRST], false, 0
  //   :  +- Exchange hashpartitioning(users_id#42, 200), ENSURE_REQUIREMENTS, [id=#166]
  //   :     +- LocalTableScan [transaction_id#40, quantity#41, users_id#42, amount#43, state#44, items#45]
  //   +- Sort [uid#126 ASC NULLS FIRST], false, 0
  //      +- Exchange hashpartitioning(uid#126, 200), ENSURE_REQUIREMENTS, [id=#167]
  //         +- LocalTableScan [uid#126, login#127, email#128, user_state#129]
  usersOrdersSortMergeDF.explain()


  // USING BUCKETING
  // You can use bucketing () so that you order the join_keys and save the result accordingly for reusing the shuffles across the network

  usersDF.orderBy(asc("uid"))
    .write.format("parquet")
    .bucketBy(8, "uid")
    .mode(SaveMode.Overwrite)
    .saveAsTable("UsersTbl")

  ordersDF.orderBy(asc("users_id"))
    .write.format("parquet")
    .bucketBy(8, "users_id")
    .mode(SaveMode.Overwrite)
    .saveAsTable("OrdersTbl")

  // Cache the tables
  spark.sql("CACHE TABLE UsersTbl")
  spark.sql("CACHE TABLE OrdersTbl")

  // Read them back in
  val usersBucketDF = spark.table("UsersTbl")
  val ordersBucketDF = spark.table("OrdersTbl")

  // Do the join and show the results
  val joinUsersOrdersBucketDF = ordersBucketDF
    .join(usersBucketDF, $"users_id" === $"uid")

  joinUsersOrdersBucketDF.show(false)

  //NO EXCHANGE TASK WHEN BUCKETING

  //== Physical Plan ==
  //AdaptiveSparkPlan isFinalPlan=false
  //+- SortMergeJoin [users_id#344], [uid#205], Inner
  //   :- Sort [users_id#344 ASC NULLS FIRST], false, 0
  //   :  +- Filter isnotnull(users_id#344)
  //   :     +- Scan In-memory table OrdersTbl [transaction_id#342, quantity#343, users_id#344, amount#345, state#346, items#347], [isnotnull(users_id#344)]
  //   :           +- InMemoryRelation [transaction_id#342, quantity#343, users_id#344, amount#345, state#346, items#347], StorageLevel(disk, memory, deserialized, 1 replicas)
  //   :                 +- *(1) ColumnarToRow
  //   :                    +- FileScan parquet default.orderstbl[transaction_id#342,quantity#343,users_id#344,amount#345,state#346,items#347] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<transaction_id:int,quantity:int,users_id:int,amount:double,state:string,items:string>, SelectedBucketsCount: 8 out of 8
  //   +- Sort [uid#205 ASC NULLS FIRST], false, 0
  //      +- Filter isnotnull(uid#205)
  //         +- Scan In-memory table UsersTbl [uid#205, login#206, email#207, user_state#208], [isnotnull(uid#205)]
  //               +- InMemoryRelation [uid#205, login#206, email#207, user_state#208], StorageLevel(disk, memory, deserialized, 1 replicas)
  //                     +- *(1) ColumnarToRow
  //                        +- FileScan parquet default.userstbl[uid#205,login#206,email#207,user_state#208] Batched: true, DataFilters: [], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<uid:int,login:string,email:string,user_state:string>, SelectedBucketsCount: 8 out of 8
  joinUsersOrdersBucketDF.explain()

  //For checking Spark UI
  while(true){}

  /** How Spark selects join strategy:
   If it is an ‘=’ join (equi-join):
    Look at the join hints, in the following order:
      1. Broadcast Hint: Pick broadcast hash join if the join type is supported.
      2. Sort merge hint: Pick sort-merge join if join keys are sortable.
      3. shuffle hash hint: Pick shuffle hash join if the join type is supported.
      4. shuffle replicate NL hint: pick cartesian product if join type is inner like.
    If there is no hint or the hints are not applicable
      1. Pick broadcast hash join if one side is small enough to broadcast, and the join type is supported.
      2. Pick shuffle hash join if one side is small enough to build the local hash map, and is much smaller than the other side, and spark.sql.join.preferSortMergeJoin is false.
      3. Pick sort-merge join if join keys are sortable.
      4. Pick cartesian product if join type is inner .
      5. Pick broadcast nested loop join as the final solution. It may OOM but there is no other choice.

  If it’s not ‘=’ join:
    Look at the join hints, in the following order:
      1. broadcast hint: pick broadcast nested loop join.
      2. shuffle replicate NL hint: pick cartesian product if join type is inner like.
    If there is no hint or the hints are not applicable
      1. Pick broadcast nested loop join if one side is small enough to broadcast.
      2. Pick cartesian product if join type is inner like.
      3. Pick broadcast nested loop join as the final solution. It may OOM but we don’t have any other choice. */
}
