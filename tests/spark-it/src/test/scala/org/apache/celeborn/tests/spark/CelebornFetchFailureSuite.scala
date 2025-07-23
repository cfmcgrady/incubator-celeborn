/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.tests.spark

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.{SparkConf, SparkContextHelper, TaskContext}
import org.apache.spark.shuffle.ShuffleHandle
import org.apache.spark.shuffle.celeborn.{CelebornShuffleHandle, ShuffleManagerHook, SparkShuffleManager, SparkUtils, TestCelebornShuffleManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.ShuffleMode
import org.apache.celeborn.service.deploy.worker.Worker

class CelebornFetchFailureSuite extends AnyFunSuite
  with SparkTestBase
  with BeforeAndAfterEach {

  override def beforeEach(): Unit = {
    ShuffleClient.reset()
  }

  override def afterEach(): Unit = {
    System.gc()
  }

  var workerDirs: Seq[String] = Seq.empty

  override def createWorker(map: Map[String, String]): Worker = {
    val storageDir = createTmpDir()
    workerDirs = workerDirs :+ storageDir
    super.createWorker(map, storageDir)
  }

  class ShuffleReaderGetHook(conf: CelebornConf) extends ShuffleManagerHook {
    var executed: AtomicBoolean = new AtomicBoolean(false)
    val lock = new Object

    override def exec(
        handle: ShuffleHandle,
        startPartition: Int,
        endPartition: Int,
        context: TaskContext): Unit = {
      if (executed.get() == true) return

      lock.synchronized {
        handle match {
          case h: CelebornShuffleHandle[_, _, _] => {
            val appUniqueId = h.appUniqueId
            val shuffleClient = ShuffleClient.get(
              h.appUniqueId,
              h.lifecycleManagerHost,
              h.lifecycleManagerPort,
              conf,
              h.userIdentifier,
              h.extension)
            val celebornShuffleId = SparkUtils.celebornShuffleId(shuffleClient, h, context, false)
            val datafile =
              workerDirs.map(dir => {
                new File(s"$dir/celeborn-worker/shuffle_data/$appUniqueId/$celebornShuffleId")
              }).filter(_.exists())
                .flatMap(_.listFiles().iterator).headOption
            datafile match {
              case Some(file) => file.delete()
              case None => throw new RuntimeException("unexpected, there must be some data file")
            }
          }
          case _ => throw new RuntimeException("unexpected, only support RssShuffleHandle here")
        }
        executed.set(true)
      }
    }
  }

  test("celeborn spark integration test - Fetch Failure") {
    val params = Array(true, false)
    for (pi <- params.indices) {
      val mockReserveFailure: Boolean = params(pi)
      if (Spark3OrNewer) {
        val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
        val sparkSession = SparkSession.builder()
          .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
          .config(s"spark.${CelebornConf.TEST_CLIENT_MOCK_RESERVE_SLOTS_FAILURE.key}", mockReserveFailure.toString)
          .config("spark.sql.shuffle.partitions", 2)
          .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
          .config("spark.celeborn.shuffle.enabled", "true")
          .config("spark.celeborn.client.spark.fetch.throwsFetchFailure", "true")
          .config(
            "spark.shuffle.manager",
            "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
          .getOrCreate()

        val celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext.getConf)
        val hook = new ShuffleReaderGetHook(celebornConf)
        TestCelebornShuffleManager.registerReaderGetHook(hook)

        val value = Range(1, 10000).mkString(",")
        val tuples = sparkSession.sparkContext.parallelize(1 to 10000, 2)
          .map { i => (i, value) }.groupByKey(16).collect()

        // verify result
        assert(hook.executed.get() == true)
        assert(tuples.length == 10000)
        for (elem <- tuples) {
          assert(elem._2.mkString(",").equals(value))
        }

        val shuffleMgr = SparkContextHelper.env
          .shuffleManager
          .asInstanceOf[TestCelebornShuffleManager]
        val lifecycleManager = shuffleMgr.getLifecycleManager

        shuffleMgr.unregisterShuffle(0)
        assert(lifecycleManager.getUnregisterShuffleTime().containsKey(0))
        assert(lifecycleManager.getUnregisterShuffleTime().containsKey(1))

        sparkSession.stop()
      }
    }
  }

  test("celeborn spark integration test - unregister shuffle with throwsFetchFailure disabled") {
    val params = Array(true, false)
    for (pi <- params.indices) {
      val mockReserveFailure: Boolean = params(pi)
      if (Spark3OrNewer) {
        val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
        val sparkSession = SparkSession.builder()
          .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
          .config(s"spark.${CelebornConf.TEST_CLIENT_MOCK_RESERVE_SLOTS_FAILURE.key}", mockReserveFailure.toString)
          .config("spark.sql.shuffle.partitions", 2)
          .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
          .config("spark.celeborn.shuffle.enabled", "true")
          .config("spark.celeborn.client.spark.fetch.throwsFetchFailure", "false")
          .getOrCreate()

        val value = Range(1, 10000).mkString(",")
        val tuples = sparkSession.sparkContext.parallelize(1 to 10000, 2)
          .map { i => (i, value) }.groupByKey(16).collect()

        // verify result
        assert(tuples.length == 10000)
        for (elem <- tuples) {
          assert(elem._2.mkString(",").equals(value))
        }

        val shuffleMgr = SparkContextHelper.env
          .shuffleManager
          .asInstanceOf[SparkShuffleManager]
        val lifecycleManager = shuffleMgr.getLifecycleManager

        shuffleMgr.unregisterShuffle(0)
        assert(lifecycleManager.getUnregisterShuffleTime().containsKey(0))

        sparkSession.stop()
      }
    }
  }

  test("celeborn spark integration test - Fetch Failure with multiple shuffle data") {
    val params = Array(true, false)
    for (pi <- params.indices) {
      val mockReserveFailure: Boolean = params(pi)
      if (Spark3OrNewer) {
        val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
        val sparkSession = SparkSession.builder()
          .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
          .config(s"spark.${CelebornConf.TEST_CLIENT_MOCK_RESERVE_SLOTS_FAILURE.key}", mockReserveFailure.toString)
          .config("spark.sql.shuffle.partitions", 2)
          .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
          .config("spark.celeborn.shuffle.enabled", "true")
          .config("spark.celeborn.client.spark.fetch.throwsFetchFailure", "true")
          .config(
            "spark.shuffle.manager",
            "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
          .getOrCreate()

        val celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext.getConf)
        val hook = new ShuffleReaderGetHook(celebornConf)
        TestCelebornShuffleManager.registerReaderGetHook(hook)

        import sparkSession.implicits._

        val df1 = Seq((1, "a"), (2, "b")).toDF("id", "data").groupBy("id").count()
        val df2 = Seq((2, "c"), (2, "d")).toDF("id", "data").groupBy("id").count()
        val tuples = df1.hint("merge").join(df2, "id").select("*").collect()

        // verify result
        assert(hook.executed.get() == true)
        val expect = "[2,1,2]"
        assert(tuples.head.toString().equals(expect))
        sparkSession.stop()
      }
    }
  }

  test("celeborn spark integration test - Fetch Failure with RDD reuse") {
    val params = Array(true, false)
    for (pi <- params.indices) {
      val mockReserveFailure: Boolean = params(pi)
      if (Spark3OrNewer) {
        val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
        val sparkSession = SparkSession.builder()
          .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
          .config(s"spark.${CelebornConf.TEST_CLIENT_MOCK_RESERVE_SLOTS_FAILURE.key}", mockReserveFailure.toString)
          .config("spark.sql.shuffle.partitions", 2)
          .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
          .config("spark.celeborn.shuffle.enabled", "true")
          .config("spark.celeborn.client.spark.fetch.throwsFetchFailure", "true")
          .config(
            "spark.shuffle.manager",
            "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
          .getOrCreate()

        val celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext.getConf)
        val hook = new ShuffleReaderGetHook(celebornConf)
        TestCelebornShuffleManager.registerReaderGetHook(hook)

        val sc = sparkSession.sparkContext
        val rdd1 = sc.parallelize(0 until 10000, 3).map(v => (v, v)).groupByKey()
        val rdd2 = sc.parallelize(0 until 10000, 2).map(v => (v, v)).groupByKey()
        val rdd3 = rdd1.map(v => (v._2, v._1))

        hook.executed.set(true)

        rdd1.count()
        rdd2.count()

        hook.executed.set(false)
        rdd3.count()
        hook.executed.set(false)
        rdd3.count()
        hook.executed.set(false)
        rdd3.count()
        hook.executed.set(false)
        rdd3.count()

        sparkSession.stop()
      }
    }
  }

  test("celeborn spark integration test - Fetch Failure with read write shuffles in one stage") {
    val params = Array(true, false)
    for (pi <- params.indices) {
      val mockReserveFailure: Boolean = params(pi)
      if (Spark3OrNewer) {
        val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
        val sparkSession = SparkSession.builder()
          .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
          .config(s"spark.${CelebornConf.TEST_CLIENT_MOCK_RESERVE_SLOTS_FAILURE.key}", mockReserveFailure.toString)
          .config("spark.sql.shuffle.partitions", 2)
          .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
          .config("spark.celeborn.shuffle.enabled", "true")
          .config("spark.celeborn.client.spark.fetch.throwsFetchFailure", "true")
          .config(
            "spark.shuffle.manager",
            "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
          .getOrCreate()

        val celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext.getConf)
        val hook = new ShuffleReaderGetHook(celebornConf)
        TestCelebornShuffleManager.registerReaderGetHook(hook)

        val sc = sparkSession.sparkContext
        val rdd1 = sc.parallelize(0 until 10000, 3).map(v => (v, v)).groupByKey()
        val rdd2 = rdd1.map(v => (v._2, v._1)).groupByKey()

        hook.executed.set(true)
        rdd1.count()

        hook.executed.set(false)
        rdd2.count()

        sparkSession.stop()
      }
    }

  }

  test("celeborn spark integration test - empty shuffle data") {
    val params = Array(true, false)
    for (pi <- params.indices) {
      val mockReserveFailure: Boolean = params(pi)
      if (Spark3OrNewer) {
        val sparkConf = new SparkConf().setAppName("rss-demo").setMaster("local[2,3]")
        val sparkSession = SparkSession.builder()
          .config(updateSparkConf(sparkConf, ShuffleMode.HASH))
          .config(s"spark.${CelebornConf.TEST_CLIENT_MOCK_RESERVE_SLOTS_FAILURE.key}", mockReserveFailure.toString)
          .config("spark.sql.shuffle.partitions", 2)
          .config("spark.celeborn.shuffle.forceFallback.partition.enabled", false)
          .config("spark.celeborn.shuffle.enabled", "true")
          .config("spark.celeborn.client.spark.fetch.throwsFetchFailure", "true")
          .getOrCreate()

        sparkSession.sql("create table if not exists t_1 (a bigint) using parquet")
        sparkSession.sql("create table if not exists t_2 (a bigint) using parquet")
        sparkSession.sql("create table if not exists t_3 (a bigint) using parquet")
        val df1 = sparkSession.table("t_1")
        val df2 = sparkSession.table("t_2")
        val df3 = sparkSession.table("t_3")
        df1.union(df2).union(df3).count()

        sparkSession.stop()
      }
    }
  }

  test(s"celeborn spark integration test - fetch failure with skewed data and reused exchange") {
    val params = Array((true,false),(false,false),(false,true),(true,true))
    for (pi <- params.indices) {
      val (mockReserveFailure: Boolean, replicateEnabled: Boolean) = params(pi)
      if (Spark3OrNewer) {
        val sparkConf = new SparkConf().setAppName("celeborn-demo")
          .setMaster("local[2]")
          .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
          .set(s"spark.${CelebornConf.TEST_CLIENT_MOCK_RESERVE_SLOTS_FAILURE.key}", mockReserveFailure.toString)
          .set("spark.sql.adaptive.skewJoin.enabled", "true")
          .set("spark.sql.adaptive.coalescePartitions.enabled", "false")
          .set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "100KB")
          .set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "100KB")
          .set("spark.sql.adaptive.skewJoin.skewedPartitionFactor","2")
          .set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
          .set(SQLConf.PARQUET_COMPRESSION.key, "gzip")
          .set(s"spark.${CelebornConf.SHUFFLE_COMPRESSION_CODEC.key}", "NONE")
          .set(s"spark.${CelebornConf.SHUFFLE_RANGE_READ_FILTER_ENABLED.key}", "true")
          .set(s"spark.${CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key}", replicateEnabled.toString)
          .set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "8")
          .set("spark.celeborn.client.spark.fetch.throwsFetchFailure", "true")
          .set("spark.sql.shuffle.partitions", "8")
          .set(
            "spark.shuffle.manager",
            "org.apache.spark.shuffle.celeborn.TestCelebornShuffleManager")
          .set(s"spark.${CelebornConf.MASTER_ENDPOINTS.key}", masterInfo._1.rpcEnv.address.toString)
          .set(s"spark.${CelebornConf.SHUFFLE_PARTITION_SPLIT_THRESHOLD.key}", "1KB")
        val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        val celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext.getConf)
        val hook = new ShuffleReaderGetHook(celebornConf)
        TestCelebornShuffleManager.registerReaderGetHook(hook)
        import sparkSession.implicits._
        val nonSkewedSize = 7
        val nonSkewedData = (1 to nonSkewedSize).map(i => {
          val key = 2 + (i % 7) // 2~8
          (key, s"FieldA-$i", s"FieldB-$i", s"FieldC-$i", s"FieldD-$i")
        })
        val skewedSize = 200000
        val skewedData = (1 to skewedSize).map(i => (1, s"fsa-$i", s"fsb-$i", s"fsc-$i", s"fsd-$i"))
        val allData = skewedData ++ nonSkewedData
        val df = sparkSession.sparkContext.parallelize(allData, 8).toDF("fa", "f1", "f2", "f3", "f4")
        df.createOrReplaceTempView("view1")
        val smallTableData = Seq(
          (1, "S1", "S2", "S3", "S4"),
          (1, "S5", "S6", "S7", "S8"),
          (2, "S9", "S10", "S11", "S12"),
          (3, "S13", "S14", "S15", "S16"),
          (4, "S17", "S18", "S19", "S20"),
          (5, "S21", "S22", "S23", "S24"),
          (6, "S25", "S26", "S27", "S28"),
          (7, "S29", "S30", "S31", "S32"),
          (8, "S33", "S34", "S35", "S36")
        )
        val df2 = sparkSession.sparkContext.parallelize(smallTableData, 8).toDF("fb", "f6", "f7", "f8", "f9")
        df2.createOrReplaceTempView("view2")
        sparkSession.sql("SELECT * from view1 a inner join view2 b on a.fa=b.fb")
          .createOrReplaceTempView("v1")
        val result = sparkSession.sql("select count(*) from ( select * from v1 union all select * from v1)").collect()(0).getLong(0)
        // verify result
        assert(hook.executed.get() == true)
        assert(result == 800014, s"Expected 800014 rows but got $result")
        sparkSession.stop()
      }
    }
  }
}
