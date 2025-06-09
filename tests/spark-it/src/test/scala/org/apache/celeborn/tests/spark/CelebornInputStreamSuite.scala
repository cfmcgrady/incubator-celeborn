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

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.CompressionCodec
import org.apache.celeborn.common.protocol.ShuffleMode

class CelebornInputStreamSuite extends AnyFunSuite
  with SparkTestBase
  with BeforeAndAfterEach {

  override def beforeAll(): Unit = {
    logInfo("test initialized , setup Celeborn mini cluster")
    val workerConf = Map(
      "celeborn.shuffle.chunk.size" -> "10k",
      "celeborn.test.mockGetReplicaChunkBlock" -> "1")
    setUpMiniCluster(workerConf=workerConf, workerNum = 5)
  }

  override def beforeEach(): Unit = {
    ShuffleClient.reset()
  }

  override def afterEach(): Unit = {
    System.gc()
  }

  test(s"celeborn spark integration test - test blocking at getFirstChunk") {
    testGetNextChunk(1)
  }

  test(s"celeborn spark integration test - test blocking at moveToNextReader") {
    testGetNextChunk(2)
  }

  test(s"celeborn spark integration test - test blocking at moveToNextChunk") {
    testGetNextChunk(3)
  }

  private def enableCeleborn(conf: SparkConf) = {
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.celeborn.SparkShuffleManager")
      .set(s"spark.${CelebornConf.MASTER_ENDPOINTS.key}", masterInfo._1.rpcEnv.address.toString)
      .set(s"spark.${CelebornConf.SHUFFLE_PARTITION_SPLIT_THRESHOLD.key}", "3MB")
  }

  def testGetNextChunk(mockGetReplicaChunkBlock:Int = 0): Unit = {
    val codec = CompressionCodec.ZSTD
    val sparkConf = new SparkConf().setAppName("celeborn-demo")
      .setMaster("local[2]")
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      .set("spark.sql.adaptive.skewJoin.enabled", "true")
      .set("spark.sql.adaptive.coalescePartitions.enabled", "false")
      .set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "1MB")
      .set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "1MB")
      .set("spark.sql.adaptive.skewJoin.skewedPartitionFactor","2")
      .set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
      .set(SQLConf.PARQUET_COMPRESSION.key, "gzip")
      .set(s"spark.${CelebornConf.SHUFFLE_COMPRESSION_CODEC.key}", codec.name)
      .set(s"spark.${CelebornConf.SHUFFLE_RANGE_READ_FILTER_ENABLED.key}", "true")
      .set(s"spark.${CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key}", "true")
      .set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "8")
      .set("spark.celeborn.test.mockGetReplicaChunkBlock", mockGetReplicaChunkBlock.toString)
      .set("spark.sql.shuffle.partitions", "8")

    enableCeleborn(sparkConf)

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._

    val nonSkewedSize = 7
    var set = new mutable.HashSet[Int]()

    val nonSkewedData = (1 to nonSkewedSize).map(i => {
      val key = 2 + (i % 7) // 2~8
      set.add(key)
      (key, s"FieldA-$i", s"FieldB-$i", s"FieldC-$i", s"FieldD-$i")
    })

    val skewedSize = 3000000
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

    sparkSession.sql("drop table if exists fres")
    sparkSession.sql("create table fres using parquet as select * from view1 a inner join view2 b on a.fa=b.fb")

    val result = sparkSession.sql("select count(*) from fres where fa=1").collect()(0).getLong(0)
    sparkSession.sql("drop table fres")
    sparkSession.stop()
    assert(result == 6000000, s"Expected 200000 rows but got $result")
  }
}
