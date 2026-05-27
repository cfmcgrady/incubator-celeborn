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

package org.apache.celeborn.tests.client

import java.util

import scala.collection.JavaConverters._

import org.apache.celeborn.client.{LifecycleManager, ShuffleClientImpl, WithShuffleClientSuite}
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.service.deploy.MiniClusterFeature

class LifecycleManagerSuite extends WithShuffleClientSuite with MiniClusterFeature {
  private val masterPort = 19097

  celebornConf.set(CelebornConf.MASTER_ENDPOINTS.key, s"localhost:$masterPort")
    .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, "true")
    .set(CelebornConf.CLIENT_PUSH_BUFFER_MAX_SIZE.key, "256K")

  override def beforeAll(): Unit = {
    super.beforeAll()
    val masterConf = Map(
      "celeborn.master.host" -> "localhost",
      "celeborn.master.port" -> masterPort.toString)
    val workerConf = Map(
      "celeborn.master.endpoints" -> s"localhost:$masterPort")
    setUpMiniCluster(masterConf, workerConf)
  }

  test("CELEBORN-1151: test request slots with client blacklist worker with filter enabled") {
    celebornConf.set(CelebornConf.REGISTER_SHUFFLE_FILTER_EXCLUDED_WORKER_ENABLED, true)
    val lifecycleManager: LifecycleManager = new LifecycleManager(APP, celebornConf)

    // test request slots without worker excluded
    val headWorkerInfo = workerInfos.keySet.head.workerInfo
    val res1 = lifecycleManager.requestMasterRequestSlotsWithRetry(0, 11)
      .workerResource.keySet()
    assert(res1.contains(headWorkerInfo))

    // test request slots with 1 worker excluded, result should not contains the excluded worker
    val commitFilesFailedWorkers = new LifecycleManager.ShuffleFailedWorkers()
    commitFilesFailedWorkers.put(
      workerInfos.keySet.head.workerInfo,
      (StatusCode.PUSH_DATA_TIMEOUT_PRIMARY, System.currentTimeMillis()))
    lifecycleManager.workerStatusTracker.recordWorkerFailure(commitFilesFailedWorkers)
    val res2 = lifecycleManager.requestMasterRequestSlotsWithRetry(1, 11)
      .workerResource.keySet()
    assert(!res2.contains(headWorkerInfo))

    // test request slots with all workers excluded, response should be WORKER_EXCLUDED
    workerInfos.keySet.foreach(worker =>
      commitFilesFailedWorkers.put(
        worker.workerInfo,
        (StatusCode.PUSH_DATA_TIMEOUT_PRIMARY, System.currentTimeMillis())))
    lifecycleManager.workerStatusTracker.recordWorkerFailure(commitFilesFailedWorkers)
    val status = lifecycleManager.requestMasterRequestSlotsWithRetry(2, 11).status
    assert(status == StatusCode.WORKER_EXCLUDED)

    lifecycleManager.stop()
  }

  test("CELEBORN-1151: test request slots with client blacklist worker with filter not enabled") {
    celebornConf.set(CelebornConf.REGISTER_SHUFFLE_FILTER_EXCLUDED_WORKER_ENABLED, false)
    val lifecycleManager: LifecycleManager = new LifecycleManager(APP, celebornConf)

    // test request slots with all workers excluded, response should not excluded any worker
    val commitFilesFailedWorkers = new LifecycleManager.ShuffleFailedWorkers()
    workerInfos.keySet.foreach(worker =>
      commitFilesFailedWorkers.put(
        worker.workerInfo,
        (StatusCode.PUSH_DATA_TIMEOUT_PRIMARY, System.currentTimeMillis())))
    lifecycleManager.workerStatusTracker.recordWorkerFailure(commitFilesFailedWorkers)
    val res = lifecycleManager.requestMasterRequestSlotsWithRetry(0, 11)
      .workerResource.keySet()
    assert(res.size() == workerInfos.size)
    assert(res.contains(workerInfos.keySet.head.workerInfo))
    lifecycleManager.stop()
  }

  test("test extra slots increases allocated partition count") {
    val extraSlots = 10
    val numPartitions = 11
    val extraSlotsConf = new CelebornConf()
      .set(CelebornConf.MASTER_ENDPOINTS.key, s"localhost:$masterPort")
      .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, "false")
      .set(CelebornConf.CLIENT_SLOT_ASSIGN_EXTRA_SLOTS.key, extraSlots.toString)

    val app = s"app-extra-slots-${System.currentTimeMillis()}"
    val lifecycleManager = new LifecycleManager(app, extraSlotsConf)
    val shuffleClient = new ShuffleClientImpl(app, extraSlotsConf, userIdentifier)
    shuffleClient.setupLifecycleManagerRef(lifecycleManager.self)

    val shuffleId = 0
    shuffleClient.getPartitionLocation(shuffleId, 8, numPartitions)

    val totalSlots = lifecycleManager.workerSnapshots(shuffleId)
      .values().asScala
      .map(_.getPrimaryPartitions().size()).sum
    assert(
      totalSlots == numPartitions + extraSlots,
      s"Expected ${numPartitions + extraSlots} slots but got $totalSlots")

    shuffleClient.shutdown()
    lifecycleManager.stop()
  }

  test("test zero extra slots keeps original partition count") {
    val numPartitions = 11
    val zeroExtraSlotsConf = new CelebornConf()
      .set(CelebornConf.MASTER_ENDPOINTS.key, s"localhost:$masterPort")
      .set(CelebornConf.CLIENT_PUSH_REPLICATE_ENABLED.key, "false")
      .set(CelebornConf.CLIENT_SLOT_ASSIGN_EXTRA_SLOTS.key, "0")

    val app = s"app-zero-extra-slots-${System.currentTimeMillis()}"
    val lifecycleManager = new LifecycleManager(app, zeroExtraSlotsConf)
    val shuffleClient = new ShuffleClientImpl(app, zeroExtraSlotsConf, userIdentifier)
    shuffleClient.setupLifecycleManagerRef(lifecycleManager.self)

    val shuffleId = 0
    shuffleClient.getPartitionLocation(shuffleId, 8, numPartitions)

    val totalSlots = lifecycleManager.workerSnapshots(shuffleId)
      .values().asScala
      .map(_.getPrimaryPartitions().size()).sum
    assert(totalSlots == numPartitions, s"Expected $numPartitions slots but got $totalSlots")

    shuffleClient.shutdown()
    lifecycleManager.stop()
  }

  override def afterAll(): Unit = {
    logInfo("all test complete , stop celeborn mini cluster")
    shutdownMiniCluster()
  }
}
