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

package org.apache.celeborn.client

import java.util
import java.util.{Set => JSet}
import java.util.concurrent.{ConcurrentHashMap, ScheduledExecutorService, ScheduledFuture, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.protocol.PartitionLocation
import org.apache.celeborn.common.protocol.message.ControlMessages.WorkerResource
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.util.{JavaUtils, ThreadUtils, Utils}

case class ChangePartitionRequest(
    context: RequestLocationCallContext,
    shuffleId: Int,
    partitionId: Int,
    epoch: Int,
    oldPartition: PartitionLocation,
    causes: Option[StatusCode])

class ChangePartitionManager(
    conf: CelebornConf,
    lifecycleManager: LifecycleManager) extends Logging {

  private val pushReplicateEnabled = conf.clientPushReplicateEnabled
  // shuffleId -> (partitionId-splitStart-splitEnd -> set of ChangePartition)
  private val changePartitionRequests =
    JavaUtils.newConcurrentHashMap[Int, ConcurrentHashMap[String, JSet[ChangePartitionRequest]]]()
  // shuffleId -> locks
  private val locks = JavaUtils.newConcurrentHashMap[Int, Array[AnyRef]]()
  private val lockBucketSize = conf.batchHandleChangePartitionBuckets
  // shuffleId -> set of partitionId-splitStart-splitEnd
  private val inBatchPartitions =
    JavaUtils.newConcurrentHashMap[Int, ConcurrentHashMap.KeySetView[String, java.lang.Boolean]]()

  private val batchHandleChangePartitionEnabled = conf.batchHandleChangePartitionEnabled
  private val batchHandleChangePartitionExecutors = ThreadUtils.newDaemonCachedThreadPool(
    "celeborn-client-lifecycle-manager-change-partition-executor",
    conf.batchHandleChangePartitionNumThreads)
  private val batchHandleChangePartitionRequestInterval =
    conf.batchHandleChangePartitionRequestInterval
  private val batchHandleChangePartitionSchedulerThread: Option[ScheduledExecutorService] =
    if (batchHandleChangePartitionEnabled) {
      Some(ThreadUtils.newDaemonSingleThreadScheduledExecutor(
        "celeborn-client-lifecycle-manager-change-partition-scheduler"))
    } else {
      None
    }

  private var batchHandleChangePartition: Option[ScheduledFuture[_]] = _

  private val testRetryRevive = conf.testRetryRevive

  def start(): Unit = {
    logInfo("Starting ChangePartitionManager")
    batchHandleChangePartition = batchHandleChangePartitionSchedulerThread.map {
      // noinspection ConvertExpressionToSAM
      _.scheduleWithFixedDelay(
        new Runnable {
          override def run(): Unit = {
            try {
              changePartitionRequests.asScala.foreach { case (shuffleId, requests) =>
                batchHandleChangePartitionExecutors.submit {
                  new Runnable {
                    override def run(): Unit = {
                      // 记录 synchronized 块开始时间和元素数量
                      val startTime = System.nanoTime()
                      val elementCount = requests.size()

                      val distinctPartitions = {
                        val requestSet = inBatchPartitions.get(shuffleId)
                        val locksForShuffle = locks.computeIfAbsent(shuffleId, locksRegisterFunc)
                        requests.asScala.map { case (partitionSplitRange, request) =>
                          locksForShuffle(
                            (partitionSplitRange.hashCode & 0x7FFFFFFF) % locksForShuffle.length).synchronized {
                            if (!requestSet.contains(partitionSplitRange) && requests.containsKey(
                                partitionSplitRange)) {
                              requestSet.add(partitionSplitRange)
                              Some(request.asScala.toArray.maxBy(_.epoch))
                            } else {
                              None
                            }
                          }
                        }.filter(_.isDefined).map(_.get).toArray
                      }

                      // 直接打印日志：函数名、耗时和元素数
                      val elapsedUs = (System.nanoTime() - startTime) / 1000.0
                      logInfo(
                        f"[LOCK_PERF] func=start-run shuffleId=$shuffleId elements=$elementCount timeUs=$elapsedUs%.1f")

                      if (distinctPartitions.nonEmpty) {
                        handleRequestPartitions(
                          shuffleId,
                          distinctPartitions)
                      }
                    }
                  }
                }
              }
            } catch {
              case e: InterruptedException =>
                logError("Partition split scheduler thread is shutting down, detail: ", e)
                throw e
            }
          }
        },
        0,
        batchHandleChangePartitionRequestInterval,
        TimeUnit.MILLISECONDS)
    }
  }

  def stop(): Unit = {
    logInfo("Stopping ChangePartitionManager")
    batchHandleChangePartition.foreach(_.cancel(true))
    batchHandleChangePartitionSchedulerThread.foreach(ThreadUtils.shutdown(_))
  }

  private val rpcContextRegisterFunc =
    new util.function.Function[
      Int,
      ConcurrentHashMap[String, util.Set[ChangePartitionRequest]]]() {
      override def apply(s: Int): ConcurrentHashMap[String, util.Set[ChangePartitionRequest]] =
        JavaUtils.newConcurrentHashMap()
    }

  private val inBatchShuffleIdRegisterFunc =
    new util.function.Function[Int, ConcurrentHashMap.KeySetView[String, java.lang.Boolean]]() {
      override def apply(s: Int): ConcurrentHashMap.KeySetView[String, java.lang.Boolean] =
        ConcurrentHashMap.newKeySet[String]()
    }

  private val locksRegisterFunc = new util.function.Function[Int, Array[AnyRef]] {
    override def apply(t: Int): Array[AnyRef] = {
      Array.fill(lockBucketSize)(new AnyRef())
    }
  }

  def handleRequestPartitionLocation(
      context: RequestLocationCallContext,
      shuffleId: Int,
      partitionId: Int,
      oldEpoch: Int,
      oldPartition: PartitionLocation,
      cause: Option[StatusCode] = None): Unit = {

    val changePartition = ChangePartitionRequest(
      context,
      shuffleId,
      partitionId,
      oldEpoch,
      oldPartition,
      cause)
    // check if there exists request for the partition, if do just register
    val requests = changePartitionRequests.computeIfAbsent(shuffleId, rpcContextRegisterFunc)
    inBatchPartitions.computeIfAbsent(shuffleId, inBatchShuffleIdRegisterFunc)

    lifecycleManager.commitManager.registerCommitPartitionRequest(
      shuffleId,
      oldPartition,
      cause)

    val partitionSplitRange =
      if (oldPartition != null) oldPartition.getSplitRange else String.valueOf(partitionId)

    logInfo(s"[handleRequestPartitionLocation] For $shuffleId," +
      s"request for partition $partitionSplitRange")

    val locksForShuffle = locks.computeIfAbsent(shuffleId, locksRegisterFunc)
    locksForShuffle(
      (partitionSplitRange.hashCode & 0x7FFFFFFF) % locksForShuffle.length).synchronized {
      if (requests.containsKey(partitionSplitRange)) {
        requests.get(partitionSplitRange).add(changePartition)
        logInfo(s"[handleRequestPartitionLocation] For $shuffleId, request for same partition" +
          s"$partitionSplitRange-$oldEpoch exists, register context.")
        return
      } else {
        // If new slot for the partition has been allocated, reply and return.
        // Else register and allocate for it.
        getLatestPartition(shuffleId, oldPartition, partitionId, oldEpoch).foreach { latestLoc =>
          context.reply(
            partitionId,
            StatusCode.SUCCESS,
            Some(latestLoc),
            lifecycleManager.workerStatusTracker.workerAvailable(oldPartition))
          logInfo(s"[handleRequestPartitionLocation]: For shuffle: $shuffleId" +
            s" old partition: $partitionSplitRange-$oldEpoch, " +
            s"new partition: $latestLoc ${latestLoc.getSplitRange}-${latestLoc.getEpoch} found, return it")
          return
        }
        val set = new util.HashSet[ChangePartitionRequest]()
        set.add(changePartition)
        requests.put(partitionSplitRange, set)
      }
    }
    if (!batchHandleChangePartitionEnabled) {
      handleRequestPartitions(shuffleId, Array(changePartition))
    }
  }

  private def getLatestPartition(
      shuffleId: Int,
      oldPartition: PartitionLocation,
      partitionId: Int,
      epoch: Int): Option[PartitionLocation] = {
    val map = lifecycleManager.latestPartitionLocation.get(shuffleId)
    if (map != null) {
      val locationManager = map.get(partitionId)
      if (locationManager != null) {
        val loc = locationManager.getLatestPartitionLocation(oldPartition)
        if (loc != null && loc.getEpoch > epoch) {
          return Some(loc)
        }
      }
    }
    None
  }

  def handleRequestPartitions(
      shuffleId: Int,
      changePartitions: Array[ChangePartitionRequest]): Unit = {
    val requestsMap = changePartitionRequests.get(shuffleId)

    logInfo(s"[handleRequestPartitions] Specific For $shuffleId")

    val changes = changePartitions.map { change =>
      s"${change.shuffleId}-${change.partitionId}-${change.epoch}-${change.oldPartition}"
    }.mkString("[", ",", "]")
    logDebug(s"Batch handle change partition for $changes")

    // Exclude all failed workers
    if (changePartitions.exists(_.causes.isDefined) && !testRetryRevive) {
      changePartitions.filter(_.causes.isDefined).foreach { changePartition =>
        lifecycleManager.workerStatusTracker.excludeWorkerFromPartition(
          shuffleId,
          changePartition.oldPartition,
          changePartition.causes.get)
      }
    }

    // remove together to reduce lock time
    def replySuccess(locations: Array[PartitionLocation]): Unit = {
      val startTime = System.nanoTime()
      val elementCount = locations.length
      val locksForShuffle = locks.computeIfAbsent(shuffleId, locksRegisterFunc)
      val results = locations.map { location =>
        // location.getParent will be null when partitionType is MAP
        val partitionSplitRange =
          if (location.getParent != null) location.getParent.getSplitRange
          else String.valueOf(location.getId)
        locksForShuffle(
          (partitionSplitRange.hashCode & 0x7FFFFFFF) % locksForShuffle.length).synchronized {
          if (batchHandleChangePartitionEnabled) {
            inBatchPartitions.get(shuffleId).remove(partitionSplitRange)
          }
          // Here one partition id can be remove more than once,
          // so need to filter null result before reply.
          location -> Option(requestsMap.remove(partitionSplitRange))
        }
      }
      val elapsedUs = (System.nanoTime() - startTime) / 1000.0
      logInfo(f"[LOCK_PERF] func=replySuccess shuffleId=$shuffleId elements=$elementCount timeUs=$elapsedUs%.1f")

      results.foreach { case (newLocation, requests) =>
        requests.map(_.asScala.toList.foreach(req =>
          req.context.reply(
            req.partitionId,
            StatusCode.SUCCESS,
            if (newLocation.getParent != null)
              Option(lifecycleManager.latestPartitionLocation.get(shuffleId)
                .get(req.partitionId).getRandomChild(newLocation.getParent))
            else Option(newLocation),
            lifecycleManager.workerStatusTracker.workerAvailable(req.oldPartition))))
      }
    }

    // remove together to reduce lock time
    def replyFailure(status: StatusCode): Unit = {
      val startTime = System.nanoTime()
      val elementCount = changePartitions.length
      val results = changePartitions.map { changePartition =>
        // changePartition.oldPartition will be null when partitionType is MAP
        val partitionSplitRange =
          if (changePartition.oldPartition != null) changePartition.oldPartition.getSplitRange
          else String.valueOf(changePartition.partitionId)
        val locksForShuffle = locks.computeIfAbsent(shuffleId, locksRegisterFunc)
        locksForShuffle(
          (partitionSplitRange.hashCode & 0x7FFFFFFF) % locksForShuffle.length).synchronized {
          if (batchHandleChangePartitionEnabled) {
            inBatchPartitions.get(shuffleId).remove(partitionSplitRange)
          }
          Option(requestsMap.remove(partitionSplitRange))
        }
      }
      val elapsedUs = (System.nanoTime() - startTime) / 1000.0
      logInfo(f"[LOCK_PERF] func=replyFailure shuffleId=$shuffleId elements=$elementCount timeUs=$elapsedUs%.1f")

      results.foreach { requests =>
        requests.map(_.asScala.toList.foreach(req =>
          req.context.reply(
            req.partitionId,
            status,
            None,
            lifecycleManager.workerStatusTracker.workerAvailable(req.oldPartition))))
      }
    }

    // Get candidate worker that not in excluded worker list of shuffleId
    val candidates =
      lifecycleManager
        .workerSnapshots(shuffleId)
        .keySet()
        .asScala
        .filter(lifecycleManager.workerStatusTracker.workerAvailable)
        .toList
    if (candidates.size < 1 || (pushReplicateEnabled && candidates.size < 2)) {
      logError("[Update partition] failed for not enough candidates for revive.")
      replyFailure(StatusCode.SLOT_NOT_AVAILABLE)
      return
    }

    // PartitionSplit all contains oldPartition
    val newlyAllocatedLocations =
      reallocateChangePartitionRequestSlotsFromCandidates(
        shuffleId,
        changePartitions.toList,
        candidates)

    if (!lifecycleManager.reserveSlotsWithRetry(
        shuffleId,
        new util.HashSet(candidates.toSet.asJava),
        newlyAllocatedLocations)) {
      logError(s"[Update partition] failed for $shuffleId.")
      replyFailure(StatusCode.RESERVE_SLOTS_FAILED)
      return
    }

    val newPrimaryLocations =
      newlyAllocatedLocations.asScala.flatMap {
        case (workInfo, (primaryLocations, replicaLocations)) =>
          // Add all re-allocated slots to worker snapshots.
          lifecycleManager.workerSnapshots(shuffleId).asScala
            .get(workInfo)
            .foreach { partitionLocationInfo =>
              partitionLocationInfo.addPrimaryPartitions(primaryLocations)
              partitionLocationInfo.addReplicaPartitions(replicaLocations)
            }
          // partition location can be null when call reserveSlotsWithRetry().
          val locations = (primaryLocations.asScala ++ replicaLocations.asScala.map(_.getPeer))
            .distinct.filter(_ != null)
          if (locations.nonEmpty) {
            val changes = locations.map { partition =>
              s"(partition ${partition.getSplitRange} epoch from ${partition.getEpoch - 1} to ${partition.getEpoch})"
            }.mkString("[", ", ", "]")
            logDebug(s"[Update partition] success for " +
              s"shuffle $shuffleId, succeed partitions: " +
              s"$changes.")
          }
          locations
      }
    replySuccess(newPrimaryLocations.toArray)
  }

  private def reallocateChangePartitionRequestSlotsFromCandidates(
      shuffleId: Int,
      changePartitionRequests: List[ChangePartitionRequest],
      candidates: List[WorkerInfo]): WorkerResource = {
    val slots = new WorkerResource()
    changePartitionRequests.foreach { partition =>
      lifecycleManager.allocateFromCandidates(
        shuffleId,
        partition.partitionId,
        partition.oldPartition,
        partition.epoch,
        candidates,
        slots,
        if (partition.oldPartition != null) partition.oldPartition.getSplitStart else -1,
        if (partition.oldPartition != null) partition.oldPartition.getSplitEnd else -1,
        conf.clientPartitionSplitNum)
    }
    slots
  }

  def removeExpiredShuffle(shuffleId: Int): Unit = {
    changePartitionRequests.remove(shuffleId)
    inBatchPartitions.remove(shuffleId)
    locks.remove(shuffleId)
  }
}
