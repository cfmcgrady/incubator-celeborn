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

package org.apache.celeborn.common.util

import java.util
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.util.Random

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.protocol.{PartitionLocation, PbCompactFileGroup, PbFileGroup, PbGetReducerFileGroupResponse, StorageInfo}
import org.apache.celeborn.common.protocol.message.ControlMessages
import org.apache.celeborn.common.protocol.message.ControlMessages.GetReducerFileGroupResponse
import org.apache.celeborn.common.protocol.message.StatusCode

/**
 * Unit test to verify the dictionary compression effectiveness for
 * PbGetReducerFileGroupResponse under realistic production configurations.
 *
 * Production scenario from online logs:
 *   - 2048 worker nodes
 *   - ~230,000 partition locations (115,000 partitions with replica)
 *   - Original GetReducerFileGroupResponse size: ~152MB
 *
 * The compact format uses a worker dictionary: each unique worker is stored
 * once in the dictionary, and partition locations reference workers by index
 * instead of embedding full worker info (host + 4 ports) in every entry.
 */
class CompactFileGroupCompressionTest extends CelebornFunSuite {

  /**
   * Build a realistic fileGroup map simulating production scenario.
   *
   * @param numWorkers    number of unique worker nodes
   * @param numPartitions number of reduce partitions
   * @param withReplica   whether each partition has a replica peer (doubles location count)
   * @param numMappers    number of map tasks (affects mapIdBitmap size)
   */
  private def buildFileGroups(
      numWorkers: Int,
      numPartitions: Int,
      withReplica: Boolean,
      numMappers: Int = 200): ConcurrentHashMap[Integer, util.Set[PartitionLocation]] = {
    val random = new Random(42)
    val fileGroups = new ConcurrentHashMap[Integer, util.Set[PartitionLocation]]()

    val workers = (0 until numWorkers).map { i =>
      (s"worker-${i}.example.com", 10000 + i, 20000 + i, 30000 + i, 40000 + i)
    }

    for (partitionId <- 0 until numPartitions) {
      val locations = new util.HashSet[PartitionLocation]()

      val workerIdx = random.nextInt(numWorkers)
      val (host, rpcPort, pushPort, fetchPort, replicatePort) = workers(workerIdx)

      val storageInfo = new StorageInfo(
        StorageInfo.Type.HDD,
        s"/mnt/disk${random.nextInt(12)}",
        false,
        s"/data/celeborn/shuffle/0/$partitionId",
        StorageInfo.LOCAL_DISK_MASK)

      val bitmap = new org.roaringbitmap.RoaringBitmap()
      for (m <- 0 until numMappers) {
        bitmap.add(m)
      }

      if (withReplica) {
        var replicaIdx = random.nextInt(numWorkers)
        while (replicaIdx == workerIdx) {
          replicaIdx = random.nextInt(numWorkers)
        }
        val (rHost, rRpcPort, rPushPort, rFetchPort, rReplicatePort) = workers(replicaIdx)

        val replicaStorageInfo = new StorageInfo(
          StorageInfo.Type.HDD,
          s"/mnt/disk${random.nextInt(12)}",
          false,
          s"/data/celeborn/shuffle/0/$partitionId",
          StorageInfo.LOCAL_DISK_MASK)

        val replica = new PartitionLocation(
          partitionId,
          0,
          rHost,
          rRpcPort,
          rPushPort,
          rFetchPort,
          rReplicatePort,
          PartitionLocation.Mode.REPLICA,
          null,
          replicaStorageInfo,
          bitmap.clone(),
          0,
          numMappers)

        val primary = new PartitionLocation(
          partitionId,
          0,
          host,
          rpcPort,
          pushPort,
          fetchPort,
          replicatePort,
          PartitionLocation.Mode.PRIMARY,
          replica,
          storageInfo,
          bitmap,
          0,
          numMappers)

        replica.setPeer(primary)
        locations.add(primary)
      } else {
        val primary = new PartitionLocation(
          partitionId,
          0,
          host,
          rpcPort,
          pushPort,
          fetchPort,
          replicatePort,
          PartitionLocation.Mode.PRIMARY,
          null,
          storageInfo,
          bitmap,
          0,
          numMappers)
        locations.add(primary)
      }

      fileGroups.put(partitionId, locations)
    }
    fileGroups
  }

  /**
   * Serialize using the OLD format (inline worker info per location).
   */
  private def serializeOldFormat(
      fileGroups: ConcurrentHashMap[Integer, util.Set[PartitionLocation]],
      attempts: Array[Int]): Array[Byte] = {
    val builder = PbGetReducerFileGroupResponse.newBuilder()
      .setStatus(StatusCode.SUCCESS.getValue)

    builder.putAllFileGroups(
      fileGroups.asScala.map { case (partitionId, locs) =>
        (
          partitionId,
          PbFileGroup.newBuilder()
            .addAllLocations(locs.asScala.map(PbSerDeUtils.toPbPartitionLocation).asJava)
            .build())
      }.asJava)

    builder.addAllAttempts(attempts.map(Integer.valueOf).toIterable.asJava)
    builder.build().toByteArray
  }

  private def serializeCompactFormat(
      fileGroups: ConcurrentHashMap[Integer, util.Set[PartitionLocation]],
      attempts: Array[Int],
      includeMapIdBitmap: Boolean = true): (Array[Byte], Int, Int) = {
    val builder = PbGetReducerFileGroupResponse.newBuilder()
      .setStatus(StatusCode.SUCCESS.getValue)

    val workerDict = new util.LinkedHashMap[WorkerInfo, Integer]()
    val mountPointDict = new util.LinkedHashMap[String, Integer]()
    builder.putAllCompactFileGroups(
      fileGroups.asScala.map { case (partitionId, locs) =>
        (
          partitionId,
          PbCompactFileGroup.newBuilder().addAllLocations(locs.asScala.map(loc =>
            PbSerDeUtils.toPbCompactPartitionLocation(
              loc,
              workerDict,
              mountPointDict,
              includeMapIdBitmap))
            .toList.asJava).build())
      }.asJava)
    builder.addAllWorkerInfos(PbSerDeUtils.buildWorkerInfoList(workerDict))
    builder.addAllMountPoints(PbSerDeUtils.buildMountPointList(mountPointDict))

    builder.addAllAttempts(attempts.map(Integer.valueOf).toIterable.asJava)
    val payload = builder.build().toByteArray
    (payload, workerDict.size(), mountPointDict.size())
  }

  // scalastyle:off println
  private def printResult(
      testName: String,
      numWorkers: Int,
      numPartitions: Int,
      numMappers: Int,
      withReplica: Boolean,
      oldSize: Int,
      compactSize: Int,
      uniqueWorkers: Int,
      uniqueMountPoints: Int): Unit = {
    val compressionRatio = (1.0 - compactSize.toDouble / oldSize) * 100
    val locMultiplier = if (withReplica) 2 else 1
    val totalLocations = numPartitions * locMultiplier
    println(s"=== $testName ===")
    println(
      s"  Config:              $numWorkers workers, $numPartitions partitions, $numMappers mappers")
    println(
      s"  Total locations:     $totalLocations${if (withReplica) " (primary + replica)" else ""}")
    println(s"  Unique workers:      $uniqueWorkers")
    println(s"  Unique mountPoints:  $uniqueMountPoints")
    println(s"  Old format size:     $oldSize bytes (${oldSize / 1024 / 1024} MB)")
    println(s"  Compact format size: $compactSize bytes (${compactSize / 1024 / 1024} MB)")
    println(
      s"  Saved:               ${oldSize - compactSize} bytes (${(oldSize - compactSize) / 1024 / 1024} MB)")
    println(s"  Compression ratio:   ${f"$compressionRatio%.1f"}%%")
  }
  // scalastyle:on println

  // ========================================================================
  // Production scenario: 2048 workers, 230k locations (115k partitions + replica)
  // This matches the online log: GetReducerFileGroupResponse size 152043520
  // ========================================================================

  test("production scenario: 2048 workers, 230k locations (115000 partitions with replica)") {
    val numWorkers = 2048
    val numPartitions = 115000 // 115k partitions × 2 (primary+replica) = 230k locations
    val numMappers = 200

    val fileGroups = buildFileGroups(numWorkers, numPartitions, withReplica = true, numMappers)
    val attempts = Array.fill(numMappers)(0)

    val oldBytes = serializeOldFormat(fileGroups, attempts)
    val (compactWithBitmap, uniqueWorkers, uniqueMountPoints) =
      serializeCompactFormat(fileGroups, attempts, includeMapIdBitmap = true)
    val (compactNoBitmap, _, _) =
      serializeCompactFormat(fileGroups, attempts, includeMapIdBitmap = false)

    printResult(
      "Production WITH bitmap (rangeReadFilter=true)",
      numWorkers,
      numPartitions,
      numMappers,
      withReplica = true,
      oldBytes.length,
      compactWithBitmap.length,
      uniqueWorkers,
      uniqueMountPoints)
    printResult(
      "Production NO bitmap (rangeReadFilter=false, DEFAULT)",
      numWorkers,
      numPartitions,
      numMappers,
      withReplica = true,
      oldBytes.length,
      compactNoBitmap.length,
      uniqueWorkers,
      uniqueMountPoints)

    assert(compactNoBitmap.length < compactWithBitmap.length)
    assert(compactWithBitmap.length < oldBytes.length)
  }

  // ========================================================================
  // Comparison: fewer workers = higher dedup ratio = better compression
  // ========================================================================

  test("high dedup: 10 workers, 230k locations (115000 partitions with replica)") {
    val numWorkers = 10
    val numPartitions = 115000
    val numMappers = 200

    val fileGroups = buildFileGroups(numWorkers, numPartitions, withReplica = true, numMappers)
    val attempts = Array.fill(numMappers)(0)

    val oldBytes = serializeOldFormat(fileGroups, attempts)
    val (compactBytes, uniqueWorkers, uniqueMountPoints) =
      serializeCompactFormat(fileGroups, attempts)

    printResult(
      "High dedup: 10 workers, 230k locations",
      numWorkers,
      numPartitions,
      numMappers,
      withReplica = true,
      oldBytes.length,
      compactBytes.length,
      uniqueWorkers,
      uniqueMountPoints)

    assert(compactBytes.length < oldBytes.length)
  }

  // ========================================================================
  // No replica scenario
  // ========================================================================

  test("no replica: 2048 workers, 230k locations (230000 partitions, no replica)") {
    val numWorkers = 2048
    val numPartitions = 230000
    val numMappers = 200

    val fileGroups = buildFileGroups(numWorkers, numPartitions, withReplica = false, numMappers)
    val attempts = Array.fill(numMappers)(0)

    val oldBytes = serializeOldFormat(fileGroups, attempts)
    val (compactBytes, uniqueWorkers, uniqueMountPoints) =
      serializeCompactFormat(fileGroups, attempts)

    printResult(
      "No replica: 2048 workers, 230k locations",
      numWorkers,
      numPartitions,
      numMappers,
      withReplica = false,
      oldBytes.length,
      compactBytes.length,
      uniqueWorkers,
      uniqueMountPoints)

    assert(compactBytes.length < oldBytes.length)
  }

  // ========================================================================
  // Correctness: roundtrip serialization/deserialization
  // ========================================================================

  test("correctness: compact format roundtrip preserves data") {
    val numWorkers = 50
    val numPartitions = 100
    val numMappers = 50

    val fileGroups = buildFileGroups(numWorkers, numPartitions, withReplica = true, numMappers)
    val attempts = Array.fill(numMappers)(0)

    val response = GetReducerFileGroupResponse(
      StatusCode.SUCCESS,
      fileGroups,
      attempts)

    val transportMessage = ControlMessages.toTransportMessage(response)
    val deserialized = ControlMessages.fromTransportMessage(transportMessage)

    deserialized match {
      case GetReducerFileGroupResponse(
            status,
            restoredFileGroups,
            restoredAttempts,
            _,
            _,
            _,
            _,
            _) =>
        assert(status == StatusCode.SUCCESS)
        assert(restoredAttempts.sameElements(attempts))
        assert(restoredFileGroups.size() == fileGroups.size())

        fileGroups.asScala.foreach { case (partitionId, originalLocs) =>
          val restoredLocs = restoredFileGroups.get(partitionId)
          assert(restoredLocs != null, s"Partition $partitionId missing in restored data")
          assert(
            restoredLocs.size() == originalLocs.size(),
            s"Partition $partitionId: expected ${originalLocs.size()} locations, got ${restoredLocs.size()}")

          val origMap = originalLocs.asScala.map(l => l.getId -> l).toMap
          val restoredMap = restoredLocs.asScala.map(l => l.getId -> l).toMap

          origMap.foreach { case (id, orig) =>
            val restored = restoredMap(id)
            assert(restored.getHost == orig.getHost, s"host mismatch for partition $partitionId")
            assert(
              restored.getRpcPort == orig.getRpcPort,
              s"rpcPort mismatch for partition $partitionId")
            assert(
              restored.getPushPort == orig.getPushPort,
              s"pushPort mismatch for partition $partitionId")
            assert(
              restored.getFetchPort == orig.getFetchPort,
              s"fetchPort mismatch for partition $partitionId")
            assert(
              restored.getReplicatePort == orig.getReplicatePort,
              s"replicatePort mismatch for partition $partitionId")
            assert(restored.getMode == orig.getMode, s"mode mismatch for partition $partitionId")

            // Verify StorageInfo (mountPoint + filePath) roundtrip
            val origSI = orig.getStorageInfo
            val restoredSI = restored.getStorageInfo
            assert(
              restoredSI.getMountPoint == origSI.getMountPoint,
              s"mountPoint mismatch for partition $partitionId: " +
                s"expected '${origSI.getMountPoint}', got '${restoredSI.getMountPoint}'")
            assert(
              restoredSI.getFilePath == origSI.getFilePath,
              s"filePath mismatch for partition $partitionId: " +
                s"expected '${origSI.getFilePath}', got '${restoredSI.getFilePath}'")
            assert(
              restoredSI.getType == origSI.getType,
              s"storageType mismatch for partition $partitionId")

            assert(restored.hasPeer == orig.hasPeer, s"hasPeer mismatch for partition $partitionId")
            if (orig.hasPeer) {
              val origPeer = orig.getPeer
              val restoredPeer = restored.getPeer
              assert(restoredPeer.getHost == origPeer.getHost)
              assert(restoredPeer.getRpcPort == origPeer.getRpcPort)
              assert(restoredPeer.getPushPort == origPeer.getPushPort)
              assert(restoredPeer.getFetchPort == origPeer.getFetchPort)
              assert(restoredPeer.getReplicatePort == origPeer.getReplicatePort)
              // Verify peer StorageInfo
              val origPeerSI = origPeer.getStorageInfo
              val restoredPeerSI = restoredPeer.getStorageInfo
              assert(
                restoredPeerSI.getMountPoint == origPeerSI.getMountPoint,
                s"peer mountPoint mismatch for partition $partitionId")
              assert(
                restoredPeerSI.getFilePath == origPeerSI.getFilePath,
                s"peer filePath mismatch for partition $partitionId")
            }
          }
        }
      case other =>
        fail(s"Unexpected deserialized type: ${other.getClass.getName}")
    }
  }

  test("correctness: compact format roundtrip without bitmap (rangeReadFilter=false)") {
    val numWorkers = 50
    val numPartitions = 100
    val numMappers = 50

    val fileGroups = buildFileGroups(numWorkers, numPartitions, withReplica = true, numMappers)
    val attempts = Array.fill(numMappers)(0)

    val response = GetReducerFileGroupResponse(
      StatusCode.SUCCESS,
      fileGroups,
      attempts,
      includeMapIdBitmap = false)

    val transportMessage = ControlMessages.toTransportMessage(response)
    val deserialized = ControlMessages.fromTransportMessage(transportMessage)

    deserialized match {
      case GetReducerFileGroupResponse(
            status,
            restoredFileGroups,
            restoredAttempts,
            _,
            _,
            _,
            _,
            _) =>
        assert(status == StatusCode.SUCCESS)
        assert(restoredAttempts.sameElements(attempts))
        assert(restoredFileGroups.size() == fileGroups.size())

        // Verify worker/storage info is preserved, bitmap should be empty
        fileGroups.asScala.foreach { case (partitionId, originalLocs) =>
          val restoredLocs = restoredFileGroups.get(partitionId)
          assert(restoredLocs != null)
          val origMap = originalLocs.asScala.map(l => l.getId -> l).toMap
          val restoredMap = restoredLocs.asScala.map(l => l.getId -> l).toMap

          origMap.foreach { case (id, orig) =>
            val restored = restoredMap(id)
            assert(restored.getHost == orig.getHost)
            assert(restored.getRpcPort == orig.getRpcPort)
            assert(restored.getStorageInfo.getMountPoint == orig.getStorageInfo.getMountPoint)
            assert(restored.getStorageInfo.getFilePath == orig.getStorageInfo.getFilePath)
            // Bitmap should be empty when includeMapIdBitmap=false
            assert(
              restored.getMapIdBitMap.isEmpty,
              s"mapIdBitmap should be empty when includeMapIdBitmap=false")
          }
        }
      case other =>
        fail(s"Unexpected deserialized type: ${other.getClass.getName}")
    }
  }

  // ========================================================================
  // Per-field size breakdown: identify exactly where memory is consumed
  // ========================================================================

  test("field-level size breakdown analysis") {
    val numWorkers = 2048
    val numPartitions = 115000
    val numMappers = 200

    val fileGroups = buildFileGroups(numWorkers, numPartitions, withReplica = true, numMappers)

    // Measure individual field sizes by serializing single-field protos
    var totalMapIdBitmapBytes = 0L
    var totalStorageInfoBytes = 0L
    var totalCompactStorageInfoBytes = 0L
    var totalWorkerInfoOldBytes = 0L // host + 4 ports in old format
    var totalLocationCount = 0L
    var sampleBitmapSize = 0
    var sampleStorageInfoSize = 0
    var sampleCompactStorageInfoSize = 0
    var sampleOldWorkerSize = 0
    val mountPointDict = new util.LinkedHashMap[String, Integer]()

    fileGroups.asScala.foreach { case (_, locs) =>
      locs.asScala.foreach { loc =>
        totalLocationCount += 1
        // mapIdBitmap size
        val bitmapBytes = Utils.roaringBitmapToByteString(loc.getMapIdBitMap)
        totalMapIdBitmapBytes += bitmapBytes.size()
        if (sampleBitmapSize == 0) sampleBitmapSize = bitmapBytes.size()

        // old StorageInfo (full PbStorageInfo) size
        val oldSI = StorageInfo.toPb(loc.getStorageInfo).toByteArray
        totalStorageInfoBytes += oldSI.length
        if (sampleStorageInfoSize == 0) sampleStorageInfoSize = oldSI.length

        // worker info in old format: host string + 4 int ports
        val workerPb = PbSerDeUtils.toPbPartitionLocation(loc).toByteArray
        val noWorkerPb = PbSerDeUtils.toPbPartitionLocation(
          new PartitionLocation(
            loc.getId,
            loc.getEpoch,
            "",
            0,
            0,
            0,
            0,
            loc.getMode,
            null,
            loc.getStorageInfo,
            loc.getMapIdBitMap,
            loc.getSplitStart,
            loc.getSplitEnd)).toByteArray
        totalWorkerInfoOldBytes += (workerPb.length - noWorkerPb.length)
        if (sampleOldWorkerSize == 0) sampleOldWorkerSize = workerPb.length - noWorkerPb.length

        // peer
        if (loc.hasPeer) {
          totalLocationCount += 1
          val peerBitmap = Utils.roaringBitmapToByteString(loc.getPeer.getMapIdBitMap)
          totalMapIdBitmapBytes += peerBitmap.size()
          val peerOldSI = StorageInfo.toPb(loc.getPeer.getStorageInfo).toByteArray
          totalStorageInfoBytes += peerOldSI.length
        }
      }
    }

    val totalBytes = 104180509 // compact format from previous test

    // scalastyle:off println
    println()
    println("=" * 70)
    println("  FIELD-LEVEL SIZE BREAKDOWN (230k locations, compact format)")
    println("=" * 70)
    println(f"  Total locations (primary+replica): $totalLocationCount%,d")
    println()
    println("  --- Per-location sample sizes ---")
    println(f"  mapIdBitmap (200 mappers):     $sampleBitmapSize bytes")
    println(f"  old PbStorageInfo:              $sampleStorageInfoSize bytes")
    println(f"  old worker info (host+ports):   $sampleOldWorkerSize bytes")
    println()
    println("  --- Aggregated sizes across all locations ---")
    println(f"  mapIdBitmap total:              ${totalMapIdBitmapBytes / 1024 / 1024} MB ($totalMapIdBitmapBytes%,d bytes)")
    println(f"  old PbStorageInfo total:        ${totalStorageInfoBytes / 1024 / 1024} MB ($totalStorageInfoBytes%,d bytes)")
    println(f"  old worker info total:          ${totalWorkerInfoOldBytes / 1024 / 1024} MB ($totalWorkerInfoOldBytes%,d bytes)")
    println()
    println("  --- Percentage of compact total (~99 MB) ---")
    println(
      f"  mapIdBitmap:                    ${totalMapIdBitmapBytes * 100.0 / totalBytes}%.1f%%")
    println(
      f"  old PbStorageInfo (for ref):    ${totalStorageInfoBytes * 100.0 / totalBytes}%.1f%%")
    println(
      f"  old worker info (for ref):      ${totalWorkerInfoOldBytes * 100.0 / totalBytes}%.1f%%")
    println()
    println("  >>> mapIdBitmap is the DOMINANT cost <<<")
    println("=" * 70)
    // scalastyle:on println
  }
}
