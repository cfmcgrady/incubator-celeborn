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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.protocol._
import org.apache.celeborn.common.protocol.message.ControlMessages._
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.rpc._
import org.apache.celeborn.common.util.PbSerDeUtils

/**
 * A standalone test LifecycleManager server for integration testing with Rust client.
 *
 * This server simulates the LifecycleManager's RPC handling to test the Rust
 * ExecutorShuffleClient's protocol implementation.
 *
 * Usage:
 * {{{
 *   // Start server programmatically
 *   val server = new TestLifecycleManagerServer(conf, "127.0.0.1", 9098)
 *   server.start()
 *   // ... run tests ...
 *   server.stop()
 *
 *   // Or run standalone
 *   TestLifecycleManagerServer.main(Array("9098"))
 * }}}
 *
 * Default port: 9098
 */
class TestLifecycleManagerServer(conf: CelebornConf, host: String, port: Int)
    extends RpcEndpoint with Logging {

  private var _rpcEnv: RpcEnv = _
  private val shuffleIdCounter = new AtomicInteger(0)
  private val registeredShuffles = new ConcurrentHashMap[Int, ShuffleInfo]()

  /** Shuffle information for tracking registered shuffles. */
  private case class ShuffleInfo(
      shuffleId: Int,
      numMappers: Int,
      numPartitions: Int,
      partitionLocations: java.util.List[PartitionLocation],
      mapperEnded: ConcurrentHashMap[Int, Boolean])

  def this(conf: CelebornConf, host: String) = this(conf, host, 9098)

  override val rpcEnv: RpcEnv = {
    if (_rpcEnv == null) {
      _rpcEnv = RpcEnv.create(
        RpcNameConstants.LIFECYCLE_MANAGER_SYS,
        host,
        host,
        port,
        conf,
        0) // numUsableCores
    }
    _rpcEnv
  }

  override def onStart(): Unit = {
    logInfo(s"TestLifecycleManagerServer started on $host:$port")
  }

  override def onStop(): Unit = {
    logInfo("TestLifecycleManagerServer stopped")
  }

  override def receive: PartialFunction[Any, Unit] = {
    case msg =>
      logDebug(s"Received one-way message: ${msg.getClass.getSimpleName}")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    // PbRegisterShuffle - protobuf message for RegisterShuffle
    case request: PbRegisterShuffle =>
      handleRegisterShuffle(request, context)

    // MapperEnd - case class
    case request: MapperEnd =>
      handleMapperEnd(request, context)

    // GetReducerFileGroup - case class
    case request: GetReducerFileGroup =>
      handleGetReducerFileGroup(request, context)

    // PbRevive - protobuf message for Revive
    case request: PbRevive =>
      handleRevive(request, context)

    // PbPartitionSplit - protobuf message for PartitionSplit
    case request: PbPartitionSplit =>
      handlePartitionSplit(request, context)

    // PbGetShuffleId - protobuf message for GetShuffleId
    case request: PbGetShuffleId =>
      handleGetShuffleId(request, context)

    // PbReportShuffleFetchFailure - protobuf message for ReportShuffleFetchFailure
    case request: PbReportShuffleFetchFailure =>
      handleReportShuffleFetchFailure(request, context)

    case msg =>
      logWarning(s"Unknown message type: ${msg.getClass.getName}")
      context.sendFailure(
        new UnsupportedOperationException(s"Unknown message type: ${msg.getClass.getName}"))
  }

  private def handleRegisterShuffle(
      request: PbRegisterShuffle,
      context: RpcCallContext): Unit = {
    val shuffleId = request.getShuffleId
    val numMappers = request.getNumMappers
    val numPartitions = request.getNumPartitions

    logInfo(s"RegisterShuffle: shuffleId=$shuffleId, numMappers=$numMappers, " +
      s"numPartitions=$numPartitions")

    // Create shuffle info
    val partitionLocations = new java.util.ArrayList[PartitionLocation]()

    // Generate partition locations
    for (partitionId <- 0 until numPartitions) {
      val location = createPartitionLocation(shuffleId, partitionId)
      partitionLocations.add(location)
    }

    val shuffleInfo = ShuffleInfo(
      shuffleId,
      numMappers,
      numPartitions,
      partitionLocations,
      new ConcurrentHashMap[Int, Boolean]())

    registeredShuffles.put(shuffleId, shuffleInfo)

    // Build response using protobuf
    val pbLocations = partitionLocations.asScala.map(PbSerDeUtils.toPbPartitionLocation).asJava
    val response = PbRegisterShuffleResponse.newBuilder()
      .setStatus(StatusCode.SUCCESS.getValue)
      .addAllPartitionLocations(pbLocations)
      .build()

    context.reply(response)
    logInfo(s"RegisterShuffle response sent for shuffleId=$shuffleId")
  }

  private def handleMapperEnd(request: MapperEnd, context: RpcCallContext): Unit = {
    val shuffleId = request.shuffleId
    val mapId = request.mapId
    val attemptId = request.attemptId

    logInfo(s"MapperEnd: shuffleId=$shuffleId, mapId=$mapId, attemptId=$attemptId")

    val shuffleInfo = registeredShuffles.get(shuffleId)
    if (shuffleInfo != null) {
      shuffleInfo.mapperEnded.put(mapId, true)
    }

    val response = MapperEndResponse(StatusCode.SUCCESS)
    context.reply(response)
    logInfo(s"MapperEnd response sent for shuffleId=$shuffleId, mapId=$mapId")
  }

  private def handleGetReducerFileGroup(
      request: GetReducerFileGroup,
      context: RpcCallContext): Unit = {
    val shuffleId = request.shuffleId

    logInfo(s"GetReducerFileGroup: shuffleId=$shuffleId")

    val shuffleInfo = registeredShuffles.get(shuffleId)

    val fileGroups = new java.util.HashMap[Integer, java.util.Set[PartitionLocation]]()
    var attempts: Array[Int] = Array.empty
    val partitionIds: java.util.Set[Integer] = new java.util.HashSet[Integer]()

    if (shuffleInfo != null) {
      // Build file groups from partition locations
      shuffleInfo.partitionLocations.asScala.foreach { loc =>
        val locations = fileGroups.computeIfAbsent(
          loc.getId,
          _ => new java.util.HashSet[PartitionLocation]())
        locations.add(loc)
        partitionIds.add(loc.getId)
      }

      // Build attempts array
      attempts = (0 until shuffleInfo.numMappers).map(_ => 0).toArray
    }

    val response = GetReducerFileGroupResponse(
      StatusCode.SUCCESS,
      fileGroups.asScala.map { case (k, v) => (k: Integer, v) }.asJava,
      attempts,
      partitionIds,
      new java.util.HashMap[String, java.util.Set[
        org.apache.celeborn.common.write.PushFailedBatch]]())

    context.reply(response)
    logInfo(s"GetReducerFileGroup response sent for shuffleId=$shuffleId")
  }

  private def handleRevive(request: PbRevive, context: RpcCallContext): Unit = {
    val shuffleId = request.getShuffleId

    logInfo(s"Revive: shuffleId=$shuffleId, partitions=${request.getPartitionInfoCount}")

    // Build response with new partition locations
    val partitionInfos = new java.util.ArrayList[PbChangeLocationPartitionInfo]()

    request.getPartitionInfoList.asScala.foreach { info =>
      val partitionId = info.getPartitionId
      val epoch = info.getEpoch

      // Create new location with incremented epoch
      val newLocation = createPartitionLocation(shuffleId, partitionId, epoch + 1)

      val partitionInfo = PbChangeLocationPartitionInfo.newBuilder()
        .setPartitionId(partitionId)
        .setStatus(StatusCode.SUCCESS.getValue)
        .setPartition(PbSerDeUtils.toPbPartitionLocation(newLocation))
        .build()

      partitionInfos.add(partitionInfo)
    }

    val response = PbChangeLocationResponse.newBuilder()
      .addAllPartitionInfo(partitionInfos)
      .build()

    context.reply(response)
    logInfo(s"Revive response sent for shuffleId=$shuffleId")
  }

  private def handlePartitionSplit(request: PbPartitionSplit, context: RpcCallContext): Unit = {
    val shuffleId = request.getShuffleId
    val partitionId = request.getPartitionId
    val epoch = request.getEpoch

    logInfo(s"PartitionSplit: shuffleId=$shuffleId, partitionId=$partitionId, epoch=$epoch")

    // Create new location with incremented epoch
    val newLocation = createPartitionLocation(shuffleId, partitionId, epoch + 1)

    val partitionInfo = PbChangeLocationPartitionInfo.newBuilder()
      .setPartitionId(partitionId)
      .setStatus(StatusCode.SUCCESS.getValue)
      .setPartition(PbSerDeUtils.toPbPartitionLocation(newLocation))
      .build()

    val response = PbChangeLocationResponse.newBuilder()
      .addPartitionInfo(partitionInfo)
      .build()

    context.reply(response)
    logInfo(s"PartitionSplit response sent for shuffleId=$shuffleId, partitionId=$partitionId")
  }

  private def handleGetShuffleId(request: PbGetShuffleId, context: RpcCallContext): Unit = {
    val appShuffleId = request.getAppShuffleId
    val appShuffleIdentifier = request.getAppShuffleIdentifier
    val isWriter = request.getIsShuffleWriter

    logInfo(s"GetShuffleId: appShuffleId=$appShuffleId, identifier=$appShuffleIdentifier, " +
      s"isWriter=$isWriter")

    // For testing, just return the appShuffleId as the shuffleId
    val response = PbGetShuffleIdResponse.newBuilder()
      .setShuffleId(appShuffleId)
      .build()

    context.reply(response)
    logInfo(s"GetShuffleId response sent: shuffleId=$appShuffleId")
  }

  private def handleReportShuffleFetchFailure(
      request: PbReportShuffleFetchFailure,
      context: RpcCallContext): Unit = {
    val appShuffleId = request.getAppShuffleId
    val shuffleId = request.getShuffleId

    logInfo(s"ReportShuffleFetchFailure: appShuffleId=$appShuffleId, shuffleId=$shuffleId")

    val response = PbReportShuffleFetchFailureResponse.newBuilder()
      .setSuccess(true)
      .build()

    context.reply(response)
    logInfo("ReportShuffleFetchFailure response sent")
  }

  private def createPartitionLocation(
      shuffleId: Int,
      partitionId: Int,
      epoch: Int = 0): PartitionLocation = {
    val storageInfo = new StorageInfo(
      StorageInfo.Type.MEMORY,
      "/tmp",
      false,
      s"/tmp/shuffle-$shuffleId-$partitionId",
      StorageInfo.ALL_TYPES_AVAILABLE_MASK)

    new PartitionLocation(
      partitionId,
      epoch,
      "127.0.0.1",
      9099, // rpcPort
      9100, // pushPort
      9101, // fetchPort
      9102, // replicatePort
      PartitionLocation.Mode.PRIMARY,
      null, // peer
      storageInfo,
      null) // mapIdBitMap
  }

  def start(): Unit = {
    rpcEnv.setupEndpoint(RpcNameConstants.LIFECYCLE_MANAGER_EP, this)
    logInfo(s"TestLifecycleManagerServer listening on $host:$port")
  }

  override def stop(): Unit = {
    rpcEnv.shutdown()
    rpcEnv.awaitTermination()
    logInfo("TestLifecycleManagerServer stopped")
  }

  def getPort: Int = port

  def getHost: String = host
}

/**
 * Main entry point for running the test server standalone.
 */
object TestLifecycleManagerServer extends Logging {
  def main(args: Array[String]): Unit = {
    val port = if (args.length > 0) {
      try {
        args(0).toInt
      } catch {
        case _: NumberFormatException =>
          System.err.println(s"Invalid port number: ${args(0)}")
          System.exit(1)
          9098
      }
    } else {
      9098
    }

    val conf = new CelebornConf()
    val host = "0.0.0.0"

    val server = new TestLifecycleManagerServer(conf, host, port)
    server.start()

    // Add shutdown hook
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      logInfo("Shutting down TestLifecycleManagerServer...")
      server.stop()
    }))

    logInfo("TestLifecycleManagerServer is running. Press Ctrl+C to stop.")

    // Keep the main thread alive
    try {
      Thread.currentThread().join()
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
    }
  }
}
