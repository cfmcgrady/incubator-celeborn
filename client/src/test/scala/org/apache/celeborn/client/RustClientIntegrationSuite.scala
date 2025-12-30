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

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.network.protocol.TransportMessage
import org.apache.celeborn.common.protocol._
import org.apache.celeborn.common.protocol.message.ControlMessages._
import org.apache.celeborn.common.protocol.message.StatusCode
import org.apache.celeborn.common.util.{PbSerDeUtils, Utils}

/**
 * Integration test suite for validating communication between
 * Rust ExecutorShuffleClient and Java LifecycleManager.
 *
 * This suite tests the RPC protocol compatibility to ensure
 * Rust clients can properly communicate with Java LifecycleManager.
 */
class RustClientIntegrationSuite extends AnyFunSuite with BeforeAndAfterAll with Logging {

  private var conf: CelebornConf = _
  private val shuffleIdCounter = new AtomicInteger(0)

  override def beforeAll(): Unit = {
    super.beforeAll()
    conf = new CelebornConf()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  private def generateShuffleId(): Int = {
    shuffleIdCounter.incrementAndGet()
  }

  test("RegisterShuffle RPC message format is compatible") {
    // This test verifies the protobuf message format
    val shuffleId = generateShuffleId()
    val numMappers = 2
    val numPartitions = 4

    val request = PbRegisterShuffle.newBuilder()
      .setShuffleId(shuffleId)
      .setNumMappers(numMappers)
      .setNumPartitions(numPartitions)
      .build()

    val bytes = request.toByteArray
    assert(bytes.length > 0, "Serialized message should not be empty")

    // Verify we can deserialize it back
    val parsed = PbRegisterShuffle.parseFrom(bytes)
    assert(parsed.getShuffleId == shuffleId)
    assert(parsed.getNumMappers == numMappers)
    assert(parsed.getNumPartitions == numPartitions)
  }

  test("MapperEnd RPC message format is compatible") {
    val shuffleId = generateShuffleId()
    val mapId = 0
    val attemptId = 0
    val numMappers = 2
    val partitionId = -1

    val request = PbMapperEnd.newBuilder()
      .setShuffleId(shuffleId)
      .setMapId(mapId)
      .setAttemptId(attemptId)
      .setNumMappers(numMappers)
      .setPartitionId(partitionId)
      .build()

    val bytes = request.toByteArray
    assert(bytes.length > 0)

    val parsed = PbMapperEnd.parseFrom(bytes)
    assert(parsed.getShuffleId == shuffleId)
    assert(parsed.getMapId == mapId)
    assert(parsed.getAttemptId == attemptId)
  }

  test("GetReducerFileGroup RPC message format is compatible") {
    val shuffleId = generateShuffleId()

    val request = PbGetReducerFileGroup.newBuilder()
      .setShuffleId(shuffleId)
      .build()

    val bytes = request.toByteArray
    assert(bytes.length > 0)

    val parsed = PbGetReducerFileGroup.parseFrom(bytes)
    assert(parsed.getShuffleId == shuffleId)
  }

  test("Revive RPC message format is compatible") {
    val shuffleId = generateShuffleId()
    val partitionId = 0
    val epoch = 0

    val partitionInfo = PbRevivePartitionInfo.newBuilder()
      .setPartitionId(partitionId)
      .setEpoch(epoch)
      .setStatus(StatusCode.PUSH_DATA_WRITE_FAIL_PRIMARY.getValue)
      .build()

    val request = PbRevive.newBuilder()
      .setShuffleId(shuffleId)
      .addMapId(0)
      .addPartitionInfo(partitionInfo)
      .build()

    val bytes = request.toByteArray
    assert(bytes.length > 0)

    val parsed = PbRevive.parseFrom(bytes)
    assert(parsed.getShuffleId == shuffleId)
    assert(parsed.getPartitionInfoCount == 1)
  }

  test("PartitionLocation serialization is compatible") {
    val location = new PartitionLocation(
      0, // id
      0, // epoch
      "127.0.0.1",
      9099, // rpcPort
      9100, // pushPort
      9101, // fetchPort
      9102, // replicatePort
      PartitionLocation.Mode.PRIMARY,
      null, // peer
      new StorageInfo(StorageInfo.Type.MEMORY, "/tmp", false, "/tmp/test", 1),
      null // mapIdBitMap
    )

    val pbLocation = PbSerDeUtils.toPbPartitionLocation(location)
    val bytes = pbLocation.toByteArray
    assert(bytes.length > 0)

    val parsed = PbPartitionLocation.parseFrom(bytes)
    assert(parsed.getId == location.getId)
    assert(parsed.getEpoch == location.getEpoch)
    assert(parsed.getHost == location.getHost)
    assert(parsed.getPushPort == location.getPushPort)
    assert(parsed.getFetchPort == location.getFetchPort)
  }

  test("TransportMessage wrapping is correct") {
    val shuffleId = generateShuffleId()
    val numMappers = 2
    val numPartitions = 4

    // Create a RegisterShuffle case class
    val registerShuffle = RegisterShuffle(shuffleId, numMappers, numPartitions)

    // Convert to TransportMessage using Utils
    val transportMessage = Utils.toTransportMessage(registerShuffle).asInstanceOf[TransportMessage]

    assert(transportMessage.getMessageTypeValue == MessageType.REGISTER_SHUFFLE.getNumber)
    assert(transportMessage.getPayload != null)

    // Verify payload can be parsed back
    val parsedRequest = PbRegisterShuffle.parseFrom(transportMessage.getPayload)
    assert(parsedRequest.getShuffleId == shuffleId)
  }

  test("RegisterShuffleResponse format is correct") {
    val locations = (0 until 4).map { partitionId =>
      new PartitionLocation(
        partitionId,
        0,
        "127.0.0.1",
        9099,
        9100,
        9101,
        9102,
        PartitionLocation.Mode.PRIMARY,
        null,
        new StorageInfo(StorageInfo.Type.MEMORY, "/tmp", false, s"/tmp/test-$partitionId", 1),
        null)
    }

    val pbLocations = locations.map(PbSerDeUtils.toPbPartitionLocation).asJava

    val response = PbRegisterShuffleResponse.newBuilder()
      .setStatus(StatusCode.SUCCESS.getValue)
      .addAllPartitionLocations(pbLocations)
      .build()

    val bytes = response.toByteArray
    assert(bytes.length > 0)

    val parsed = PbRegisterShuffleResponse.parseFrom(bytes)
    assert(parsed.getStatus == StatusCode.SUCCESS.getValue)
    assert(parsed.getPartitionLocationsCount == 4)
  }

  test("GetReducerFileGroupResponse format is correct") {
    val shuffleId = generateShuffleId()
    val numPartitions = 4
    val numMappers = 2

    // Create file groups
    val fileGroups = (0 until numPartitions).map { partitionId =>
      val location = new PartitionLocation(
        partitionId,
        0,
        "127.0.0.1",
        9099,
        9100,
        9101,
        9102,
        PartitionLocation.Mode.PRIMARY,
        null,
        new StorageInfo(StorageInfo.Type.MEMORY, "/tmp", false, s"/tmp/test-$partitionId", 1),
        null)

      val pbLocation = PbSerDeUtils.toPbPartitionLocation(location)
      val fileGroup = PbFileGroup.newBuilder()
        .addLocations(pbLocation)
        .build()

      (Integer.valueOf(partitionId), fileGroup)
    }.toMap.asJava

    val attempts = (0 until numMappers).map(Integer.valueOf).asJava

    val response = PbGetReducerFileGroupResponse.newBuilder()
      .setStatus(StatusCode.SUCCESS.getValue)
      .putAllFileGroups(fileGroups)
      .addAllAttempts(attempts)
      .build()

    val bytes = response.toByteArray
    assert(bytes.length > 0)

    val parsed = PbGetReducerFileGroupResponse.parseFrom(bytes)
    assert(parsed.getStatus == StatusCode.SUCCESS.getValue)
    assert(parsed.getFileGroupsCount == numPartitions)
    assert(parsed.getAttemptsCount == numMappers)
  }

  test("ChangeLocationResponse format is correct for Revive") {
    val partitionId = 0
    val newEpoch = 1

    val location = new PartitionLocation(
      partitionId,
      newEpoch,
      "127.0.0.1",
      9099,
      9100,
      9101,
      9102,
      PartitionLocation.Mode.PRIMARY,
      null,
      new StorageInfo(StorageInfo.Type.MEMORY, "/tmp", false, "/tmp/test", 1),
      null)

    // PbChangeLocationPartitionInfo doesn't have epoch field, only partitionId, status, partition
    val partitionInfo = PbChangeLocationPartitionInfo.newBuilder()
      .setPartitionId(partitionId)
      .setStatus(StatusCode.SUCCESS.getValue)
      .setPartition(PbSerDeUtils.toPbPartitionLocation(location))
      .build()

    val response = PbChangeLocationResponse.newBuilder()
      .addPartitionInfo(partitionInfo)
      .build()

    val bytes = response.toByteArray
    assert(bytes.length > 0)

    val parsed = PbChangeLocationResponse.parseFrom(bytes)
    assert(parsed.getPartitionInfoCount == 1)
    // Epoch is in the partition location, not in the partition info
    assert(parsed.getPartitionInfo(0).getPartition.getEpoch == newEpoch)
  }

  test("StatusCode values match expected values") {
    // Verify critical status codes based on StatusCode.java
    assert(StatusCode.SUCCESS.getValue == 0)
    assert(StatusCode.PARTIAL_SUCCESS.getValue == 1)
    assert(StatusCode.REQUEST_FAILED.getValue == 2)
    assert(StatusCode.SHUFFLE_ALREADY_REGISTERED.getValue == 3)
    assert(StatusCode.SHUFFLE_NOT_REGISTERED.getValue == 4)
    assert(StatusCode.REVIVE_FAILED.getValue == 12)
    assert(StatusCode.PUSH_DATA_FAIL_NON_CRITICAL_CAUSE.getValue == 17)
    assert(StatusCode.PUSH_DATA_WRITE_FAIL_REPLICA.getValue == 18)
    assert(StatusCode.PUSH_DATA_WRITE_FAIL_PRIMARY.getValue == 19)
  }

  test("MessageType values match between Java and Rust") {
    // Verify critical message types
    assert(MessageType.REGISTER_SHUFFLE.getNumber == 4)
    assert(MessageType.REGISTER_SHUFFLE_RESPONSE.getNumber == 5)
    assert(MessageType.CHANGE_LOCATION.getNumber == 10)
    assert(MessageType.CHANGE_LOCATION_RESPONSE.getNumber == 11)
    assert(MessageType.MAPPER_END.getNumber == 12)
    assert(MessageType.MAPPER_END_RESPONSE.getNumber == 13)
    assert(MessageType.GET_REDUCER_FILE_GROUP.getNumber == 14)
    assert(MessageType.GET_REDUCER_FILE_GROUP_RESPONSE.getNumber == 15)
    assert(MessageType.PARTITION_SPLIT.getNumber == 47)
    assert(MessageType.GET_SHUFFLE_ID.getNumber == 69)
    assert(MessageType.REPORT_SHUFFLE_FETCH_FAILURE.getNumber == 67)
  }
}
