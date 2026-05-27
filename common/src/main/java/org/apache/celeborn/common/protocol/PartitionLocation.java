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

package org.apache.celeborn.common.protocol;

import java.io.Serializable;

import org.roaringbitmap.RoaringBitmap;

import org.apache.celeborn.common.meta.WorkerInfo;

public class PartitionLocation implements Serializable {
  public enum Mode {
    PRIMARY(0),
    REPLICA(1);

    private final byte mode;

    Mode(int id) {
      assert id < 128 : "Cannot have more than 128 message types";
      this.mode = (byte) id;
    }

    public byte mode() {
      return mode;
    }
  }

  public static PartitionLocation.Mode getMode(byte mode) {
    if (mode == 0) {
      return Mode.PRIMARY;
    } else {
      return Mode.REPLICA;
    }
  }

  public static String getFileName(String uniqueId, Mode mode) {
    return uniqueId + "-" + mode.mode();
  }

  private int id;
  private int epoch;

  private String host;
  private short rpcPort;
  private short pushPort;
  private short fetchPort;
  private short replicatePort;
  private byte mode;

  private PartitionLocation peer;
  private StorageInfo storageInfo;
  private RoaringBitmap mapIdBitMap;
  private int splitStart;
  private int splitEnd;

  private transient PartitionLocation parent;

  public PartitionLocation(PartitionLocation loc) {
    this.id = loc.id;
    this.epoch = loc.epoch;
    this.host = loc.host;
    this.rpcPort = loc.rpcPort;
    this.pushPort = loc.pushPort;
    this.fetchPort = loc.fetchPort;
    this.replicatePort = loc.replicatePort;
    this.mode = loc.mode;
    this.peer = loc.peer;
    this.storageInfo = loc.storageInfo;
    this.mapIdBitMap = loc.mapIdBitMap;
    this.splitStart = loc.splitStart;
    this.splitEnd = loc.splitEnd;
  }

  public PartitionLocation(
      int id,
      int epoch,
      String host,
      int rpcPort,
      int pushPort,
      int fetchPort,
      int replicatePort,
      Mode mode) {
    this(
        id,
        epoch,
        host,
        rpcPort,
        pushPort,
        fetchPort,
        replicatePort,
        mode,
        null,
        new StorageInfo(),
        new RoaringBitmap());
  }

  public PartitionLocation(
      int id,
      int epoch,
      String host,
      int rpcPort,
      int pushPort,
      int fetchPort,
      int replicatePort,
      Mode mode,
      PartitionLocation peer) {
    this(
        id,
        epoch,
        host,
        rpcPort,
        pushPort,
        fetchPort,
        replicatePort,
        mode,
        peer,
        new StorageInfo(),
        new RoaringBitmap());
  }

  public PartitionLocation(
      int id,
      int epoch,
      String host,
      int rpcPort,
      int pushPort,
      int fetchPort,
      int replicatePort,
      Mode mode,
      PartitionLocation peer,
      StorageInfo hint,
      RoaringBitmap mapIdBitMap) {
    this(
        id,
        epoch,
        host,
        rpcPort,
        pushPort,
        fetchPort,
        replicatePort,
        mode,
        peer,
        hint,
        mapIdBitMap,
        -1,
        -1);
  }

  public PartitionLocation(
      int id,
      int epoch,
      String host,
      int rpcPort,
      int pushPort,
      int fetchPort,
      int replicatePort,
      Mode mode,
      PartitionLocation peer,
      StorageInfo hint,
      RoaringBitmap mapIdBitMap,
      int splitStart,
      int splitEnd) {
    this.id = id;
    this.epoch = epoch;

    this.host = (host != null) ? host.intern() : null;
    this.rpcPort = (short) rpcPort;
    this.pushPort = (short) pushPort;
    this.fetchPort = (short) fetchPort;
    this.replicatePort = (short) replicatePort;
    this.mode = mode.mode();

    this.peer = peer;
    this.storageInfo = hint;
    this.mapIdBitMap = mapIdBitMap;
    this.splitStart = splitStart;
    this.splitEnd = splitEnd;
  }

  public PartitionLocation getParent() {
    return parent;
  }

  public void setParent(PartitionLocation parent) {
    this.parent = parent;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public int getEpoch() {
    return epoch;
  }

  public void setEpoch(int epoch) {
    this.epoch = epoch;
  }

  public int getSplitStart() {
    return splitStart;
  }

  public int getSplitEnd() {
    return splitEnd;
  }

  public void doSetSplitRange(int splitStart, int splitEnd) {
    this.splitStart = splitStart;
    this.splitEnd = splitEnd;
  }

  public void setSplitRange(int splitStart, int splitEnd) {
    doSetSplitRange(splitStart, splitEnd);
    if (peer != null) {
      peer.doSetSplitRange(splitStart, splitEnd);
    }
  }

  public String getSplitRange() {
    return id + "_" + splitStart + "_" + splitEnd;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = (host != null) ? host.intern() : null;
  }

  public int getPushPort() {
    return pushPort & 0xFFFF;
  }

  public void setPushPort(int pushPort) {
    this.pushPort = (short) pushPort;
  }

  public int getFetchPort() {
    return fetchPort & 0xFFFF;
  }

  public void setFetchPort(int fetchPort) {
    this.fetchPort = (short) fetchPort;
  }

  public int getRpcPort() {
    return rpcPort & 0xFFFF;
  }

  public void setRpcPort(int rpcPort) {
    this.rpcPort = (short) rpcPort;
  }

  public int getReplicatePort() {
    return replicatePort & 0xFFFF;
  }

  public void setReplicatePort(int replicatePort) {
    this.replicatePort = (short) replicatePort;
  }

  public String hostAndPorts() {
    return "host-rpcPort-pushPort-fetchPort-replicatePort:"
        + host
        + "-"
        + getRpcPort()
        + "-"
        + getPushPort()
        + "-"
        + getFetchPort()
        + "-"
        + getReplicatePort();
  }

  public String hostAndFetchPort() {
    return host + ":" + getFetchPort();
  }

  public String hostAndPushPort() {
    return host + ":" + getPushPort();
  }

  public Mode getMode() {
    return getMode(this.mode);
  }

  public void setMode(Mode mode) {
    this.mode = mode.mode();
  }

  public PartitionLocation getPeer() {
    return peer;
  }

  public void setPeer(PartitionLocation peer) {
    this.peer = peer;
  }

  public boolean hasPeer() {
    return peer != null;
  }

  public String getUniqueId() {
    return id + "-" + epoch;
  }

  /** @see PartitionLocation#getFileName */
  public String getFileName() {
    return id + "-" + epoch + "-" + mode;
  }

  public StorageInfo getStorageInfo() {
    return storageInfo;
  }

  public void setStorageInfo(StorageInfo storageInfo) {
    this.storageInfo = storageInfo;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof PartitionLocation)) {
      return false;
    }
    PartitionLocation o = (PartitionLocation) other;
    return id == o.id
        && epoch == o.epoch
        && host.equals(o.host)
        && rpcPort == o.rpcPort
        && pushPort == o.pushPort
        && fetchPort == o.fetchPort;
  }

  @Override
  public int hashCode() {
    return (id + epoch + host + rpcPort + pushPort + fetchPort).hashCode();
  }

  @Override
  public String toString() {
    String peerAddr = "empty";
    if (peer != null) {
      peerAddr = peer.hostAndPorts();
    }
    return "PartitionLocation["
        + "\n  id-epoch:"
        + id
        + "-"
        + epoch
        + "\n  host-rpcPort-pushPort-fetchPort-replicatePort:"
        + host
        + "-"
        + getRpcPort()
        + "-"
        + getPushPort()
        + "-"
        + getFetchPort()
        + "-"
        + getReplicatePort()
        + "\n  mode:"
        + getMode()
        + "\n  peer:("
        + peerAddr
        + ")\n  storage hint:"
        + storageInfo
        + "\n  mapIdBitMap:"
        + mapIdBitMap
        + "]";
  }

  public WorkerInfo getWorker() {
    return new WorkerInfo(host, getRpcPort(), getPushPort(), getFetchPort(), getReplicatePort());
  }

  public RoaringBitmap getMapIdBitMap() {
    if (mapIdBitMap == null) {
      mapIdBitMap = new RoaringBitmap();
    }
    return mapIdBitMap;
  }

  public void setMapIdBitMap(RoaringBitmap mapIdBitMap) {
    this.mapIdBitMap = mapIdBitMap;
  }
}
