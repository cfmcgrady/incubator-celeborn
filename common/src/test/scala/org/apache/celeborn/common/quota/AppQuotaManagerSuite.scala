package org.apache.celeborn.common.quota

import java.util

import scala.collection.JavaConverters._

import org.scalatest.BeforeAndAfterAll

import org.apache.celeborn.CelebornFunSuite
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.WorkerInfo
import org.apache.celeborn.common.protocol.message.ControlMessages.CheckQuotaResponse
import org.apache.celeborn.common.util.Utils

class AppQuotaManagerSuite extends CelebornFunSuite
  with BeforeAndAfterAll
  with Logging {

  private var quotaManager: QuotaManager = _

  private val conf = new CelebornConf()

  override def beforeAll(): Unit = {
    super.beforeAll()
    conf.set("celeborn.quota.app.enabled", "true")
    conf.set("celeborn.quota.manager", "org.apache.celeborn.common.quota.AppQuotaManager")
    conf.set("celeborn.quota.app.diskBytesWritten", "200G")
    quotaManager = QuotaManager.instantiate(conf)
    assert(quotaManager.isInstanceOf[AppQuotaManager])
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("test application quota checker") {
    val user = UserIdentifier("red_tenant", "xinxi")
    val workers = new util.ArrayList[WorkerInfo]

    val worker1 = new WorkerInfo(
      "localhost",
      10001,
      10002,
      10003,
      10004)
    val rc1 = ResourceConsumption(
      Utils.byteStringAsBytes("200G"),
      20000,
      Utils.byteStringAsBytes("30G"),
      40)
    rc1.subResourceConsumptions =
      Map(
        "app1" -> ResourceConsumption(
          Utils.byteStringAsBytes("150G"),
          15000,
          Utils.byteStringAsBytes("25G"),
          20),
        "app2" -> ResourceConsumption(
          Utils.byteStringAsBytes("50G"),
          5000,
          Utils.byteStringAsBytes("5G"),
          20)).asJava
    worker1.userResourceConsumption.put(user, rc1)
    workers.add(worker1)

    val worker2 = new WorkerInfo(
      "localhost",
      10005,
      10006,
      10007,
      10008)
    val rc2 = ResourceConsumption(
      Utils.byteStringAsBytes("200G"),
      20000,
      Utils.byteStringAsBytes("30G"),
      40)
    rc2.subResourceConsumptions =
      Map(
        "app1" -> ResourceConsumption(
          Utils.byteStringAsBytes("150G"),
          15000,
          Utils.byteStringAsBytes("25G"),
          20),
        "app2" -> ResourceConsumption(
          Utils.byteStringAsBytes("50G"),
          5000,
          Utils.byteStringAsBytes("5G"),
          20)).asJava
    worker2.userResourceConsumption.put(user, rc1)
    workers.add(worker2)

    val workerResourcesIter = workers.asScala.iterator.flatMap { workerInfo =>
      workerInfo.userResourceConsumption.asScala.iterator
    }
    quotaManager.refresh(workerResourcesIter)

    assert(quotaManager.appQuotaStatus.size() == 1)

    val res1 = quotaManager.checkApplicationQuotaStatus("app1")
    assert(res1 == CheckQuotaResponse(
      false,
      "Interrupt application caused by the app storage usage reach threshold. " +
        "Used: ResourceConsumption(" +
        "diskBytesWritten: 300.0 GiB, " +
        "diskFileCount: 30000, " +
        "hdfsBytesWritten: 50.0 GiB, " +
        "hdfsFileCount: 40), " +
        "Threshold: " +
        "Quota[" +
        "diskBytesWritten=200.0 GiB, " +
        "diskFileCount=9223372036854775807, " +
        "hdfsBytesWritten=8.0 EiB, " +
        "hdfsFileCount=9223372036854775807]."))

    val res2 = quotaManager.checkApplicationQuotaStatus("app2")
    assert(res2 == CheckQuotaResponse(true, ""))
  }

}
