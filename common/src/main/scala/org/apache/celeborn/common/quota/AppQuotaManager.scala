package org.apache.celeborn.common.quota

import scala.collection.mutable

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.identity.UserIdentifier

class AppQuotaManager(conf: CelebornConf) extends QuotaManager(conf) {
  private def expireReason = QuotaStatus.APP_EXHAUSTED

  private def getQuotaThreshold = Quota(
    conf.quotaAppDiskBytesWritten,
    conf.quotaAppDiskFileCount,
    conf.quotaAppHdfsBytesWritten,
    conf.quotaAppHdfsFileCount)

  override def initialize(): Unit = {
    // Nothing to do
  }

  override def refresh(workerResources: Iterator[(UserIdentifier, ResourceConsumption)]): Unit = {
    val appAggregated = mutable.Map.empty[String, ResourceConsumption]
    val quotaThreshold = getQuotaThreshold

    // 直接处理迭代器
    workerResources.foreach { case (_, userConsumption) =>
      val subResources = userConsumption.subResourceConsumptions
      if (subResources != null && !subResources.isEmpty) {
        // 使用Java迭代器避免创建Scala Map
        val iter = subResources.entrySet().iterator()
        while (iter.hasNext) {
          val entry = iter.next()
          val appId = entry.getKey
          val consumption = entry.getValue

          appAggregated(appId) = appAggregated.get(appId)
            .map(_.add(consumption))
            .getOrElse(consumption)
        }
      }
    }

    appAggregated.foreach { case (appId, consumption) =>
      if (checkConsumptionExceeded(consumption, quotaThreshold)) {
        appQuotaStatus.put(
          appId,
          QuotaStatus(
            exceed = true,
            s"$expireReason Used: ${consumption.simpleString}, Threshold: $quotaThreshold."))
      } else {
        appQuotaStatus.remove(appId)
      }
    }
  }
}
