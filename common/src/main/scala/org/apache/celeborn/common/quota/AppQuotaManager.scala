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

    val exceededQuotaAppInfoMap = appAggregated.collect {
      case (appId, consumption) if checkConsumptionExceeded(consumption, quotaThreshold) =>
        appId -> QuotaStatus(
          exceed = true,
          s"$expireReason Used: ${consumption.simpleString}, Threshold: $quotaThreshold.")
    }.toMap

    // 清理过期的配额状态：移除已终止或者已不再超过配额的appId
    // 主备master的appQuotaStatus独立更新，需要确保过期数据被及时清理
    // 通过遍历当前状态集合并移除过期的appId，保证数据一致，且内存健康
    // 注：考虑到appQuotaStatus仅记录超配额的app，数据量有限，遍历的性能开销可接受
    val validAppIds = exceededQuotaAppInfoMap.keySet
    val iterator = appQuotaStatus.keySet().iterator()
    while (iterator.hasNext) {
      val appId = iterator.next()
      if (!validAppIds.contains(appId)) {
        appQuotaStatus.remove(appId)
      }
    }

    exceededQuotaAppInfoMap.foreach { case (appId, status) =>
      appQuotaStatus.put(appId, status)
    }
  }
}
