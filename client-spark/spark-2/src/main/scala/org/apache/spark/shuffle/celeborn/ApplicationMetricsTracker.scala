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

package org.apache.spark.shuffle.celeborn

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{JobFailed, JobSucceeded, SparkListener, SparkListenerApplicationEnd, SparkListenerJobEnd}
import org.apache.spark.shuffle.celeborn.ApplicationMetricsTracker.{APPLICATION_HAS_CELEBORN_FAILURE_JOB_COUNT, APPLICATION_SUCCEEDED_COUNT, JOB_FAILED_CELEBORN_COUNT, JOB_FAILED_OTHER_COUNT, JOB_SUCCEEDED_COUNT}

import org.apache.celeborn.client.LifecycleManager
import org.apache.celeborn.common.protocol.PbReportApplicationCounterMetrics

class ApplicationMetricsTracker(lifecycleManager: LifecycleManager) extends SparkListener
  with Logging {
  private val isReportedAppJobFailureToMaster = new AtomicBoolean(false)
  logInfo("adding ApplicationMetricsTracker to listener bus.")
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    logInfo(s"received event $jobEnd")
    val jobEndMetricsBuilder = PbReportApplicationCounterMetrics.newBuilder()
    jobEnd.jobResult match {
      case JobFailed(exception) if ApplicationMetricsTracker.causedByCeleborn(exception) =>
        jobEndMetricsBuilder.setMetricsName(JOB_FAILED_CELEBORN_COUNT).setValue(1)
        if (!isReportedAppJobFailureToMaster.getAndSet(true)) {
          lifecycleManager.reportMasterApplicationCounterMetrics(
            PbReportApplicationCounterMetrics.newBuilder()
              .setMetricsName(APPLICATION_HAS_CELEBORN_FAILURE_JOB_COUNT)
              .setValue(1)
              .build())
        }
      case JobFailed(_) =>
        jobEndMetricsBuilder.setMetricsName(JOB_FAILED_OTHER_COUNT).setValue(1)
      case JobSucceeded =>
        jobEndMetricsBuilder.setMetricsName(JOB_SUCCEEDED_COUNT).setValue(1)
    }
    lifecycleManager.reportMasterApplicationCounterMetrics(jobEndMetricsBuilder.build())
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    if (!isReportedAppJobFailureToMaster.get()) {
      lifecycleManager.reportMasterApplicationCounterMetrics(
        PbReportApplicationCounterMetrics.newBuilder()
          .setMetricsName(APPLICATION_SUCCEEDED_COUNT)
          .setValue(1)
          .build())
    }
  }
}

object ApplicationMetricsTracker {
  val APPLICATION_HAS_CELEBORN_FAILURE_JOB_COUNT = "ApplicationHasCelebornFailureJobCount"
  val APPLICATION_SUCCEEDED_COUNT = "ApplicationSucceededCount"
  val JOB_FAILED_CELEBORN_COUNT = "JobFailedCelebornCount"
  val JOB_FAILED_OTHER_COUNT = "JobFailedOtherCount"
  val JOB_SUCCEEDED_COUNT = "JobSucceededCount"
  def causedByCeleborn(ex: Throwable): Boolean = {
    ex != null && ex.getMessage != null && ex.getMessage.contains("CelebornIOException")
  }
}
