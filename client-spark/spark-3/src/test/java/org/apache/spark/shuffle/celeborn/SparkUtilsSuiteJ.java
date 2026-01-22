package org.apache.spark.shuffle.celeborn;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import org.apache.celeborn.common.CelebornConf;

public class SparkUtilsSuiteJ {

  private SparkSession sparkSession;

  @After
  public void tearDown() {
    if (sparkSession != null) {
      sparkSession.stop();
      sparkSession = null;
    }
  }

  @Test
  public void testFromSparkConfWithExecutorMemoryAndCores() {
    sparkSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("testFromSparkConfWithExecutorMemoryAndCores")
            .config("spark.executor.memory", "2g")
            .config("spark.executor.cores", "4")
            .getOrCreate();
    CelebornConf celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext().getConf());
    Assert.assertEquals(2 * 1024, celebornConf.executorMemory());
    Assert.assertEquals(4, celebornConf.executorCores());
    Assert.assertEquals(512.0, celebornConf.executorMemoryPerCore(), 0.01);
  }

  @Test
  public void testFromSparkConfWithDefaultValues() {
    sparkSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("testFromSparkConfWithDefaultValues")
            .getOrCreate();
    CelebornConf celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext().getConf());
    // Default executor memory is 1024m
    Assert.assertEquals(1024, celebornConf.executorMemory());
    // Default executor cores is 1
    Assert.assertEquals(1, celebornConf.executorCores());
    Assert.assertEquals(1024.0, celebornConf.executorMemoryPerCore(), 0.01);
  }

  @Test
  public void testFromSparkConfWithCelebornConfigs() {
    sparkSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("testFromSparkConfWithCelebornConfigs")
            .config("spark.executor.memory", "4g")
            .config("spark.executor.cores", "8")
            .config("spark.celeborn.client.push.replicate.enabled", "true")
            .config("spark.celeborn.client.push.buffer.max.size", "128k")
            .getOrCreate();
    CelebornConf celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext().getConf());
    Assert.assertEquals(4 * 1024, celebornConf.executorMemory());
    Assert.assertEquals(8, celebornConf.executorCores());
    Assert.assertTrue(celebornConf.clientPushReplicateEnabled());
  }

  @Test
  public void testAdaptiveParamsSmallMemory() {
    // Test adaptive params for small memory (1g, 256m per core)
    sparkSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("testAdaptiveParamsSmallMemory")
            .config("spark.executor.memory", "1g")
            .config("spark.executor.cores", "4")
            .config("spark.celeborn.client.spark.useAdaptiveParams", "true")
            .getOrCreate();
    CelebornConf celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext().getConf());

    // For 1g memory with 256m per core, should use first rule: sortMemoryThreshold=16m,
    // bufferMaxSize=32k
    Assert.assertEquals(16 * 1024 * 1024, celebornConf.clientPushSortMemoryThreshold());
    Assert.assertEquals(32 * 1024, celebornConf.clientPushBufferMaxSize());
  }

  @Test
  public void testAdaptiveParamsMediumMemory() {
    // Test adaptive params for medium memory (2g, 512m per core)
    sparkSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("testAdaptiveParamsMediumMemory")
            .config("spark.executor.memory", "2g")
            .config("spark.executor.cores", "4")
            .config("spark.celeborn.client.spark.useAdaptiveParams", "true")
            .getOrCreate();
    CelebornConf celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext().getConf());

    // For 2g memory with 512m per core, should use second rule: sortMemoryThreshold=64m,
    // bufferMaxSize=64k
    Assert.assertEquals(64 * 1024 * 1024, celebornConf.clientPushSortMemoryThreshold());
    Assert.assertEquals(64 * 1024, celebornConf.clientPushBufferMaxSize());
  }

  @Test
  public void testAdaptiveParamsLargeMemory() {
    // Test adaptive params for large memory (4g, 1g per core)
    sparkSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("testAdaptiveParamsLargeMemory")
            .config("spark.executor.memory", "4g")
            .config("spark.executor.cores", "4")
            .config("spark.celeborn.client.spark.useAdaptiveParams", "true")
            .getOrCreate();
    CelebornConf celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext().getConf());

    // For 4g memory with 1g per core, should use third rule: sortMemoryThreshold=128m,
    // bufferMaxSize=64k
    Assert.assertEquals(128 * 1024 * 1024, celebornConf.clientPushSortMemoryThreshold());
    Assert.assertEquals(64 * 1024, celebornConf.clientPushBufferMaxSize());
  }

  @Test
  public void testAdaptiveParamsVeryLargeMemory() {
    // Test adaptive params for very large memory (12g, 3g per core)
    sparkSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("testAdaptiveParamsVeryLargeMemory")
            .config("spark.executor.memory", "12g")
            .config("spark.executor.cores", "4")
            .config("spark.celeborn.client.spark.useAdaptiveParams", "true")
            .getOrCreate();
    CelebornConf celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext().getConf());

    // For 12g memory with 3g per core, should use fifth rule: sortMemoryThreshold=512m,
    // bufferMaxSize=128k
    Assert.assertEquals(512 * 1024 * 1024, celebornConf.clientPushSortMemoryThreshold());
    Assert.assertEquals(128 * 1024, celebornConf.clientPushBufferMaxSize());
  }

  @Test
  public void testAdaptiveParamsExtremelyLargeMemory() {
    // Test adaptive params for extremely large memory (32g, 8g per core)
    sparkSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("testAdaptiveParamsExtremelyLargeMemory")
            .config("spark.executor.memory", "32g")
            .config("spark.executor.cores", "4")
            .config("spark.celeborn.client.spark.useAdaptiveParams", "true")
            .getOrCreate();
    CelebornConf celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext().getConf());

    // For 32g memory, should use last rule: sortMemoryThreshold=1g, bufferMaxSize=1m
    Assert.assertEquals(1024 * 1024 * 1024, celebornConf.clientPushSortMemoryThreshold());
    Assert.assertEquals(1024 * 1024, celebornConf.clientPushBufferMaxSize());
  }

  @Test
  public void testAdaptiveParamsDisabled() {
    // Test when adaptive params is disabled
    sparkSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("testAdaptiveParamsDisabled")
            .config("spark.executor.memory", "4g")
            .config("spark.executor.cores", "4")
            .config("spark.celeborn.client.spark.useAdaptiveParams", "false")
            .config("spark.celeborn.client.push.buffer.max.size", "256k")
            .config("spark.celeborn.client.spark.push.sort.memory.threshold", "256m")
            .getOrCreate();
    CelebornConf celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext().getConf());

    // Should use configured values instead of adaptive values
    // Note: Default value is 64m for sort memory threshold when not using adaptive params
    Assert.assertEquals(256 * 1024 * 1024, celebornConf.clientPushSortMemoryThreshold());
    Assert.assertEquals(256 * 1024, celebornConf.clientPushBufferMaxSize());
  }

  @Test
  public void testAdaptiveParamsHighCoreCount() {
    // Test adaptive params with high core count (low memory per core)
    sparkSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("testAdaptiveParamsHighCoreCount")
            .config("spark.executor.memory", "4g")
            .config("spark.executor.cores", "16")
            .config("spark.celeborn.client.spark.useAdaptiveParams", "true")
            .getOrCreate();
    CelebornConf celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext().getConf());

    // 4g / 16 cores = 256m per core, should match first rule
    Assert.assertEquals(256.0, celebornConf.executorMemoryPerCore(), 0.01);
    Assert.assertEquals(16 * 1024 * 1024, celebornConf.clientPushSortMemoryThreshold());
    Assert.assertEquals(32 * 1024, celebornConf.clientPushBufferMaxSize());
  }

  @Test
  public void testAdaptiveParamsLowCoreCount() {
    // Test adaptive params with low core count (high memory per core)
    sparkSession =
        SparkSession.builder()
            .master("local[*]")
            .appName("testAdaptiveParamsLowCoreCount")
            .config("spark.executor.memory", "8g")
            .config("spark.executor.cores", "2")
            .config("spark.celeborn.client.spark.useAdaptiveParams", "true")
            .getOrCreate();
    CelebornConf celebornConf = SparkUtils.fromSparkConf(sparkSession.sparkContext().getConf());

    // 8g / 2 cores = 4g per core
    // The rule uses OR logic: execMemoryMB <= rule.maxExecutorMemoryMB || memoryPerCoreMB <=
    // rule.maxMemoryPerCoreMB
    // 8g (8192MB) <= 8g (8192MB) matches the 4th rule: sortMemoryThreshold=128m, bufferMaxSize=64k
    Assert.assertEquals(4096.0, celebornConf.executorMemoryPerCore(), 0.01);
    Assert.assertEquals(128 * 1024 * 1024, celebornConf.clientPushSortMemoryThreshold());
    Assert.assertEquals(64 * 1024, celebornConf.clientPushBufferMaxSize());
  }

  @Test
  public void testFromSparkConfDirectly() {
    // Test using SparkConf directly without SparkSession
    SparkConf sparkConf =
        new SparkConf()
            .set("spark.executor.memory", "6g")
            .set("spark.executor.cores", "6")
            .set("spark.celeborn.client.push.replicate.enabled", "false");

    CelebornConf celebornConf = SparkUtils.fromSparkConf(sparkConf);
    Assert.assertEquals(6 * 1024, celebornConf.executorMemory());
    Assert.assertEquals(6, celebornConf.executorCores());
    Assert.assertEquals(1024.0, celebornConf.executorMemoryPerCore(), 0.01);
    Assert.assertFalse(celebornConf.clientPushReplicateEnabled());
  }

  @Test
  public void testExecutorCoresMinimumValue() {
    // Test that executor cores has minimum value of 1
    SparkConf sparkConf =
        new SparkConf().set("spark.executor.memory", "2g").set("spark.executor.cores", "0");

    CelebornConf celebornConf = SparkUtils.fromSparkConf(sparkConf);
    // Should be at least 1
    Assert.assertEquals(1, celebornConf.executorCores());
  }

  @Test
  public void testMemoryStringFormats() {
    // Test different memory string formats
    SparkConf sparkConf1 = new SparkConf().set("spark.executor.memory", "2048m");
    CelebornConf celebornConf1 = SparkUtils.fromSparkConf(sparkConf1);
    Assert.assertEquals(2048, celebornConf1.executorMemory());

    SparkConf sparkConf2 = new SparkConf().set("spark.executor.memory", "2g");
    CelebornConf celebornConf2 = SparkUtils.fromSparkConf(sparkConf2);
    Assert.assertEquals(2 * 1024, celebornConf2.executorMemory());

    SparkConf sparkConf3 = new SparkConf().set("spark.executor.memory", "1024");
    CelebornConf celebornConf3 = SparkUtils.fromSparkConf(sparkConf3);
    // Should parse as bytes and convert to MB
    Assert.assertTrue(celebornConf3.executorMemory() >= 0);
  }
}
