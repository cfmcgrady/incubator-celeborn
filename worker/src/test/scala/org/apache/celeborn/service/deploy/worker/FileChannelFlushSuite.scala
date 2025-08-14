package org.apache.celeborn.service.deploy.worker

import org.apache.celeborn.CelebornFunSuite

import java.io.{File, RandomAccessFile}
import java.nio.channels.FileChannel

import io.netty.buffer.{CompositeByteBuf, Unpooled}
import scala.util.Random

class FileChannelLargeFlushSuite extends CelebornFunSuite {
  val totalGB = 1
  val minChunkSize = 1024      // 1K
  val maxChunkSize = 64 * 1024 // 64K
  val batchChunks = 64         // 一个批次的分片数
  // 平均每个chunk约32K, 那一批大约2M, 1GB需批次数=1GB/2MB=~512
  // 性能测试时，可适当调大batchChunks

  // 为了估算总数据量，统计每轮实际batch总字节数
  def genChunkSizes(batchChunks: Int): Array[Int] =
    Array.fill(batchChunks)(Random.nextInt(maxChunkSize - minChunkSize + 1) + minChunkSize)

  // 返回(CompositeByteBuf, 数组:该batch每个chunk长度)
  def allocCompositeBatch(chunkSizes: Array[Int]): CompositeByteBuf = {
    val buf = Unpooled.compositeBuffer(batchChunks)
    for (s <- chunkSizes) {
      buf.addComponent(true, Unpooled.directBuffer(s).writeZero(s))
    }
    buf
  }

  def method1_individualWrite(buffer: CompositeByteBuf, fc: FileChannel): Unit = {
    val buffers = buffer.nioBuffers()
    for (b <- buffers) while (b.hasRemaining) fc.write(b)
  }
  def method2_batchWrite(buffer: CompositeByteBuf, fc: FileChannel): Unit = {
    val buffers = buffer.nioBuffers()
    var remain = buffers.map(_.remaining().toLong).sum
    while (remain > 0) remain -= fc.write(buffers)
  }
  def method3_consolidateWrite(buffer: CompositeByteBuf, fc: FileChannel): Unit = {
    val consolidated = buffer.consolidate()
    val buffers = consolidated.nioBuffers()
    for (b <- buffers) while (b.hasRemaining) fc.write(b)
    consolidated.release()
  }

  test("file channel 1GB, random chunk size 1K~64K") {
    val outdir = new File("/tmp/fc_bench_test")
    outdir.mkdirs()
    println(s"随机chunk输出：每轮每片[1K~64K]，共${batchChunks}个chunk一批。测试1 GB.")

    for ((desc, fn) <- Seq(
      "单片写 method1_individualWrite" -> method1_individualWrite _
//      "批量write(ByteBuffer[]) method2_batchWrite" -> method2_batchWrite _,
//      "consolidate后一次写 method3_consolidateWrite" -> method3_consolidateWrite _
    )) {
      val raf = new RandomAccessFile(new File(outdir, s"${desc.replaceAll("[^a-zA-Z0-9]", "")}.data"), "rw")
      val fc = raf.getChannel
      val start = System.nanoTime()
      var totalWrittenBytes: Long = 0
      var bidx = 0
      while (totalWrittenBytes < totalGB * 1024 * 1024 * 1024L) {
        val chunkSizes = genChunkSizes(batchChunks)
        val buffer = allocCompositeBatch(chunkSizes)
        fn(buffer, fc)
        // 注意：consolidate分支已自动release，其它分支需要手动release
        if (!desc.contains("consolidate")) buffer.release()
        val batchBytes = chunkSizes.sum
        totalWrittenBytes += batchBytes
        bidx += 1
        if (bidx % 256 == 0) {
          println(s"[$desc] Progress: ${totalWrittenBytes / (1024*1024)} MB")
        }
      }
      fc.close(); raf.close()
      val used = (System.nanoTime() - start) / 1000000
      println(s"$desc : $used ms, 实际写入${totalWrittenBytes/1024/1024} MB")
      // 注释掉自动删除，文件可用
      // val file = new File(outdir, s"${desc.replaceAll("[^a-zA-Z0-9]", "")}.data")
      // file.delete()
    }
    println("测试完成！")
  }
}
