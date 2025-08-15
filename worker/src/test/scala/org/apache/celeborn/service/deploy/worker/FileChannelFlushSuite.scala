package org.apache.celeborn.service.deploy.worker

import org.apache.celeborn.CelebornFunSuite

import java.io.{File, RandomAccessFile}
import java.nio.channels.FileChannel

import io.netty.buffer.{CompositeByteBuf, Unpooled}
import scala.util.Random

class FileChannelLargeFlushSuite extends CelebornFunSuite {
  val totalGB = 1
  val minChunkSize = 1         // 1B
  val maxChunkSize = 64 * 1024 // 64K
  val batchChunks = 64         // 一个批次的分片数
  val flushBatchSize = 256 * 1024  // 256K per flush batch

  def genRandomChunkSize(): Int = Random.nextInt(maxChunkSize - minChunkSize + 1) + minChunkSize
  def allocFlushBatch(): (CompositeByteBuf, Int) = {
    val buf = Unpooled.compositeBuffer()
    var totalSize = 0
    while (totalSize < flushBatchSize) {
      val s = genRandomChunkSize()
      buf.addComponent(true, Unpooled.directBuffer(s).writeZero(s))
      totalSize += s
    }
    (buf, totalSize)
  }

  def genChunkSizes(batchChunks: Int): Array[Int] =
    Array.fill(batchChunks)(Random.nextInt(maxChunkSize - minChunkSize + 1) + minChunkSize)

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
    // 注意这里不用对原 buffer release，否则会二次释放
  }

  // 智能小块合并，只批量 release CompositeByteBuf
  def smartConsolidateSmallComponents(
      buf: CompositeByteBuf,
      fc: FileChannel,
      sizeThreshold: Int = 4 * 1024,
      minConsecutive: Int = 4): Unit = {
    // 自动合并连续小块
    var i = 0
    while (i < buf.numComponents) {
      if (buf.component(i).readableBytes() < sizeThreshold) {
        var j = i + 1
        while (j < buf.numComponents && buf.component(j).readableBytes() < sizeThreshold) j += 1
        val count = j - i
        if (count >= minConsecutive) {
          buf.consolidate(i, count)
          // 合并后数量变少，重新开始
          i = 0
        } else {
          i = j
        }
      } else {
        i += 1
      }
    }
    // 批量写
    val nioBufs = buf.nioBuffers()
    var remain = nioBufs.map(_.remaining().toLong).sum
    while (remain > 0) remain -= fc.write(nioBufs)
    // 注意不用对子 buf 单独 release！
    // 只需调用 buf.release()，即可递归 release
  }

  test("file channel 1GB, random chunk size 1K~64K") {
    val outdir = new File("/tmp/fc_bench_test")
    outdir.mkdirs()
    println(s"随机chunk输出：每轮每片[1K~64K]，共${batchChunks}个chunk一批。测试1 GB.")

    for ((desc, fn, needRelease) <- Seq(
      ("单片写 method1_individualWrite",           method1_individualWrite _, true),
//      ("批量write(ByteBuffer[]) method2_batchWrite", method2_batchWrite _,     true),
//      ("consolidate后一次写 method3_consolidateWrite", method3_consolidateWrite _, false),
//      ("智能小块合并（consolidate(int,int)）", (buf: CompositeByteBuf, fc: FileChannel) =>
//        smartConsolidateSmallComponents(buf, fc, sizeThreshold = 4 * 1024, minConsecutive = 4), true)
    )) {
      val raf = new RandomAccessFile(new File(outdir, s"${desc.replaceAll("[^a-zA-Z0-9]", "")}.data"), "rw")
      val fc = raf.getChannel
      val start = System.nanoTime()
      var totalWrittenBytes: Long = 0
      var bidx = 0
      while (totalWrittenBytes < totalGB * 1024 * 1024 * 1024L) {
        val chunkSizes = genChunkSizes(batchChunks)
        val (buffer, _) = allocFlushBatch()
        fn(buffer, fc)
        if (needRelease) {
          buffer.release() // 只对需要的三种方式release一次
        }
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
      // 你可以取消自动删除，保留产出文件
    }
    println("测试完成！")
  }
}
