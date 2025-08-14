package org.apache.celeborn.service.deploy.worker

import org.apache.celeborn.CelebornFunSuite
import java.io.{File, RandomAccessFile}
import java.nio.channels.FileChannel
import io.netty.buffer.{CompositeByteBuf, Unpooled}

class FileChannelLargeFlushSuite extends CelebornFunSuite {
  val totalGB = 1
  val chunkSize = 4 * 1024
  val batchChunks = 64
  val totalChunks = (totalGB * 1024 * 1024 * 1024L / chunkSize).toInt
  val totalBatch = totalChunks / batchChunks

  def allocCompositeBatch(): CompositeByteBuf = {
    val buf = Unpooled.compositeBuffer(batchChunks)
    for (_ <- 0 until batchChunks) {
      buf.addComponent(true, Unpooled.directBuffer(chunkSize).writeZero(chunkSize))
    }
    buf
  }

  // --- 写入方法
  def method1_individualWrite(buffer: CompositeByteBuf, fc: FileChannel): Unit = {
    val buffers = buffer.nioBuffers()
    for (b <- buffers) while (b.hasRemaining) fc.write(b)
  }
  def method2_batchWrite(buffer: CompositeByteBuf, fc: FileChannel): Unit = {
    val buffers = buffer.nioBuffers()
    var remain = buffers.map(_.remaining().toLong).sum
    while (remain > 0) {
      val w = fc.write(buffers)
      remain -= w
    }
  }
  def method3_consolidateWrite(buffer: CompositeByteBuf, fc: FileChannel): Unit = {
    val consolidated = buffer.consolidate()
    val buffers = consolidated.nioBuffers()
    for (b <- buffers) while (b.hasRemaining) fc.write(b)
    consolidated.release() // 只release consolidate出来的，不再release原buf
  }

  test("file channel 1GB benchmark with batched direct buffer") {
    val outdir = new File("/tmp/fc_bench_test")
    outdir.mkdirs()
    println(s"Flush ${totalGB} GB data, $totalChunks chunks, batch size=$batchChunks * $chunkSize = ${batchChunks*chunkSize/1024} KB")

    for ((desc, fn) <- Seq(
      "单片写 method1_individualWrite" -> method1_individualWrite _,
      "批量write(ByteBuffer[]) method2_batchWrite" -> method2_batchWrite _,
      "consolidate后一次写 method3_consolidateWrite" -> method3_consolidateWrite _
    )) {
      val raf = new RandomAccessFile(new File(outdir, s"${desc.replaceAll("[^a-zA-Z0-9]", "")}.data"), "rw")
      val fc = raf.getChannel
      val start = System.nanoTime()
      for (i <- 0 until totalBatch) {
        val buffer = allocCompositeBatch()
        if(desc.contains("consolidate")) {
          fn(buffer, fc) // fn里已经release consolidateBuf
          // 不能再release buffer，否则会二次release子buf
        } else {
          fn(buffer, fc)
          buffer.release()
        }
        if (i % 2048 == 1024) println(s"[$desc] progress: ${(i.toLong*batchChunks*chunkSize/1024/1024/1024)} GB")
      }
      fc.close(); raf.close()
      val used = (System.nanoTime() - start) / 1000000
      println(s"$desc : $used ms")
      val file = new File(outdir, s"${desc.replaceAll("[^a-zA-Z0-9]", "")}.data")
//      file.delete()
    }
    println("测试完成")
  }
}
