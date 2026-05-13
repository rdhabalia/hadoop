/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.io.nativeio.NativeIO.POSIX.POSIX_FADV_DONTNEED;

import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

/**
 * Pooled-buffer block writer that accumulates packet writes in an off-heap
 * Netty {@link ByteBuf} and flushes the contents to disk through a
 * {@link FileChannel} opened with {@link StandardOpenOption#DSYNC} so each
 * flush is durably persisted without paying for an explicit fsync per packet.
 *
 * <p>Concurrency:
 * <ul>
 *   <li>Total concurrent in-flight buffers across the DataNode are bounded by
 *       {@code writeMemoryBufferMaxConcurrentWrites} (semaphore).</li>
 *   <li>Per-volume flush concurrency is bounded by {@code flushWritesSemaphore}
 *       which prevents many large flushes piling up on the same spindle.</li>
 *   <li>Mutating methods are {@code synchronized} so that a write coming in
 *       from the receiver thread cannot race a flush triggered from a different
 *       path (e.g. responder interrupt → close).</li>
 * </ul></p>
 */
public class BufferedBlockWriterImpl implements BufferedBlockWriter {

  public static final Logger LOG =
      LoggerFactory.getLogger(BufferedBlockWriterImpl.class);

  private ByteBuf nettyBuf;
  private volatile FileChannel fc;
  private final File file;
  private final FsVolumeImpl volume;
  private final BlockReceiver blockReceiver;
  private final Semaphore writeMemoryBufferMaxConcurrentWrites;
  private final Semaphore flushWritesSemaphore;
  private final ExecutorService volumeExecutor;
  private final String blockName;
  private long totalFlushedBytes;
  // Last position successfully written to disk; used to resume the channel
  // after a ClosedByInterruptException.
  private long lastFilePos = 0L;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private boolean permitAcquired = false;

  /**
   * @param blockReceiver owning receiver — used as a callback for
   *                      checksum-stream coordination during flushOrSync.
   * @param file          target block data file.
   * @param volume        backing volume — must be non-null; the caller
   *                      ({@link BlockReceiver}) must short-circuit to
   *                      {@link #NO_OP_INSTANCE} otherwise.
   * @param writeMemoryBufferMaxConcurrentWrites global concurrency budget.
   */
  public BufferedBlockWriterImpl(BlockReceiver blockReceiver, File file,
      FsVolumeImpl volume, Semaphore writeMemoryBufferMaxConcurrentWrites)
      throws IOException {
    this.file = file;
    this.volume = volume;
    this.blockReceiver = blockReceiver;
    this.blockName = blockReceiver.getBlock().getBlockName();
    this.writeMemoryBufferMaxConcurrentWrites =
        writeMemoryBufferMaxConcurrentWrites;
    FsVolumeImpl.BufferWriteResource resources = volume.getBufferResources();
    this.flushWritesSemaphore = resources != null
        ? resources.getFlushPermitSemaphore().orElse(null) : null;
    this.volumeExecutor = resources != null
        ? resources.getVolumeExecutor() : null;

    // Allocate the buffer and acquire the global permit BEFORE opening the
    // channel — if we fail later, release() will clean things up.
    boolean ok = false;
    ByteBuf buf = null;
    try {
      acquirePermit();
      buf = PooledByteBufAllocator.DEFAULT
          .buffer(volume.getMaxWriteBufferCapacityBytes());
      this.nettyBuf = buf;
      this.fc = FileChannel.open(file.toPath(), StandardOpenOption.CREATE,
          StandardOpenOption.WRITE, StandardOpenOption.DSYNC);
      ok = true;
    } finally {
      if (!ok) {
        if (buf != null) {
          buf.release();
          this.nettyBuf = null;
        }
        if (permitAcquired) {
          writeMemoryBufferMaxConcurrentWrites.release();
          permitAcquired = false;
        }
      }
    }
  }

  @Override
  public synchronized void writeData(ByteBuffer dataBuf, int startByteToDisk,
      int numBytesToDisk) throws IOException {
    if (nettyBuf == null) {
      throw new IOException(
          "Write buffer for " + blockName + " is already released");
    }
    byte[] data = dataBuf.array();
    int remaining = numBytesToDisk;
    while (remaining > 0) {
      int writable = Math.min(nettyBuf.writableBytes(), remaining);
      nettyBuf.writeBytes(data, startByteToDisk, writable);
      startByteToDisk += writable;
      remaining -= writable;

      if (nettyBuf.writableBytes() == 0) {
        // Buffer full — route through the receiver so the checksum stream
        // stays in lockstep with the data stream.
        flushOrSync(true, true, false);
      }
    }
  }

  @Override
  public void flush() throws IOException {
    acquireFlushPermit();
    try {
      flushInternal();
    } finally {
      releaseFlushPermit();
    }
  }

  @Override
  public void flushOrSync(boolean fsync, boolean bufferFlush, boolean isClosed)
      throws IOException {
    // Delegate to the BlockReceiver so checksum stream + data stream stay
    // coherent. The receiver will call back into our flush()/syncData() as
    // appropriate.
    blockReceiver.flushOrSync(fsync,
        false /* fsync checksum during closing the block */, bufferFlush,
        isClosed);
  }

  @Override
  public synchronized void release() {
    if (!closed.compareAndSet(false, true)) {
      return; // Idempotent — already released.
    }
    try {
      closeFileChannelAndDropPageCache();
    } finally {
      if (nettyBuf != null) {
        try {
          nettyBuf.release();
        } catch (Throwable t) {
          LOG.warn("Failed to release netty buffer for {}", blockName, t);
        }
        nettyBuf = null;
      }
      if (permitAcquired) {
        writeMemoryBufferMaxConcurrentWrites.release();
        permitAcquired = false;
      }
    }
  }

  /**
   * Write all currently-buffered bytes to the file channel. Retries once on
   * {@link ClosedByInterruptException} which is the usual outcome when an
   * upstream failure causes the receiver thread to be interrupted in the
   * middle of an NIO write.
   */
  synchronized void flushInternal() throws IOException {
    if (nettyBuf == null || nettyBuf.readableBytes() == 0) {
      return;
    }
    nettyBuf.markReaderIndex();
    boolean success = false;
    try {
      writeBufferToChannel();
      success = true;
    } catch (ClosedByInterruptException e) {
      boolean wasInterrupted = Thread.currentThread().isInterrupted();
      LOG.warn(
          "Flush failed, retrying once from file position {} for block {}, "
              + "interrupted={}",
          lastFilePos, blockName, wasInterrupted, e);
      // Clear the interrupt flag so the retry NIO write doesn't immediately
      // get cancelled. We'll restore it below.
      Thread.interrupted();
      try {
        fc.close();
      } catch (IOException ignored) {
        // best-effort
      }
      try {
        fc = FileChannel.open(file.toPath(), StandardOpenOption.CREATE,
            StandardOpenOption.WRITE, StandardOpenOption.DSYNC);
        fc.position(lastFilePos);
        nettyBuf.resetReaderIndex();
        writeBufferToChannel();
        success = true;
      } catch (IOException ex) {
        LOG.error("Retry flush failed for block {}", blockName, ex);
        throw ex;
      } finally {
        if (wasInterrupted) {
          Thread.currentThread().interrupt();
        }
      }
    } finally {
      if (success) {
        nettyBuf.clear();
      } else {
        nettyBuf.resetReaderIndex();
      }
    }
  }

  private void writeBufferToChannel() throws IOException {
    int readableBytes = nettyBuf.readableBytes();
    if (readableBytes == 0) {
      return;
    }
    ByteBuffer[] nioBufs = nettyBuf.nioBuffers(nettyBuf.readerIndex(),
        readableBytes);
    long remaining = readableBytes;
    while (remaining > 0) {
      long written = fc.write(nioBufs);
      if (written <= 0) {
        // Should not happen with a blocking FileChannel, but guard against a
        // pathological 0-byte write to avoid looping forever.
        throw new IOException(
            "FileChannel.write returned " + written + " for " + blockName);
      }
      remaining -= written;
    }
    lastFilePos = fc.position();
    totalFlushedBytes += readableBytes;
  }

  @Override
  public void syncData(String blockName, boolean isClosed) {
    FileChannel localFc = fc;
    if (localFc == null) {
      return;
    }
    try {
      localFc.force(false);
    } catch (Exception e) {
      LOG.warn("Failed to fsync {}", this.blockName, e);
    }
  }

  /**
   * Close the file channel and, on a best-effort basis, drop the page cache
   * for the block file so the kernel can reclaim it for read-side use.
   */
  private void closeFileChannelAndDropPageCache() {
    FileChannel localFc = fc;
    if (localFc != null) {
      try {
        localFc.close();
      } catch (Exception e) {
        LOG.warn("Failed to close file channel for {}", blockName, e);
      }
    }
    if (volumeExecutor == null) {
      return;
    }
    try {
      volumeExecutor.execute(() -> {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
          FileDescriptor fd = raf.getFD();
          NativeIO.POSIX.getCacheManipulator().posixFadviseIfPossible(blockName,
              fd, 0, 0, POSIX_FADV_DONTNEED);
        } catch (Exception e) {
          LOG.warn("Failed to drop page cache for {}", blockName, e);
        }
      });
    } catch (Exception e) {
      LOG.warn("Failed to schedule page-cache drop for {}", blockName, e);
    }
  }

  private void acquirePermit() {
    if (writeMemoryBufferMaxConcurrentWrites.availablePermits() <= 0
        && LOG.isInfoEnabled()) {
      LOG.info(
          "Max concurrent write buffers reached (increase {}); blocking incoming requests..",
          DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_MAX_CAPACITY_MB);
    }
    try {
      writeMemoryBufferMaxConcurrentWrites.acquire();
      permitAcquired = true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.info("Interrupted while acquiring write-buffer permit for {}",
          blockName);
    }
  }

  private void acquireFlushPermit() {
    if (flushWritesSemaphore == null) {
      return;
    }
    if (flushWritesSemaphore.availablePermits() <= 0 && LOG.isDebugEnabled()) {
      LOG.debug("Restricting flush concurrency on {}", volume.getBaseURI());
    }
    try {
      flushWritesSemaphore.acquire();
    } catch (InterruptedException e) {
      LOG.info("Interrupted while acquiring flush permit on volume={}",
          volume.getBaseURI());
      Thread.currentThread().interrupt();
    }
  }

  private void releaseFlushPermit() {
    if (flushWritesSemaphore != null) {
      flushWritesSemaphore.release();
    }
  }

  @Override
  public long getFlushedBytes() {
    return totalFlushedBytes;
  }
}
