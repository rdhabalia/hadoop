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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jna.LastErrorException;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

/**
 * Experimental O_DIRECT variant of {@link BufferedBlockWriter}. Bypasses the
 * Linux page cache for aligned segments and falls back to a DSYNC
 * {@link FileChannel} for the trailing un-aligned tail of a block.
 *
 * <p>Disabled by default; enable with
 * {@code dfs.datanode.write.o.direct.enabled=true}. Requires Linux + JNA.</p>
 */
public class DirectIOBufferedBlockWriter implements BufferedBlockWriter {

  public static final Logger LOG =
      LoggerFactory.getLogger(DirectIOBufferedBlockWriter.class);
  /** Alignment for O_DIRECT (file offset, buffer address, and length). */
  private static final int BLOCK_SIZE = 4 * 1024;
  /** Per-writer Netty buffer capacity. */
  public static final int BUFFER_CAPACITY = 8 * 1024 * 1024;

  private final FsVolumeImpl volume;
  private ByteBuf nettyBuf;
  private final BlockReceiver blockReceiver;
  private final String blockName;
  private final Semaphore writeMemoryBufferMaxConcurrentWrites;
  private final int directFd;
  private final FileChannel ioChannel;
  private long fileOffset = 0L;
  private volatile long totalFlushBytes = 0L;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private boolean permitAcquired = false;

  /** JNA bridge for libc I/O. */
  public interface CLibrary extends Library {
    CLibrary INSTANCE = Native.load(Platform.C_LIBRARY_NAME, CLibrary.class);

    int open(String path, int flags, int mode) throws LastErrorException;

    int write(int fd, Pointer buf, int count) throws LastErrorException;

    int fsync(int fd);

    int close(int fd);

    int posix_memalign(PointerByReference memptr, long alignment, long size);

    void free(Pointer ptr);
  }

  /** Linux open flags + mode bits. */
  public static final class NativeOpen {
    private static final int O_CREAT = 0100;
    private static final int O_WRONLY = 01;
    private static final int O_TRUNC = 01000;
    private static final int O_DIRECT = 040000;
    private static final int S_IRUSR = 00400;
    private static final int S_IWUSR = 00200;

    private NativeOpen() {
    }

    public static int openO_DIRECT(String path) {
      return CLibrary.INSTANCE.open(path,
          O_CREAT | O_WRONLY | O_TRUNC | O_DIRECT, S_IRUSR | S_IWUSR);
    }

    public static int close(int fd) {
      return CLibrary.INSTANCE.close(fd);
    }

    public static int fsync(int fd) {
      return CLibrary.INSTANCE.fsync(fd);
    }
  }

  public DirectIOBufferedBlockWriter(BlockReceiver blockReceiver, File file,
      FsVolumeImpl volume, Semaphore writeMemoryBufferMaxConcurrentWrites)
      throws IOException {
    this.volume = volume;
    this.blockReceiver = blockReceiver;
    this.blockName = blockReceiver.getBlock().getBlockName();
    this.writeMemoryBufferMaxConcurrentWrites =
        writeMemoryBufferMaxConcurrentWrites;

    int fd = -1;
    FileChannel chan = null;
    ByteBuf buf = null;
    boolean ok = false;
    try {
      acquirePermit();
      buf = PooledByteBufAllocator.DEFAULT.buffer(BUFFER_CAPACITY);
      fd = NativeOpen.openO_DIRECT(file.getAbsolutePath());
      if (fd < 0) {
        throw new IOException(
            "open() with O_DIRECT failed for " + file.getAbsolutePath());
      }
      // Fallback DSYNC channel for small or misaligned writes; positioned
      // after the bytes we wrote via O_DIRECT.
      chan = FileChannel.open(file.toPath(), StandardOpenOption.CREATE,
          StandardOpenOption.WRITE, StandardOpenOption.DSYNC);
      chan.position(fileOffset);
      this.nettyBuf = buf;
      this.directFd = fd;
      this.ioChannel = chan;
      ok = true;
    } finally {
      if (!ok) {
        if (chan != null) {
          try { chan.close(); } catch (IOException ignored) { /* best-effort */ }
        }
        if (fd >= 0) {
          NativeOpen.close(fd);
        }
        if (buf != null) {
          buf.release();
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
        flushOrSync(true, true, false);
      }
    }
  }

  @Override
  public synchronized void flush() throws IOException {
    int readable = nettyBuf == null ? 0 : nettyBuf.readableBytes();
    if (readable == 0) {
      return;
    }
    Object volumeLock = volume.getBufferResources() != null
        ? volume.getBufferResources().getVolumeAccessLock() : this;
    synchronized (volumeLock) {
      int alignedLen = (readable / BLOCK_SIZE) * BLOCK_SIZE;
      if (alignedLen > 0) {
        writeDirect(nettyBuf, alignedLen);
        nettyBuf.skipBytes(alignedLen);
      }
      if (nettyBuf.readableBytes() > 0) {
        writeNonAlignedData(nettyBuf);
      }
      totalFlushBytes += readable;
      nettyBuf.clear();
    }
  }

  @Override
  public void syncData(String blockName, boolean isClosed) {
    if (!isClosed) {
      return;
    }
    Object volumeLock = volume.getBufferResources() != null
        ? volume.getBufferResources().getVolumeAccessLock() : this;
    synchronized (volumeLock) {
      try {
        NativeOpen.fsync(directFd);
        ioChannel.force(true);
      } catch (Exception e) {
        LOG.warn("Failed to fsync {}", this.blockName, e);
      }
    }
  }

  @Override
  public void flushOrSync(boolean fsync, boolean bufferFlush, boolean isClosed)
      throws IOException {
    blockReceiver.flushOrSync(fsync,
        isClosed /* fsync checksum during closing the block */, bufferFlush,
        isClosed);
  }

  @Override
  public synchronized void release() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    try {
      NativeOpen.close(directFd);
    } catch (Throwable t) {
      LOG.warn("Failed to close O_DIRECT fd for {}", blockName, t);
    }
    try {
      ioChannel.close();
    } catch (Throwable t) {
      LOG.warn("Failed to close DSYNC channel for {}", blockName, t);
    }
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

  /**
   * Copy {@code length} bytes (must be a multiple of {@link #BLOCK_SIZE})
   * from {@code src} into a 4K-aligned native buffer and write to the
   * O_DIRECT file descriptor.
   */
  private void writeDirect(ByteBuf src, int length) throws IOException {
    if (length <= 0 || (length % BLOCK_SIZE) != 0) {
      throw new IOException(
          "O_DIRECT write length must be a positive multiple of " + BLOCK_SIZE
              + " but got " + length);
    }
    PointerByReference memptr = new PointerByReference();
    int rc = CLibrary.INSTANCE.posix_memalign(memptr, BLOCK_SIZE, length);
    if (rc != 0) {
      throw new IOException("posix_memalign failed rc=" + rc);
    }
    Pointer alignedPtr = memptr.getValue();
    if (alignedPtr == null) {
      throw new IOException("posix_memalign returned null pointer");
    }
    try {
      ByteBuffer alignedBuf = alignedPtr.getByteBuffer(0, length);
      src.getBytes(src.readerIndex(), alignedBuf);
      int written;
      try {
        written = CLibrary.INSTANCE.write(directFd, alignedPtr, length);
      } catch (LastErrorException le) {
        throw new IOException(
            "O_DIRECT write failed errno=" + le.getErrorCode(), le);
      }
      if (written < 0) {
        throw new IOException(
            "O_DIRECT write failed, errno=" + Native.getLastError());
      }
      if (written != length) {
        throw new IOException(
            "O_DIRECT short write: expected=" + length + " got=" + written);
      }
      fileOffset += length;
    } finally {
      CLibrary.INSTANCE.free(alignedPtr);
    }
  }

  /** Write a partial / non-4K-aligned tail via the DSYNC FileChannel. */
  private void writeNonAlignedData(ByteBuf src) throws IOException {
    int count = src.readableBytes();
    // Skip past the O_DIRECT bytes we already wrote on this volume.
    ioChannel.position(fileOffset);
    ByteBuffer buffer = ByteBuffer.allocate(count);
    src.readBytes(buffer);
    buffer.flip();
    while (buffer.hasRemaining()) {
      ioChannel.write(buffer);
    }
    fileOffset += count;
  }

  private void acquirePermit() {
    if (writeMemoryBufferMaxConcurrentWrites.availablePermits() <= 0
        && LOG.isInfoEnabled()) {
      LOG.info(
          "Max concurrent write buffers reached (increase {}); blocking..",
          DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_MAX_CAPACITY_MB);
    }
    try {
      writeMemoryBufferMaxConcurrentWrites.acquire();
      permitAcquired = true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.info("Interrupted while acquiring O_DIRECT permit for {}", blockName);
    }
  }

  @Override
  public long getFlushedBytes() {
    return totalFlushBytes;
  }
}
