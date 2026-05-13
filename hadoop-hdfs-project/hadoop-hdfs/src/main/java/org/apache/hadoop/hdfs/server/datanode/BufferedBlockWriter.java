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

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Abstraction for a reusable, memory-backed buffer used in the DataNode write
 * pipeline. Implementations accumulate small packet writes into a larger
 * in-memory segment and flush the segment to disk in one shot, reducing the
 * number of random write IOPs and freeing the page cache for reads.
 *
 * <p>Implementations may back the buffer with a pooled off-heap (Netty)
 * allocator and flush via a DSYNC FileChannel or O_DIRECT, depending on
 * configuration.</p>
 *
 * <p>The lifecycle is:
 * <ol>
 *   <li>{@link #writeData} called for each packet — may auto-flush when full.</li>
 *   <li>{@link #flush} / {@link #flushOrSync} called by the receiver on hsync /
 *       hflush / close to drain the buffer.</li>
 *   <li>{@link #syncData} called on close to fsync the file.</li>
 *   <li>{@link #release} releases buffer memory and permits.</li>
 * </ol>
 * </p>
 */
public interface BufferedBlockWriter {

  /** Shared no-op instance used when buffering is disabled or not applicable. */
  BufferedBlockWriter NO_OP_INSTANCE = new BufferedBlockWriterNoOp();

  /**
   * Append data to the in-memory buffer. Implementations may auto-flush the
   * buffer to disk if it fills up.
   *
   * @param dataBuf         source buffer
   * @param startByteToDisk offset within {@code dataBuf}
   * @param numBytesToDisk  number of bytes to append
   */
  void writeData(ByteBuffer dataBuf, int startByteToDisk, int numBytesToDisk)
      throws IOException;

  /**
   * Persist any in-memory buffered data to disk. Does not fsync — durability
   * is provided by the underlying channel mode (e.g. DSYNC) or an explicit
   * {@link #syncData(String, boolean)}.
   */
  void flush() throws IOException;

  /**
   * Fsync the underlying file. May be a no-op for some implementations when
   * the block is still being received; the receiver should pass
   * {@code isClosed=true} when called from the close path.
   *
   * @param blockName the HDFS block name (used for logging only)
   * @param isClosed  whether this is the final sync on close
   */
  void syncData(String blockName, boolean isClosed);

  /**
   * Convenience entry point that routes a flush/sync request back through
   * the owning {@link BlockReceiver} so the checksum stream and data stream
   * are kept consistent.
   */
  void flushOrSync(boolean fsync, boolean bufferFlush, boolean isClosed)
      throws IOException;

  /**
   * Release pooled memory, semaphore permits, and the underlying file
   * descriptor. Must be idempotent — the receiver may invoke this from
   * multiple cleanup paths on failure.
   */
  void release();

  /** @return total bytes that have been flushed to disk so far. */
  long getFlushedBytes();

  /**
   * No-op implementation used when buffering is disabled, or when the
   * replica is not backed by a real on-disk volume (for example
   * RAM_DISK / lazy-persist replicas). Keeps the call sites in
   * {@link BlockReceiver} branch-free.
   */
  class BufferedBlockWriterNoOp implements BufferedBlockWriter {

    @Override
    public void writeData(ByteBuffer dataBuf, int startByteToDisk,
        int numBytesToDisk) {
    }

    @Override
    public void flush() {
    }

    @Override
    public void syncData(String blockName, boolean isClosed) {
    }

    @Override
    public void flushOrSync(boolean fsync, boolean bufferFlush,
        boolean isClosed) {
    }

    @Override
    public void release() {
    }

    @Override
    public long getFlushedBytes() {
      return 0L;
    }
  }
}
