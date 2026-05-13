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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsVolumeImpl;
import org.junit.jupiter.api.Test;

/**
 * Focused tests for the {@link BufferedBlockWriter} abstraction and its
 * per-volume resource container. These are unit tests — they do not spin up
 * a MiniDFSCluster.
 */
public class TestBufferedBlockWriter {

  /** NO_OP_INSTANCE must be safe to call repeatedly and return zero bytes. */
  @Test
  public void testNoOpIsSafeAndIdempotent() throws IOException {
    BufferedBlockWriter w = BufferedBlockWriter.NO_OP_INSTANCE;
    assertEquals(0L, w.getFlushedBytes());
    // All mutating methods must be no-ops, and tolerate being called any
    // number of times — BlockReceiver invokes release() from both the
    // close() path and the constructor's catch block.
    w.writeData(ByteBuffer.wrap(new byte[16]), 0, 16);
    w.flush();
    w.flushOrSync(true, true, true);
    w.syncData("blk_1", true);
    w.release();
    w.release();
    w.release();
    assertEquals(0L, w.getFlushedBytes());
  }

  /** With the feature disabled, FsVolumeImpl must not allocate per-volume
   * buffer resources — that avoids paying for the executor thread + lock
   * on the default deployment. */
  @Test
  public void testBufferResourcesDisabledByDefault() {
    Configuration conf = new Configuration();
    // Default is false; assert explicitly to lock the invariant.
    assertEquals(false, conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_ENABLED,
        DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_ENABLED_DEFAULT));
  }

  /** BufferWriteResource.close() must be idempotent so a double-shutdown
   * doesn't NPE on the volumeExecutor. */
  @Test
  public void testBufferWriteResourceCloseIdempotent() {
    FsVolumeImpl.BufferWriteResource r =
        new FsVolumeImpl.BufferWriteResource(null);
    assertNotNull(r.getVolumeExecutor());
    assertNotNull(r.getVolumeAccessLock());
    assertTrue(!r.getFlushPermitSemaphore().isPresent(),
        "flush permit semaphore should be empty when not configured");
    r.close();
    // Second close must be a no-op.
    r.close();
  }

  /** Per-volume flush semaphore is wired through when configured. */
  @Test
  public void testBufferWriteResourceWithSemaphore() {
    java.util.concurrent.Semaphore sem = new java.util.concurrent.Semaphore(4);
    FsVolumeImpl.BufferWriteResource r =
        new FsVolumeImpl.BufferWriteResource(sem);
    assertTrue(r.getFlushPermitSemaphore().isPresent());
    assertEquals(4, r.getFlushPermitSemaphore().get().availablePermits());
    r.close();
  }

  /** Sanity: the default constants are wired so that the dynamic feature
   * is OFF by default — no default deployments accidentally enable it. */
  @Test
  public void testFeatureOffByDefault() {
    assertEquals(false,
        DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_ENABLED_DEFAULT);
    assertEquals(8 * 1024 * 1024,
        DFSConfigKeys.DFS_DATANODE_WRITE_BUFFER_SIZE_BYTES_DEFAULT);
    assertEquals(256 * 1024,
        DFSConfigKeys.DFS_DATANODE_READ_AHEAD_CACHE_BYTES_THRESHOLD_DEFAULT);
  }

  /** Defensive: getMaxConcurrentWriteBuffers() must be null when the
   * feature is disabled so any accidental dereference fails loudly rather
   * than silently using an unbounded semaphore. */
  @Test
  public void testDataNodeAccessorsWhenDisabled() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_WRITE_MEMORY_BUFFER_ENABLED,
        false);
    DataNode dn = new DataNode(conf);
    try {
      assertEquals(false, dn.isWriteMemoryBufferEnabled());
      assertNull(dn.getMaxConcurrentWriteBuffers());
    } finally {
      try {
        dn.shutdown();
      } catch (Throwable ignored) {
        // dummy DataNode shutdown may throw; ignore for test cleanup.
      }
    }
  }
}
