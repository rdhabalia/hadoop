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
package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintStream;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeAffinityManager;
import org.apache.hadoop.hdfs.server.blockmanagement.FileDatanodeAffinityManager;
import org.apache.hadoop.hdfs.tools.DFSck;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the new {@code hdfs fsck -favored-nodes} flag wired to
 * {@link DatanodeAffinityManager}.
 */
public class TestFsckFavoredNodes {

  private MiniDFSCluster cluster;
  private Configuration conf;
  private File affinityFile;

  @BeforeEach
  public void setUp() throws Exception {
    conf = new HdfsConfiguration();
    affinityFile = File.createTempFile("affinity-fsck", ".json",
        GenericTestUtils.getTestDir());
    affinityFile.deleteOnExit();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
    if (affinityFile != null) {
      affinityFile.delete();
    }
  }

  /**
   * When an affinity rule matches the file path, {@code -favored-nodes}
   * must print the resolved {@code "host:port"} list.
   */
  @Test
  public void testFsckFavoredNodesMatchingRule() throws Exception {
    try (FileWriter fw = new FileWriter(affinityFile)) {
      fw.write("[{\"affinityGroupName\":\"test-group\","
          + "\"regexPattern\":\"^/hcl-data/.*\","
          + "\"datanodeRegex\":\".*\"}]");
    }

    conf.set(DFSConfigKeys.DFS_DATANODE_AFFINITY_MANAGER_CLASSNAME_KEY,
        FileDatanodeAffinityManager.class.getName());
    conf.set(DFSConfigKeys.DFS_DATANODE_AFFINITY_FILE_PATH_KEY,
        affinityFile.getAbsolutePath());

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();

    DatanodeAffinityManager affinityMgr = cluster.getNameNode().getNamesystem()
        .getBlockManager().getDatanodeManager().getDatanodeAffinityManager();
    assertNotNull(affinityMgr,
        "DatanodeAffinityManager must be configured");
    affinityMgr.refresh();

    FileSystem fs = cluster.getFileSystem();
    DFSTestUtil.createFile(fs, new Path("/hcl-data/test.parquet"),
        512, (short) 3, 0L);

    String out = runFsck("/hcl-data", "-files", "-favored-nodes");
    assertTrue(out.contains("Affinity favored nodes"),
        "Output must contain 'Affinity favored nodes'; got=\n" + out);
    assertFalse(out.contains("Affinity favored nodes: none"),
        "Favored nodes must not be empty when rule matches");
  }

  /**
   * Dry-run path: {@code -favored-nodes} on a non-existent HDFS path must
   * still resolve and print the affinity DataNodes (no file-system lookup
   * required).
   */
  @Test
  public void testFsckFavoredNodesNonExistentPathDryRun() throws Exception {
    try (FileWriter fw = new FileWriter(affinityFile)) {
      fw.write("[{\"affinityGroupName\":\"test-group\","
          + "\"regexPattern\":\"^/hcl-data/.*\","
          + "\"datanodeRegex\":\".*\"}]");
    }

    conf.set(DFSConfigKeys.DFS_DATANODE_AFFINITY_MANAGER_CLASSNAME_KEY,
        FileDatanodeAffinityManager.class.getName());
    conf.set(DFSConfigKeys.DFS_DATANODE_AFFINITY_FILE_PATH_KEY,
        affinityFile.getAbsolutePath());

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();

    DatanodeAffinityManager affinityMgr = cluster.getNameNode().getNamesystem()
        .getBlockManager().getDatanodeManager().getDatanodeAffinityManager();
    affinityMgr.refresh();

    String out = runFsck("/hcl-data/never-existed.bin", "-favored-nodes");
    assertTrue(out.contains("Affinity favored nodes"),
        "Dry-run on non-existent path must still print favored nodes; got=\n"
            + out);
    assertTrue(out.contains("HEALTHY"),
        "Dry-run status must be HEALTHY; got=\n" + out);
  }

  /**
   * With no affinity manager configured, {@code -favored-nodes} must report
   * the absence rather than crash.
   */
  @Test
  public void testFsckFavoredNodesNoAffinityManager() throws Exception {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();

    FileSystem fs = cluster.getFileSystem();
    DFSTestUtil.createFile(fs, new Path("/some.bin"), 512, (short) 1, 0L);

    String out = runFsck("/some.bin", "-files", "-favored-nodes");
    assertTrue(out.contains("DatanodeAffinityManager not configured")
            || out.contains("Affinity favored nodes: none"),
        "Output must indicate no affinity is configured; got=\n" + out);
  }

  private String runFsck(String... args) throws Exception {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    PrintStream stdout = System.out;
    System.setOut(new PrintStream(bout));
    try {
      DFSck fsck = new DFSck(conf, new PrintStream(bout));
      ToolRunner.run(fsck, args);
    } finally {
      System.setOut(stdout);
    }
    return bout.toString();
  }
}
