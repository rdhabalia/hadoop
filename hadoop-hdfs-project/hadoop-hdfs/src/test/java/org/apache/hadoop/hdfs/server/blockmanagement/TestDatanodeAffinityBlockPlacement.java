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
package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests that verify {@link DatanodeAffinityManager} drives
 * block placement to the correct DataNodes in a MiniDFSCluster.
 *
 * <p>Three scenarios:
 * <ol>
 *   <li>Files under the affinity directory land exclusively on the
 *       affinity DataNodes.</li>
 *   <li>Files outside the affinity directory use the non-affinity
 *       DataNodes (the affinity DNs are removed from the default topology
 *       so they're never picked).</li>
 *   <li>Topology isolation is enforced even for paths that do not match
 *       the affinity regex.</li>
 * </ol>
 */
public class TestDatanodeAffinityBlockPlacement {

  private static final int NUM_DATANODES = 10;
  private static final int AFFINITY_COUNT = 3;
  private static final short REPLICATION = 3;
  private static final int NUM_FILES = 5;
  private static final int FILE_SIZE = 1024;
  private static final String AFFINITY_DIR = "/hcl-data";
  private static final String OTHER_DIR = "/other-data";

  private MiniDFSCluster cluster;
  private Configuration conf;
  private File affinityJsonFile;

  @BeforeEach
  public void setUp() throws Exception {
    conf = new Configuration();
    affinityJsonFile = File.createTempFile("dn-affinity-e2e", ".json",
        GenericTestUtils.getTestDir());
    affinityJsonFile.deleteOnExit();
    writeJson("[]");

    conf.set(DFSConfigKeys.DFS_DATANODE_AFFINITY_MANAGER_CLASSNAME_KEY,
        FileDatanodeAffinityManager.class.getName());
    conf.set(DFSConfigKeys.DFS_DATANODE_AFFINITY_FILE_PATH_KEY,
        affinityJsonFile.getAbsolutePath());

    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(NUM_DATANODES)
        .build();
    cluster.waitActive();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
    if (affinityJsonFile != null) {
      affinityJsonFile.delete();
    }
  }

  @Test
  public void testBlocksPlacedOnlyOnAffinityDatanodes() throws Exception {
    DatanodeManager dnManager = cluster.getNameNode().getNamesystem()
        .getBlockManager().getDatanodeManager();

    List<DatanodeDescriptor> allDNs =
        new ArrayList<>(dnManager.getAllDatanodes());
    assertEquals(NUM_DATANODES, allDNs.size());
    allDNs.sort(Comparator.comparingInt(DatanodeDescriptor::getXferPort));

    List<DatanodeDescriptor> affinityDNs = allDNs.subList(0, AFFINITY_COUNT);
    List<DatanodeDescriptor> nonAffinityDNs =
        allDNs.subList(AFFINITY_COUNT, allDNs.size());

    Set<String> affinityAddrs = affinityDNs.stream()
        .map(DatanodeDescriptor::getXferAddr)
        .collect(Collectors.toSet());
    Set<String> nonAffinityAddrs = nonAffinityDNs.stream()
        .map(DatanodeDescriptor::getXferAddr)
        .collect(Collectors.toSet());

    assertEquals(AFFINITY_COUNT, affinityAddrs.size());
    assertEquals(NUM_DATANODES - AFFINITY_COUNT, nonAffinityAddrs.size());

    String datanodeRegex = affinityAddrs.stream()
        .map(addr -> "(" + escapeForRegex(addr) + ")")
        .collect(Collectors.joining("|"));

    writeJson(buildAffinityJson("hcl-group",
        "^" + AFFINITY_DIR + "/.*", datanodeRegex));

    DatanodeAffinityManager affinityManager =
        dnManager.getDatanodeAffinityManager();
    assertNotNull(affinityManager,
        "DatanodeAffinityManager must be configured");
    affinityManager.refresh();

    List<String> resolved = affinityManager.getFileRegexToDataNodeMap()
        .get("^" + AFFINITY_DIR + "/.*");
    assertNotNull(resolved,
        "Affinity map must contain entry for " + AFFINITY_DIR);
    assertEquals(AFFINITY_COUNT, resolved.size(),
        "Affinity map must list exactly " + AFFINITY_COUNT + " DataNodes");
    assertTrue(new HashSet<>(resolved).equals(affinityAddrs),
        "Resolved addresses must equal affinityAddrs");

    DistributedFileSystem dfs = cluster.getFileSystem();
    List<Path> createdFiles = new ArrayList<>();
    for (int i = 0; i < NUM_FILES; i++) {
      Path p = new Path(AFFINITY_DIR + "/file-" + i + ".parquet");
      DFSTestUtil.createFile(dfs, p, FILE_SIZE, REPLICATION, i);
      createdFiles.add(p);
    }
    for (Path p : createdFiles) {
      DFSTestUtil.waitReplication(dfs, p, REPLICATION);
    }

    for (Path filePath : createdFiles) {
      long fileLen = dfs.getFileStatus(filePath).getLen();
      LocatedBlocks locatedBlocks = dfs.getClient()
          .getLocatedBlocks(filePath.toString(), 0, fileLen);

      assertFalse(locatedBlocks.getLocatedBlocks().isEmpty(),
          "File " + filePath + " has no blocks");
      for (LocatedBlock lb : locatedBlocks.getLocatedBlocks()) {
        DatanodeInfo[] locations = lb.getLocations();
        assertEquals(REPLICATION, locations.length,
            "Block must have " + REPLICATION + " replicas");
        for (DatanodeInfo dn : locations) {
          String hostPort = dn.getXferAddr();
          assertFalse(nonAffinityAddrs.contains(hostPort),
              "Block replica found on non-affinity node " + hostPort
                  + " for file " + filePath);
          assertTrue(affinityAddrs.contains(hostPort),
              "Block replica " + hostPort + " not in affinity set for "
                  + filePath);
        }
      }
    }
  }

  @Test
  public void testBlocksOutsideAffinityDirUseNonAffinityDatanodes()
      throws Exception {
    DatanodeManager dnManager = cluster.getNameNode().getNamesystem()
        .getBlockManager().getDatanodeManager();

    List<DatanodeDescriptor> allDNs =
        new ArrayList<>(dnManager.getAllDatanodes());
    allDNs.sort(Comparator.comparingInt(DatanodeDescriptor::getXferPort));
    List<DatanodeDescriptor> affinityDNs = allDNs.subList(0, AFFINITY_COUNT);

    Set<String> affinityAddrs = affinityDNs.stream()
        .map(DatanodeDescriptor::getXferAddr)
        .collect(Collectors.toSet());
    String datanodeRegex = affinityAddrs.stream()
        .map(addr -> "(" + escapeForRegex(addr) + ")")
        .collect(Collectors.joining("|"));

    writeJson(buildAffinityJson("hcl-group",
        "^" + AFFINITY_DIR + "/.*", datanodeRegex));
    dnManager.getDatanodeAffinityManager().refresh();

    DistributedFileSystem dfs = cluster.getFileSystem();
    Set<String> seenAddrs = new HashSet<>();
    for (int i = 0; i < 20; i++) {
      Path p = new Path(OTHER_DIR + "/file-" + i + ".parquet");
      DFSTestUtil.createFile(dfs, p, FILE_SIZE, REPLICATION, i);
      DFSTestUtil.waitReplication(dfs, p, REPLICATION);

      long fileLen = dfs.getFileStatus(p).getLen();
      LocatedBlocks lb =
          dfs.getClient().getLocatedBlocks(p.toString(), 0, fileLen);
      for (LocatedBlock block : lb.getLocatedBlocks()) {
        for (DatanodeInfo dn : block.getLocations()) {
          seenAddrs.add(dn.getXferAddr());
        }
      }
    }

    // No isolated DataNode must have received any non-affinity replica.
    Set<String> intersection = new HashSet<>(seenAddrs);
    intersection.retainAll(affinityAddrs);
    assertTrue(intersection.isEmpty(),
        "Isolated DataNodes must NOT receive blocks for files outside "
            + AFFINITY_DIR + "; seenAddrs=" + seenAddrs
            + ", affinityAddrs=" + affinityAddrs);
  }

  /**
   * Helper: write the JSON contents to the affinity file.
   */
  private void writeJson(String content) throws Exception {
    try (FileWriter fw = new FileWriter(affinityJsonFile)) {
      fw.write(content);
    }
  }

  private static String buildAffinityJson(String groupName,
      String fileRegex, String datanodeRegex) {
    String escapedFileRegex = fileRegex.replace("\\", "\\\\");
    String escapedDnRegex = datanodeRegex.replace("\\", "\\\\");
    return String.format(
        "[{\"affinityGroupName\":\"%s\","
            + "\"regexPattern\":\"%s\","
            + "\"datanodeRegex\":\"%s\"}]",
        groupName, escapedFileRegex, escapedDnRegex);
  }

  /**
   * Escape a literal string so it is safe to embed as a Java regex.
   * Replaces {@code .} and {@code +} which are the only special regex
   * characters that appear in {@code "127.0.0.1:PORT"} strings.
   */
  private static String escapeForRegex(String literal) {
    return literal.replace(".", "\\.").replace("+", "\\+");
  }
}
