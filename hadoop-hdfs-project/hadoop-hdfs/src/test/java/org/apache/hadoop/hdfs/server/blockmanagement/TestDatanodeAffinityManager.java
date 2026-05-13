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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.zaxxer.hikari.HikariConfig;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link MysqlDatanodeAffinityManager} and
 * {@link FileDatanodeAffinityManager}.
 *
 * <p>SQL tests use an in-memory Derby database (no real MySQL required).
 * File tests write a temporary JSON file under the test directory.
 */
public class TestDatanodeAffinityManager {

  private static final String JDBC_URL =
      "jdbc:derby:memory:affinityTestDb;create=true";
  private static final String TABLE = "dn_affinity";

  /** Cluster DataNodes provided by the mock {@link DatanodeManager}. */
  private static final String[] CLUSTER_HOSTS =
      { "worker-hcl64372.grid.example.com",
          "worker-hcl72.grid.example.com",
          "worker-hcl63472.grid.example.com",
          "worker-gpu01.grid.example.com",
          "worker-cpu01.grid.example.com" };

  private static final int XFER_PORT = 9866;

  /** Regex matching the three HCL workers against "hostname:port". */
  private static final String HCL_DN_REGEX =
      "^worker-hcl(64372|72|63472)\\.grid\\.example\\.com:";

  /** Regex matching ALL worker-hcl nodes regardless of numeric suffix. */
  private static final String HCL_ALL_DN_REGEX =
      "^worker-hcl.*\\.grid\\.example\\.com:";

  private MysqlDatanodeAffinityManager sqlImpl;
  private Connection conn;

  private FileDatanodeAffinityManager fileImpl;
  private File affinityFile;

  @BeforeEach
  public void setUp() throws Exception {
    setUpSQL();
    setUpFile();
  }

  private void setUpSQL() throws Exception {
    conn = DriverManager.getConnection(JDBC_URL);
    try (Statement st = conn.createStatement()) {
      st.executeUpdate("CREATE TABLE " + TABLE + " ("
          + "affinity_group_name VARCHAR(255) NOT NULL, "
          + "file_regex          VARCHAR(1024) NOT NULL, "
          + "datanode_regex      VARCHAR(2048) NOT NULL, "
          + "PRIMARY KEY (affinity_group_name, file_regex))");
    } catch (SQLException e) {
      if (!"X0Y32".equals(e.getSQLState())) {
        throw e;
      }
      try (Statement st = conn.createStatement()) {
        st.executeUpdate("DELETE FROM " + TABLE);
      }
    }

    sqlImpl = Mockito.spy(new MysqlDatanodeAffinityManager());
    Mockito.doNothing().when(sqlImpl).createTableIfNotExists();
    Mockito.doNothing().when(sqlImpl)
        .setQueryTimeout(Mockito.any(java.sql.PreparedStatement.class));
    Mockito.doNothing().when(sqlImpl)
        .setNetworkTimeout(Mockito.any(java.sql.Connection.class));

    HikariConfig hikari = new HikariConfig();
    hikari.setJdbcUrl(JDBC_URL);
    hikari.setDriverClassName("org.apache.derby.jdbc.EmbeddedDriver");
    hikari.setUsername("");
    hikari.setPassword("");
    hikari.setMaximumPoolSize(5);
    sqlImpl.setConf(buildSQLConf());
    sqlImpl.initDataSource(hikari);
    sqlImpl.setDatanodeManager(buildMockDatanodeManager());
  }

  private void setUpFile() throws Exception {
    File testDir = GenericTestUtils.getTestDir();
    affinityFile = new File(testDir, "dn_affinity.json");
    affinityFile.deleteOnExit();

    Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_DATANODE_AFFINITY_FILE_PATH_KEY,
        affinityFile.getAbsolutePath());

    fileImpl = new FileDatanodeAffinityManager();
    fileImpl.setConf(conf);
    fileImpl.setDatanodeManager(buildMockDatanodeManager());
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (conn != null) {
      conn.close();
    }
    try {
      DriverManager
          .getConnection("jdbc:derby:memory:affinityTestDb;shutdown=true");
    } catch (SQLException ignored) {
      // Derby always throws on shutdown.
    }
    if (affinityFile != null) {
      affinityFile.delete();
    }
  }

  private Configuration buildSQLConf() {
    Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_DATANODE_AFFINITY_MYSQL_URL, JDBC_URL);
    conf.set(DFSConfigKeys.DFS_DATANODE_AFFINITY_MYSQL_TABLE, TABLE);
    conf.set(DFSConfigKeys.DFS_DATANODE_AFFINITY_MYSQL_USERNAME, "");
    conf.set(DFSConfigKeys.DFS_DATANODE_AFFINITY_MYSQL_DRIVER,
        "org.apache.derby.jdbc.EmbeddedDriver");
    conf.setInt(DFSConfigKeys.DFS_DATANODE_AFFINITY_MYSQL_NETWORK_TIMEOUT_MS, 0);
    conf.setInt(DFSConfigKeys.DFS_DATANODE_AFFINITY_MYSQL_QUERY_TIMEOUT_MS, 0);
    return conf;
  }

  /**
   * Build a mock DatanodeManager whose getAllDatanodes() returns one
   * DatanodeDescriptor per CLUSTER_HOSTS entry.
   */
  private DatanodeManager buildMockDatanodeManager() {
    DatanodeManager dm = Mockito.mock(DatanodeManager.class);
    List<DatanodeDescriptor> descriptors = new ArrayList<>();
    for (String host : CLUSTER_HOSTS) {
      DatanodeDescriptor dd = Mockito.mock(DatanodeDescriptor.class);
      Mockito.when(dd.getHostName()).thenReturn(host);
      Mockito.when(dd.getXferPort()).thenReturn(XFER_PORT);
      Mockito.when(dd.getXferAddr()).thenReturn(host + ":" + XFER_PORT);
      Mockito.when(dd.getXferAddrWithHostname())
          .thenReturn(host + ":" + XFER_PORT);
      descriptors.add(dd);
    }
    Mockito.when(dm.getAllDatanodes()).thenReturn(descriptors);
    return dm;
  }

  private static String addr(String hostname) {
    return hostname + ":" + XFER_PORT;
  }

  private void insertSQLRow(String groupName, String fileRegex,
      String datanodeRegex) throws SQLException {
    try (Statement st = conn.createStatement()) {
      st.executeUpdate(String.format(
          "INSERT INTO %s (affinity_group_name, file_regex, datanode_regex) "
              + "VALUES ('%s', '%s', '%s')",
          TABLE, groupName, fileRegex, datanodeRegex));
    }
  }

  private void writeAffinityJson(String json) throws IOException {
    try (FileWriter fw = new FileWriter(affinityFile)) {
      fw.write(json);
    }
  }

  private String singleGroupJson(String groupName, String regexPattern,
      String datanodeRegex) {
    return String.format(
        "[{\"affinityGroupName\":\"%s\",\"regexPattern\":\"%s\","
            + "\"datanodeRegex\":\"%s\"}]",
        groupName, regexPattern, datanodeRegex);
  }

  /**
   * Replicates what the original PR's removed getFavoredDatanodes() did:
   * iterate the map, collect nodes from every regex key that matches the
   * given path.
   */
  private static List<String> nodesForPath(DatanodeAffinityManager mgr,
      String path) {
    List<String> result = new ArrayList<>();
    for (Map.Entry<String, List<String>> entry :
        mgr.getFileRegexToDataNodeMap().entrySet()) {
      if (Pattern.compile(entry.getKey()).matcher(path).find()) {
        for (String s : entry.getValue()) {
          if (!result.contains(s)) {
            result.add(s);
          }
        }
      }
    }
    return Collections.unmodifiableList(result);
  }

  // ==========================================================================
  // MysqlDatanodeAffinityManager — inject tests
  // ==========================================================================

  @Test
  public void testSQLImplInjectMapMatchingPath() {
    Map<String, List<String>> map = new LinkedHashMap<>();
    map.put("^/data/hcl/.*",
        Arrays.asList(addr("worker-hcl64372.grid.example.com"),
            addr("worker-hcl72.grid.example.com"),
            addr("worker-hcl63472.grid.example.com")));
    sqlImpl.setFileRegexToDataNodeMap(map);

    List<String> result = nodesForPath(sqlImpl, "/data/hcl/file.parquet");
    assertEquals(3, result.size());
    assertTrue(result.contains(addr("worker-hcl64372.grid.example.com")));
    assertTrue(result.contains(addr("worker-hcl72.grid.example.com")));
    assertTrue(result.contains(addr("worker-hcl63472.grid.example.com")));
  }

  @Test
  public void testSQLImplInjectMapNonMatchingPath() {
    Map<String, List<String>> map = new LinkedHashMap<>();
    map.put("^/data/hcl/.*",
        Collections.singletonList(addr("worker-hcl64372.grid.example.com")));
    sqlImpl.setFileRegexToDataNodeMap(map);

    assertTrue(nodesForPath(sqlImpl, "/other/path/file.orc").isEmpty());
  }

  @Test
  public void testSQLImplInjectMapMultipleKeysOnlyMatchReturned() {
    Map<String, List<String>> map = new LinkedHashMap<>();
    map.put("^/data/hcl/.*",
        Arrays.asList(addr("worker-hcl64372.grid.example.com"),
            addr("worker-hcl72.grid.example.com")));
    map.put("^/data/gpu/.*",
        Collections.singletonList(addr("worker-gpu01.grid.example.com")));
    sqlImpl.setFileRegexToDataNodeMap(map);

    List<String> hcl = nodesForPath(sqlImpl, "/data/hcl/x");
    assertTrue(hcl.contains(addr("worker-hcl64372.grid.example.com")));
    assertFalse(hcl.contains(addr("worker-gpu01.grid.example.com")));

    List<String> gpu = nodesForPath(sqlImpl, "/data/gpu/x");
    assertTrue(gpu.contains(addr("worker-gpu01.grid.example.com")));
    assertFalse(gpu.contains(addr("worker-hcl64372.grid.example.com")));
  }

  // ==========================================================================
  // MysqlDatanodeAffinityManager — full refresh() tests
  // ==========================================================================

  @Test
  public void testSQLImplRefreshBuildsMap() throws Exception {
    insertSQLRow("hcl-group", "^/data/hcl/.*", HCL_DN_REGEX);
    sqlImpl.refresh();

    Map<String, List<String>> map = sqlImpl.getFileRegexToDataNodeMap();
    assertTrue(map.containsKey("^/data/hcl/.*"));
    List<String> nodes = map.get("^/data/hcl/.*");
    assertEquals(3, nodes.size());
    assertTrue(nodes.contains(addr("worker-hcl64372.grid.example.com")));
    assertTrue(nodes.contains(addr("worker-hcl72.grid.example.com")));
    assertTrue(nodes.contains(addr("worker-hcl63472.grid.example.com")));
    assertFalse(nodes.contains(addr("worker-gpu01.grid.example.com")));
  }

  @Test
  public void testSQLImplRefreshThenGetFavoredDatanodes() throws Exception {
    insertSQLRow("hcl-group", "^/data/hcl/.*", HCL_DN_REGEX);
    sqlImpl.refresh();

    List<String> result = nodesForPath(sqlImpl, "/data/hcl/foo.parquet");
    assertEquals(3, result.size());
    assertTrue(result.contains(addr("worker-hcl64372.grid.example.com")));
  }

  @Test
  public void testSQLImplSecondRefreshReplacesMap() throws Exception {
    insertSQLRow("old-group", "^/old/.*",
        "worker-gpu01\\.grid\\.example\\.com");
    sqlImpl.refresh();
    assertFalse(nodesForPath(sqlImpl, "/old/path").isEmpty());

    try (Statement st = conn.createStatement()) {
      st.executeUpdate("DELETE FROM " + TABLE);
    }
    insertSQLRow("hcl-group", "^/data/hcl/.*", HCL_DN_REGEX);
    sqlImpl.refresh();

    assertFalse(nodesForPath(sqlImpl, "/data/hcl/x").isEmpty());
    assertTrue(nodesForPath(sqlImpl, "/old/path").isEmpty());
  }

  @Test
  public void testSQLImplEmptyTableReturnsEmptyList() throws Exception {
    sqlImpl.refresh();
    assertNotNull(nodesForPath(sqlImpl, "/any/path"));
    assertTrue(nodesForPath(sqlImpl, "/any/path").isEmpty());
    assertTrue(sqlImpl.getFileRegexToDataNodeMap().isEmpty());
  }

  @Test
  public void testSQLImplCreateTableCalledOnlyOnFirstRefresh()
      throws Exception {
    sqlImpl.refresh();
    Mockito.verify(sqlImpl, Mockito.times(1)).createTableIfNotExists();

    sqlImpl.refresh();
    Mockito.verify(sqlImpl, Mockito.times(1)).createTableIfNotExists();
  }

  @Test
  public void testSQLImplSameRegexPatternMergesDatanodes() throws Exception {
    insertSQLRow("group-a", "^/data/mixed/.*",
        "worker-hcl64372\\.grid\\.example\\.com");
    insertSQLRow("group-b", "^/data/mixed/.*",
        "worker-gpu01\\.grid\\.example\\.com");
    sqlImpl.refresh();

    List<String> result = nodesForPath(sqlImpl, "/data/mixed/f");
    assertTrue(result.contains(addr("worker-hcl64372.grid.example.com")));
    assertTrue(result.contains(addr("worker-gpu01.grid.example.com")));
    assertEquals(1, sqlImpl.getFileRegexToDataNodeMap().size());
  }

  // ==========================================================================
  // Idempotency: identical records → no rebuild
  // ==========================================================================

  /**
   * If the backing store returns identical records, refresh() must skip
   * the rebuild — verified by the loadAffinityRecords spy returning the
   * same list twice and confirming pathRegexToDataNodeMap is unchanged
   * (same instance) on the second call.
   */
  @Test
  public void testSQLImplIdempotentRefreshSkipsRebuild() throws Exception {
    insertSQLRow("hcl-group", "^/data/hcl/.*", HCL_DN_REGEX);
    sqlImpl.refresh();
    Map<String, List<String>> first = sqlImpl.getFileRegexToDataNodeMap();
    sqlImpl.refresh();
    Map<String, List<String>> second = sqlImpl.getFileRegexToDataNodeMap();
    assertTrue(first == second,
        "Identical refresh must keep the same map instance");
  }

  // ==========================================================================
  // FileDatanodeAffinityManager — inject tests
  // ==========================================================================

  @Test
  public void testFileImplInjectMapMatchingPath() {
    Map<String, List<String>> map = new LinkedHashMap<>();
    map.put("^/data/hcl/.*",
        Arrays.asList(addr("worker-hcl64372.grid.example.com"),
            addr("worker-hcl72.grid.example.com"),
            addr("worker-hcl63472.grid.example.com")));
    fileImpl.setFileRegexToDataNodeMap(map);

    List<String> result =
        nodesForPath(fileImpl, "/data/hcl/part-00000.parquet");
    assertEquals(3, result.size());
    assertTrue(result.contains(addr("worker-hcl64372.grid.example.com")));
    assertFalse(result.contains(addr("worker-cpu01.grid.example.com")));
  }

  @Test
  public void testFileImplInjectMapNonMatchingPath() {
    Map<String, List<String>> map = new LinkedHashMap<>();
    map.put("^/data/hcl/.*",
        Collections.singletonList(addr("worker-hcl64372.grid.example.com")));
    fileImpl.setFileRegexToDataNodeMap(map);

    assertTrue(nodesForPath(fileImpl, "/other/path").isEmpty());
  }

  // ==========================================================================
  // FileDatanodeAffinityManager — full refresh() tests
  // ==========================================================================

  @Test
  public void testFileImplRefreshBuildsMap() throws Exception {
    writeAffinityJson(singleGroupJson("hcl-group", "^/data/hcl/.*",
        escapeRegexForJson(HCL_DN_REGEX)));
    fileImpl.refresh();

    Map<String, List<String>> map = fileImpl.getFileRegexToDataNodeMap();
    assertTrue(map.containsKey("^/data/hcl/.*"));
    List<String> nodes = map.get("^/data/hcl/.*");
    assertEquals(3, nodes.size());
    assertTrue(nodes.contains(addr("worker-hcl64372.grid.example.com")));
  }

  @Test
  public void testFileImplRefreshThenGetFavoredDatanodes() throws Exception {
    writeAffinityJson(singleGroupJson("hcl-group", "^/data/hcl/.*",
        escapeRegexForJson(HCL_DN_REGEX)));
    fileImpl.refresh();

    List<String> result = nodesForPath(fileImpl, "/data/hcl/foo");
    assertEquals(3, result.size());
    assertTrue(result.contains(addr("worker-hcl72.grid.example.com")));
  }

  @Test
  public void testFileImplSecondRefreshReplacesMap() throws Exception {
    writeAffinityJson(singleGroupJson("old-group", "^/old/.*",
        escapeRegexForJson("worker-gpu01\\.grid\\.example\\.com")));
    fileImpl.refresh();
    assertFalse(nodesForPath(fileImpl, "/old/path").isEmpty());

    writeAffinityJson(singleGroupJson("hcl-group", "^/data/hcl/.*",
        escapeRegexForJson(HCL_DN_REGEX)));
    fileImpl.refresh();

    assertFalse(nodesForPath(fileImpl, "/data/hcl/x").isEmpty());
    assertTrue(nodesForPath(fileImpl, "/old/path").isEmpty());
  }

  @Test
  public void testFileImplEmptyJsonArrayReturnsEmptyList() throws Exception {
    writeAffinityJson("[]");
    fileImpl.refresh();

    assertNotNull(nodesForPath(fileImpl, "/any/path"));
    assertTrue(nodesForPath(fileImpl, "/any/path").isEmpty());
    assertTrue(fileImpl.getFileRegexToDataNodeMap().isEmpty());
  }

  @Test
  public void testFileImplMissingFilePathReturnsEmptyList() throws Exception {
    Configuration conf = new Configuration();
    FileDatanodeAffinityManager impl = new FileDatanodeAffinityManager();
    impl.setConf(conf);
    impl.setDatanodeManager(buildMockDatanodeManager());
    impl.refresh();

    assertNotNull(nodesForPath(impl, "/any/path"));
    assertTrue(nodesForPath(impl, "/any/path").isEmpty());
  }

  /**
   * refresh() silently handles a non-existent file (logs a warning) and
   * leaves the affinity map empty rather than propagating the IOException.
   */
  @Test
  public void testFileImplNonExistentFileNoThrow() throws Exception {
    Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_DATANODE_AFFINITY_FILE_PATH_KEY,
        "/nonexistent/path/affinity.json");
    FileDatanodeAffinityManager impl = new FileDatanodeAffinityManager();
    impl.setConf(conf);
    impl.setDatanodeManager(buildMockDatanodeManager());
    impl.refresh();
    assertTrue(impl.getFileRegexToDataNodeMap().isEmpty(),
        "affinity map must be empty when file is missing");
  }

  /**
   * readFromFile() (the file helper) still throws when the file is missing
   * — only refresh() swallows the exception.
   */
  @Test
  public void testFileImplReadFromFileThrowsOnMissingFile() {
    assertThrows(IOException.class,
        () -> fileImpl.readFromFile("/nonexistent/affinity.json"));
  }

  @Test
  public void testFileImplMultipleGroupsOnlyMatchingReturned()
      throws Exception {
    String json = "["
        + singleGroupJson("hcl-group", "^/data/hcl/.*",
            escapeRegexForJson(HCL_DN_REGEX)).replace("[", "").replace("]", "")
        + ","
        + singleGroupJson("gpu-group", "^/data/gpu/.*",
            escapeRegexForJson("worker-gpu01\\.grid\\.example\\.com"))
                .replace("[", "").replace("]", "")
        + "]";
    writeAffinityJson(json);
    fileImpl.refresh();

    List<String> hcl = nodesForPath(fileImpl, "/data/hcl/f");
    assertTrue(hcl.contains(addr("worker-hcl64372.grid.example.com")));
    assertFalse(hcl.contains(addr("worker-gpu01.grid.example.com")));

    List<String> gpu = nodesForPath(fileImpl, "/data/gpu/f");
    assertTrue(gpu.contains(addr("worker-gpu01.grid.example.com")));
    assertFalse(gpu.contains(addr("worker-hcl64372.grid.example.com")));
  }

  /**
   * Malformed JSON entries (missing regexPattern or datanodeRegex) must be
   * skipped, but valid entries in the same array must still be parsed.
   */
  @Test
  public void testFileImplSkipsMalformedEntries() throws Exception {
    String json = "["
        + "{\"affinityGroupName\":\"no-pattern\"},"
        + singleGroupJson("hcl-group", "^/data/hcl/.*",
            escapeRegexForJson(HCL_DN_REGEX)).replace("[", "").replace("]", "")
        + "]";
    writeAffinityJson(json);
    fileImpl.refresh();
    assertEquals(1, fileImpl.getFileRegexToDataNodeMap().size());
    assertTrue(fileImpl.getFileRegexToDataNodeMap()
        .containsKey("^/data/hcl/.*"));
  }

  // ==========================================================================
  // End-to-end flow tests
  // ==========================================================================

  @Test
  public void testSQLImplEndToEndFlow() throws Exception {
    Map<String, List<String>> init = new LinkedHashMap<>();
    init.put("^/init/.*",
        Arrays.asList(addr("worker-hcl64372.grid.example.com"),
            addr("worker-hcl72.grid.example.com"),
            addr("worker-hcl63472.grid.example.com")));
    sqlImpl.setFileRegexToDataNodeMap(init);

    assertEquals(3, nodesForPath(sqlImpl, "/init/file").size());

    insertSQLRow("gpu-group", "^/data/gpu/.*",
        "worker-gpu01\\.grid\\.example\\.com");
    sqlImpl.refresh();

    List<String> gpu = nodesForPath(sqlImpl, "/data/gpu/x");
    assertEquals(1, gpu.size());
    assertTrue(gpu.contains(addr("worker-gpu01.grid.example.com")));
    assertTrue(nodesForPath(sqlImpl, "/init/file").isEmpty());
  }

  @Test
  public void testFileImplEndToEndFlow() throws Exception {
    Map<String, List<String>> init = new LinkedHashMap<>();
    init.put("^/init/.*",
        Arrays.asList(addr("worker-hcl64372.grid.example.com"),
            addr("worker-hcl72.grid.example.com"),
            addr("worker-hcl63472.grid.example.com")));
    fileImpl.setFileRegexToDataNodeMap(init);

    assertEquals(3, nodesForPath(fileImpl, "/init/file").size());

    writeAffinityJson(singleGroupJson("gpu-group", "^/data/gpu/.*",
        escapeRegexForJson("worker-gpu01\\.grid\\.example\\.com")));
    fileImpl.refresh();

    List<String> gpu = nodesForPath(fileImpl, "/data/gpu/x");
    assertEquals(1, gpu.size());
    assertTrue(gpu.contains(addr("worker-gpu01.grid.example.com")));
    assertTrue(nodesForPath(fileImpl, "/init/file").isEmpty());
  }

  // ==========================================================================
  // onDatanodeRegistered — incremental tests
  // ==========================================================================

  @Test
  public void testOnDatanodeRegisteredMatchingNodeAddedToMap()
      throws Exception {
    insertSQLRow("hcl-group", "^/data/hcl/.*", HCL_ALL_DN_REGEX);
    sqlImpl.refresh();

    List<String> before = nodesForPath(sqlImpl, "/data/hcl/x");
    assertEquals(3, before.size());

    String newHost = "worker-hcl99999.grid.example.com";
    DatanodeDescriptor newDn = Mockito.mock(DatanodeDescriptor.class);
    Mockito.when(newDn.getXferAddrWithHostname())
        .thenReturn(newHost + ":" + XFER_PORT);

    sqlImpl.onDatanodeRegistered(newDn);

    List<String> after = nodesForPath(sqlImpl, "/data/hcl/x");
    assertEquals(4, after.size());
    assertTrue(after.contains(addr(newHost)));
  }

  @Test
  public void testOnDatanodeRegisteredNonMatchingNodeNotAdded()
      throws Exception {
    insertSQLRow("hcl-group", "^/data/hcl/.*", HCL_ALL_DN_REGEX);
    sqlImpl.refresh();

    List<String> before = nodesForPath(sqlImpl, "/data/hcl/x");
    int sizeBefore = before.size();

    DatanodeDescriptor cpuDn = Mockito.mock(DatanodeDescriptor.class);
    Mockito.when(cpuDn.getXferAddrWithHostname())
        .thenReturn("worker-cpu99.grid.example.com:" + XFER_PORT);

    sqlImpl.onDatanodeRegistered(cpuDn);

    List<String> after = nodesForPath(sqlImpl, "/data/hcl/x");
    assertEquals(sizeBefore, after.size());
    assertFalse(after.contains(addr("worker-cpu99.grid.example.com")));
  }

  @Test
  public void testOnDatanodeRegisteredBeforeRefreshNoOp() {
    FileDatanodeAffinityManager fresh = new FileDatanodeAffinityManager();
    fresh.setConf(new Configuration());
    fresh.setDatanodeManager(buildMockDatanodeManager());

    DatanodeDescriptor dn = Mockito.mock(DatanodeDescriptor.class);
    Mockito.when(dn.getXferAddrWithHostname())
        .thenReturn("worker-hcl64372.grid.example.com:" + XFER_PORT);

    fresh.onDatanodeRegistered(dn);
    assertTrue(nodesForPath(fresh, "/data/hcl/x").isEmpty());
  }

  @Test
  public void testOnDatanodeRegisteredDuplicateRegistrationNotDuplicated()
      throws Exception {
    insertSQLRow("hcl-group", "^/data/hcl/.*", HCL_ALL_DN_REGEX);
    sqlImpl.refresh();

    String newHost = "worker-hcl77777.grid.example.com";
    DatanodeDescriptor newDn = Mockito.mock(DatanodeDescriptor.class);
    Mockito.when(newDn.getXferAddrWithHostname())
        .thenReturn(newHost + ":" + XFER_PORT);

    sqlImpl.onDatanodeRegistered(newDn);
    sqlImpl.onDatanodeRegistered(newDn);

    List<String> result = nodesForPath(sqlImpl, "/data/hcl/x");
    long count = result.stream().filter(s -> s.equals(addr(newHost))).count();
    assertEquals(1, count,
        "Node must appear exactly once after duplicate registration");
  }

  /**
   * A DataNode with a {@code null} xfer address must not cause an NPE in
   * the incremental registration path.
   */
  @Test
  public void testOnDatanodeRegisteredNullXferAddrSafe() throws Exception {
    insertSQLRow("hcl-group", "^/data/hcl/.*", HCL_ALL_DN_REGEX);
    sqlImpl.refresh();
    DatanodeDescriptor dn = Mockito.mock(DatanodeDescriptor.class);
    Mockito.when(dn.getXferAddrWithHostname()).thenReturn(null);
    assertFalse(sqlImpl.onDatanodeRegistered(dn),
        "null xfer addr must not add the node to any group");
  }

  @Test
  public void testRefreshDatanodeRegexMatchesNothingReturnsEmpty()
      throws Exception {
    insertSQLRow("no-match-group", "^/data/hcl/.*",
        "^worker-nonexistent.*\\.grid\\.example\\.com:");
    sqlImpl.refresh();

    List<String> result = nodesForPath(sqlImpl, "/data/hcl/x");
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testEmptyPathRegexMapReturnsEmpty() {
    sqlImpl.setFileRegexToDataNodeMap(Collections.emptyMap());
    List<String> result = nodesForPath(sqlImpl, "/any/path");
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testRegisterDatanodeThenGetFavoredDatanodes() throws Exception {
    String newHost = "worker-hcl55555.grid.example.com";
    insertSQLRow("new-node-group", "^/data/hcl/.*",
        "^worker-hcl55555\\.grid\\.example\\.com:");
    sqlImpl.refresh();

    assertTrue(nodesForPath(sqlImpl, "/data/hcl/x").isEmpty());

    DatanodeDescriptor newDn = Mockito.mock(DatanodeDescriptor.class);
    Mockito.when(newDn.getXferAddrWithHostname())
        .thenReturn(newHost + ":" + XFER_PORT);
    sqlImpl.onDatanodeRegistered(newDn);

    List<String> after = nodesForPath(sqlImpl, "/data/hcl/x");
    assertEquals(1, after.size());
    assertTrue(after.contains(addr(newHost)));
  }

  // ==========================================================================
  // getIsolatedDatanodes — bidirectional isolation tests
  // ==========================================================================

  @Test
  public void testGetIsolatedDatanodesContainsUnionOfAllGroups()
      throws Exception {
    insertSQLRow("hcl-group", "^/data/hcl/.*", HCL_DN_REGEX);
    insertSQLRow("gpu-group", "^/data/gpu/.*",
        "worker-gpu01\\.grid\\.example\\.com");
    sqlImpl.refresh();

    Set<String> isolated = sqlImpl.getIsolatedDatanodes();
    assertTrue(isolated.contains(addr("worker-hcl64372.grid.example.com")));
    assertTrue(isolated.contains(addr("worker-hcl72.grid.example.com")));
    assertTrue(isolated.contains(addr("worker-hcl63472.grid.example.com")));
    assertTrue(isolated.contains(addr("worker-gpu01.grid.example.com")));
    assertEquals(4, isolated.size());
  }

  @Test
  public void testOnDatanodeRegisteredUpdatesIsolatedDatanodes()
      throws Exception {
    insertSQLRow("hcl-group", "^/data/hcl/.*", HCL_ALL_DN_REGEX);
    sqlImpl.refresh();
    assertEquals(3, sqlImpl.getIsolatedDatanodes().size());

    String newHost = "worker-hcl88888.grid.example.com";
    DatanodeDescriptor newDn = Mockito.mock(DatanodeDescriptor.class);
    Mockito.when(newDn.getXferAddrWithHostname())
        .thenReturn(newHost + ":" + XFER_PORT);
    sqlImpl.onDatanodeRegistered(newDn);

    Set<String> isolated = sqlImpl.getIsolatedDatanodes();
    assertEquals(4, isolated.size());
    assertTrue(isolated.contains(addr(newHost)));
  }

  @Test
  public void testGetIsolatedDatanodesEmptyWhenNoAffinityGroups()
      throws Exception {
    sqlImpl.refresh();
    assertTrue(sqlImpl.getIsolatedDatanodes().isEmpty());
  }

  @Test
  public void testSetFileRegexToDataNodeMapPopulatesIsolatedDatanodes() {
    Map<String, List<String>> map = new LinkedHashMap<>();
    map.put("^/data/hcl/.*",
        Arrays.asList(addr("worker-hcl64372.grid.example.com"),
            addr("worker-hcl72.grid.example.com")));
    map.put("^/data/gpu/.*",
        Collections.singletonList(addr("worker-gpu01.grid.example.com")));
    sqlImpl.setFileRegexToDataNodeMap(map);

    Set<String> isolated = sqlImpl.getIsolatedDatanodes();
    assertEquals(3, isolated.size());
    assertTrue(isolated.contains(addr("worker-hcl64372.grid.example.com")));
    assertTrue(isolated.contains(addr("worker-hcl72.grid.example.com")));
    assertTrue(isolated.contains(addr("worker-gpu01.grid.example.com")));
  }

  // ==========================================================================
  // Pathological / failure-mode tests
  // ==========================================================================

  /**
   * An invalid datanodesRegex must be skipped with a warning — the rest of
   * the records must still be loaded successfully.
   */
  @Test
  public void testInvalidDatanodesRegexSkipped() throws Exception {
    insertSQLRow("bad-group", "^/data/bad/.*", "[unclosed[");
    insertSQLRow("good-group", "^/data/hcl/.*", HCL_DN_REGEX);
    sqlImpl.refresh();
    assertTrue(nodesForPath(sqlImpl, "/data/bad/x").isEmpty());
    assertFalse(nodesForPath(sqlImpl, "/data/hcl/x").isEmpty());
  }

  /** Escape backslashes for embedding a Java regex as a JSON string. */
  private static String escapeRegexForJson(String regex) {
    return regex.replace("\\", "\\\\");
  }
}
