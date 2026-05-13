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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * MySQL-backed implementation of {@link DatanodeAffinityManager}.
 *
 * <p>Stores affinity groups in a MySQL table created automatically on first
 * startup. Schema:
 * <pre>
 *   CREATE TABLE IF NOT EXISTS &lt;table&gt; (
 *     affinity_group_name  VARCHAR(255)  NOT NULL,
 *     file_regex           VARCHAR(1024) NOT NULL,  -- matches HDFS srcPath
 *     datanode_regex       VARCHAR(2048) NOT NULL,  -- matches DN hostname:port
 *     PRIMARY KEY (affinity_group_name, file_regex)
 *   );
 * </pre>
 *
 * <p>Required configuration keys (in {@code hdfs-site.xml}):
 * <ul>
 *   <li>{@link DFSConfigKeys#DFS_DATANODE_AFFINITY_MYSQL_URL}</li>
 *   <li>{@link DFSConfigKeys#DFS_DATANODE_AFFINITY_MYSQL_USERNAME}</li>
 *   <li>{@link DFSConfigKeys#DFS_DATANODE_AFFINITY_MYSQL_TABLE}</li>
 * </ul>
 */
public class MysqlDatanodeAffinityManager extends DatanodeAffinityManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(MysqlDatanodeAffinityManager.class);

  /** Shared executor used by JDBC for network-timeout cancellation. */
  private static final ExecutorService NETWORK_TIMEOUT_EXECUTOR =
      Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "dn-affinity-mysql-network-timeout");
        t.setDaemon(true);
        return t;
      });

  private Configuration conf;
  private HikariDataSource dataSource;
  private int queryTimeoutSec = 0;
  private int networkTimeoutMs = 0;

  /** Guards one-time table-creation on the very first refresh. */
  private final AtomicBoolean startupRefresh = new AtomicBoolean(true);

  @Override
  public void setConf(Configuration conf) {
    if (this.conf != null) {
      return; // already initialised
    }
    this.conf = conf;
    this.queryTimeoutSec = conf.getInt(
        DFSConfigKeys.DFS_DATANODE_AFFINITY_MYSQL_QUERY_TIMEOUT_MS,
        DFSConfigKeys.DFS_DATANODE_AFFINITY_MYSQL_QUERY_TIMEOUT_MS_DEFAULT)
        / 1000;
    this.networkTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_DATANODE_AFFINITY_MYSQL_NETWORK_TIMEOUT_MS,
        DFSConfigKeys.DFS_DATANODE_AFFINITY_MYSQL_NETWORK_TIMEOUT_MS_DEFAULT);

    String url = conf.get(DFSConfigKeys.DFS_DATANODE_AFFINITY_MYSQL_URL);
    if (url == null || url.isEmpty()) {
      LOG.warn(
          "MysqlDatanodeAffinityManager: {} is not set; the manager will be "
              + "disabled until configuration is provided",
          DFSConfigKeys.DFS_DATANODE_AFFINITY_MYSQL_URL);
      return;
    }

    Properties props = new Properties();
    props.setProperty("jdbcUrl", url);
    props.setProperty("username",
        conf.get(DFSConfigKeys.DFS_DATANODE_AFFINITY_MYSQL_USERNAME, ""));
    // Cert-based auth deployments still need a non-null password property;
    // the actual auth is handled by the JDBC driver.
    props.setProperty("password", "dummy_password");
    props.setProperty("driverClassName",
        conf.get(DFSConfigKeys.DFS_DATANODE_AFFINITY_MYSQL_DRIVER,
            DFSConfigKeys.DFS_DATANODE_AFFINITY_MYSQL_DRIVER_DEFAULT));
    props.setProperty("connectionTimeout",
        String.valueOf(networkTimeoutMs > 0 ? networkTimeoutMs : 30000));
    props.putAll(conf.getPropsWithPrefix(
        DFSConfigKeys.DFS_DATANODE_AFFINITY_MYSQL_HIKARI_PREFIX));

    initDataSource(new HikariConfig(props));
  }

  @VisibleForTesting
  void initDataSource(HikariConfig hikariConfig) {
    if (this.dataSource != null) {
      this.dataSource.close();
    }
    this.dataSource = new HikariDataSource(hikariConfig);
    // Best-effort shutdown of the pool on JVM exit. The lambda captures
    // {@code this} so each instance gets its own hook — fine because
    // there is at most one MysqlDatanodeAffinityManager per NameNode.
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        if (dataSource != null && !dataSource.isClosed()) {
          dataSource.close();
        }
      } catch (Throwable t) {
        // JVM is shutting down; nothing useful to do.
      }
    }, "dn-affinity-mysql-shutdown"));
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * On the first call, ensure the backing table exists; then delegate to
   * {@link DatanodeAffinityManager#refresh()} which calls
   * {@link #loadAffinityRecords()} and rebuilds the map.
   *
   * <p>Does not propagate exceptions so a transient SQL error never breaks
   * a {@code dfsadmin -refreshNodes} call.
   */
  @Override
  public void refresh() {
    if (dataSource == null) {
      LOG.debug("MysqlDatanodeAffinityManager: not initialised, skipping "
          + "refresh");
      return;
    }
    boolean doStartup = startupRefresh.compareAndSet(true, false);
    try {
      if (doStartup) {
        createTableIfNotExists();
      }
    } catch (Exception e) {
      LOG.error("MysqlDatanodeAffinityManager: failed to create table", e);
      // Leave startupRefresh false so we don't retry on every refresh.
    }
    super.refresh();
  }

  @Override
  protected List<AffinityRecord> loadAffinityRecords() throws IOException {
    try {
      return readFromMysql();
    } catch (SQLException e) {
      LOG.error("MysqlDatanodeAffinityManager: failed to read affinity "
          + "records", e);
      throw new IOException(
          "MysqlDatanodeAffinityManager: failed to read affinity records", e);
    }
  }

  @VisibleForTesting
  void createTableIfNotExists() throws SQLException {
    String table = conf.get(DFSConfigKeys.DFS_DATANODE_AFFINITY_MYSQL_TABLE);
    if (table == null || table.isEmpty()) {
      throw new SQLException(
          DFSConfigKeys.DFS_DATANODE_AFFINITY_MYSQL_TABLE + " is not set");
    }
    try (Connection conn = dataSource.getConnection()) {
      setNetworkTimeout(conn);
      try (PreparedStatement stmt = conn.prepareStatement(
          String.format(
              "CREATE TABLE IF NOT EXISTS %s ("
                  + "`affinity_group_name` VARCHAR(255) NOT NULL, "
                  + "`file_regex`          VARCHAR(1024) NOT NULL, "
                  + "`datanode_regex`      VARCHAR(2048) NOT NULL, "
                  + "PRIMARY KEY (`affinity_group_name`, `file_regex`))",
              table))) {
        setQueryTimeout(stmt);
        stmt.executeUpdate();
        LOG.info("MysqlDatanodeAffinityManager: ensured table {} exists",
            table);
      }
    } catch (SQLException e) {
      LOG.error("MysqlDatanodeAffinityManager: failed to create table {}",
          table, e);
      throw e;
    }
  }

  @VisibleForTesting
  List<AffinityRecord> readFromMysql() throws SQLException {
    String table = conf.get(DFSConfigKeys.DFS_DATANODE_AFFINITY_MYSQL_TABLE);
    if (table == null || table.isEmpty()) {
      throw new SQLException(
          DFSConfigKeys.DFS_DATANODE_AFFINITY_MYSQL_TABLE + " is not set");
    }
    List<AffinityRecord> result = new ArrayList<>();
    try (Connection conn = dataSource.getConnection()) {
      setNetworkTimeout(conn);
      try (PreparedStatement stmt = conn.prepareStatement(
          String.format(
              "SELECT affinity_group_name, file_regex, datanode_regex FROM %s",
              table))) {
        setQueryTimeout(stmt);
        try (ResultSet rs = stmt.executeQuery()) {
          while (rs.next()) {
            result.add(new AffinityRecord(
                rs.getString("affinity_group_name"),
                rs.getString("file_regex"),
                rs.getString("datanode_regex")));
          }
        }
      }
    } catch (SQLException e) {
      LOG.error("MysqlDatanodeAffinityManager: failed to read from table {}",
          table, e);
      throw e;
    }
    return Collections.unmodifiableList(result);
  }

  @VisibleForTesting
  void setQueryTimeout(PreparedStatement stmt) throws SQLException {
    if (queryTimeoutSec > 0) {
      stmt.setQueryTimeout(queryTimeoutSec);
    }
  }

  @VisibleForTesting
  void setNetworkTimeout(Connection conn) throws SQLException {
    if (networkTimeoutMs > 0) {
      conn.setNetworkTimeout(NETWORK_TIMEOUT_EXECUTOR, networkTimeoutMs);
    }
  }
}
