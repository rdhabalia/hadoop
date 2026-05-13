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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * JSON-file-backed implementation of {@link DatanodeAffinityManager}.
 *
 * <p>Reads affinity groups from a JSON file referenced by
 * {@link DFSConfigKeys#DFS_DATANODE_AFFINITY_FILE_PATH_KEY}.
 *
 * <p>Expected JSON format — an array of affinity group objects:
 * <pre>
 * [
 *   {
 *     "affinityGroupName": "hcl-group",
 *     "regexPattern":      "^/data/hcl/.*",
 *     "datanodeRegex":     "^[a-zA-Z]+-hcl(64372|72|63472|6123)\\.grid\\..*"
 *   }
 * ]
 * </pre>
 *
 * <p>Call {@code dfsadmin -refreshNodes} to hot-reload the file without a
 * NameNode restart.
 *
 * <p>The {@link #refresh()} method intentionally does not propagate
 * {@link IOException}: a missing or malformed file leaves the previous
 * mapping in effect and logs a warning, so the NameNode stays operational
 * if the affinity file is temporarily unavailable.
 */
public class FileDatanodeAffinityManager extends DatanodeAffinityManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(FileDatanodeAffinityManager.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private Configuration conf;

  /** JSON-deserialisable representation of one affinity group object. */
  static final class AffinityGroupEntry {
    @JsonProperty("affinityGroupName") public String affinityGroupName;
    @JsonProperty("regexPattern") public String regexPattern;
    @JsonProperty("datanodeRegex") public String datanodeRegex;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  /**
   * Handle the "no file path configured" case (log + clear map), then
   * delegate to {@link DatanodeAffinityManager#refresh()}.
   */
  @Override
  public void refresh() {
    String filePath =
        conf.get(DFSConfigKeys.DFS_DATANODE_AFFINITY_FILE_PATH_KEY);
    if (filePath == null || filePath.isEmpty()) {
      LOG.warn("FileDatanodeAffinityManager: {} is not set; "
          + "no affinity groups loaded",
          DFSConfigKeys.DFS_DATANODE_AFFINITY_FILE_PATH_KEY);
      setFileRegexToDataNodeMap(Collections.emptyMap());
      return;
    }
    super.refresh();
  }

  /**
   * Read the JSON file and return its entries as raw {@link AffinityRecord}s.
   * The base class handles datanode resolution.
   */
  @Override
  protected List<AffinityRecord> loadAffinityRecords() throws IOException {
    String filePath =
        conf.get(DFSConfigKeys.DFS_DATANODE_AFFINITY_FILE_PATH_KEY);
    return readFromFile(filePath);
  }

  @VisibleForTesting
  List<AffinityRecord> readFromFile(String filePath) throws IOException {
    File file = new File(filePath);
    if (!file.exists()) {
      throw new IOException(
          "FileDatanodeAffinityManager: file not found: " + filePath);
    }
    try {
      AffinityGroupEntry[] entries =
          MAPPER.readValue(file, AffinityGroupEntry[].class);
      List<AffinityRecord> result = new ArrayList<>();
      if (entries != null) {
        for (AffinityGroupEntry entry : entries) {
          if (entry == null
              || entry.regexPattern == null
              || entry.datanodeRegex == null) {
            LOG.warn("FileDatanodeAffinityManager: skipping malformed entry "
                + "in {} (missing regexPattern or datanodeRegex)", filePath);
            continue;
          }
          result.add(new AffinityRecord(
              entry.affinityGroupName,
              entry.regexPattern,
              entry.datanodeRegex));
        }
      }
      return Collections.unmodifiableList(result);
    } catch (IOException e) {
      LOG.error("FileDatanodeAffinityManager: failed to parse {}", filePath, e);
      throw new IOException(
          "FileDatanodeAffinityManager: failed to parse " + filePath, e);
    }
  }
}
