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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Abstract base class for regex-based DataNode affinity (tenant isolation).
 *
 * <p>Each affinity group, stored in a backing store (MySQL or JSON file),
 * has:
 * <ul>
 *   <li>{@code regexPattern} — matched against the HDFS source file path
 *       via {@link java.util.regex.Matcher#find()}.</li>
 *   <li>{@code datanodesRegex} — matched against live cluster DataNode
 *       hostnames (formatted as {@code "hostname:port"}).</li>
 * </ul>
 *
 * <h3>Lifecycle</h3>
 * <ol>
 *   <li>{@link DatanodeManager} instantiates the configured implementation,
 *       calls {@link #setDatanodeManager(DatanodeManager)}, then calls
 *       {@link #refresh()}.</li>
 *   <li>{@link #refresh()} delegates raw record loading to the abstract
 *       {@link #loadAffinityRecords()} method, then resolves each record's
 *       {@code datanodesRegex} against all live cluster datanodes, builds
 *       {@link #pathRegexToDataNodeMap} ({@code regexPattern → List<host:port>}),
 *       and creates a restricted {@link NetworkTopology} per group.</li>
 *   <li>{@link #refresh()} calls
 *       {@link DatanodeManager#postAffinityRefresh(Set)} to remove newly
 *       isolated DataNodes from the default {@link NetworkTopology} and
 *       rebuild per-group {@link BlockPlacementPolicies} in
 *       {@link BlockManager}.</li>
 *   <li>{@link BlockManager} holds one {@link BlockPlacementPolicies} per
 *       affinity group, each backed by the restricted topology — block
 *       placement for matching paths is therefore strictly confined to the
 *       isolated pool, while non-matching paths use the default topology
 *       which no longer contains the isolated DataNodes.</li>
 * </ol>
 *
 * <p>To enable, set {@link
 * org.apache.hadoop.hdfs.DFSConfigKeys#DFS_DATANODE_AFFINITY_MANAGER_CLASSNAME_KEY}
 * to the fully qualified name of a concrete implementation. If the property
 * is empty or absent the manager is disabled and
 * {@link org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy}
 * falls back to its default behaviour with zero overhead.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class DatanodeAffinityManager implements Configurable {

  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeAffinityManager.class);

  /**
   * One raw record from the backing store (one DB row or JSON object).
   * The base class uses this to build {@link #pathRegexToDataNodeMap} and
   * the per-group restricted {@link NetworkTopology}.
   *
   * <p>{@link #equals(Object)} / {@link #hashCode()} are defined so that
   * {@link #refresh()} can skip the expensive datanode-resolution +
   * topology-rebuild pass when the backing store returns an identical set
   * of records as the previous refresh.
   */
  public static final class AffinityRecord {
    /** Human-readable group name, used for logging. */
    public final String groupName;
    /** Java regex matched against the HDFS source path. */
    public final String regexPattern;
    /** Java regex matched against cluster datanode hostnames. */
    public final String datanodesRegex;

    public AffinityRecord(String groupName, String regexPattern,
        String datanodesRegex) {
      this.groupName = groupName;
      this.regexPattern = regexPattern;
      this.datanodesRegex = datanodesRegex;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof AffinityRecord)) {
        return false;
      }
      AffinityRecord that = (AffinityRecord) o;
      return Objects.equals(groupName, that.groupName)
          && Objects.equals(regexPattern, that.regexPattern)
          && Objects.equals(datanodesRegex, that.datanodesRegex);
    }

    @Override
    public int hashCode() {
      return Objects.hash(groupName, regexPattern, datanodesRegex);
    }
  }

  /**
   * Holds a restricted {@link NetworkTopology} for one affinity group.
   *
   * <p>The topology contains only the DataNodes eligible for the group
   * identified by {@code pathPattern}. {@link BlockManager} creates one
   * {@link BlockPlacementPolicies} per group using this topology so that
   * {@link NetworkTopology#chooseRandom} never has to scan a large
   * exclusion list — it only sees the small set of in-pool nodes.
   */
  public static final class AffinityGroupTopology {
    /** File-path regex that identifies this affinity group. */
    public final Pattern pathPattern;
    /** {@link NetworkTopology} containing only this group's eligible DataNodes. */
    public final NetworkTopology topology;

    AffinityGroupTopology(Pattern pathPattern, NetworkTopology topology) {
      this.pathPattern = pathPattern;
      this.topology = topology;
    }
  }

  /** Reference to the {@link DatanodeManager} for enumerating live cluster nodes. */
  protected DatanodeManager datanodeManager;

  /**
   * Primary map of {@code regexPattern → List<"host:port">}. Built during
   * {@link #refresh()} by resolving each record's {@code datanodesRegex}
   * against live cluster nodes. Records sharing the same {@code regexPattern}
   * have their resolved DataNode lists merged.
   */
  private volatile Map<String, List<String>> pathRegexToDataNodeMap =
      Collections.emptyMap();

  /**
   * One entry per raw {@link AffinityRecord}: compiled {@code datanodesRegex}
   * paired with the record's {@code regexPattern}. Used by
   * {@link #onDatanodeRegistered(DatanodeDescriptor)} to decide which map
   * entries / topologies a freshly registered DataNode belongs to, without
   * reloading anything from the backing store.
   */
  private volatile List<AbstractMap.SimpleEntry<Pattern, String>>
      datanodePatterns = Collections.emptyList();

  /**
   * Union of every {@code "hostname:port"} address that belongs to ANY
   * affinity group. {@link DatanodeManager} uses this to remove isolated
   * DataNodes from the default {@link NetworkTopology} (so the default
   * {@link BlockPlacementPolicy} can never select them) and to re-add nodes
   * that drop out of an affinity group.
   */
  private volatile Set<String> isolatedDatanodes =
      ConcurrentHashMap.newKeySet();

  /**
   * One {@link AffinityGroupTopology} per unique {@code regexPattern} key.
   * Replaced atomically on every {@link #refresh()}.
   */
  private volatile List<AffinityGroupTopology> affinityGroupTopologies =
      Collections.emptyList();

  /**
   * Maps {@code regexPattern → NetworkTopology} for the per-group restricted
   * topology. Kept as a separate field so that
   * {@link #onDatanodeRegistered(DatanodeDescriptor)} can add a new node to
   * the correct topology without scanning the list.
   */
  private volatile Map<String, NetworkTopology> fileRegexToGroupTopology =
      Collections.emptyMap();

  /**
   * Snapshot of the {@link AffinityRecord} list from the most recent
   * successful {@link #internalRefresh()}. {@code null} before the first
   * refresh. Used for idempotency: if the backing store returns an
   * identical set of records, the expensive datanode-resolution and
   * topology-rebuild steps are skipped.
   */
  private volatile List<AffinityRecord> lastLoadedRecords = null;

  /**
   * Inject the {@link DatanodeManager} so the base class can enumerate live
   * cluster nodes during {@link #refresh()}.
   */
  public void setDatanodeManager(DatanodeManager dm) {
    this.datanodeManager = dm;
  }

  /**
   * Load raw affinity records from the backing store (DB rows or JSON
   * objects).
   *
   * <p>The base class calls this from {@link #refresh()} and uses the
   * returned records to build {@link #pathRegexToDataNodeMap}.
   * Implementations do NOT need to resolve datanodes — that is done here.
   *
   * @return non-null list of raw affinity records (may be empty)
   * @throws IOException if the backing store cannot be accessed
   */
  protected abstract List<AffinityRecord> loadAffinityRecords()
      throws IOException;

  /**
   * Reload affinity data and rebuild {@link #pathRegexToDataNodeMap} plus
   * the per-group {@link NetworkTopology}.
   *
   * <p>Wrapped in a try/catch so a transient backing-store error never
   * propagates out and breaks NameNode operations. The previous map
   * remains in effect if the rebuild fails.
   *
   * <p>Subclasses that need pre/post processing (e.g. table creation on
   * first call) should override this method and call {@code super.refresh()}.
   */
  public void refresh() {
    try {
      internalRefresh();
    } catch (Exception e) {
      LOG.warn("Failed to refresh affinity datanode manager", e);
    }
  }

  @SuppressWarnings("unchecked")
  private void internalRefresh() throws IOException {
    List<AffinityRecord> records = loadAffinityRecords();

    // Idempotency: skip the expensive datanode-resolution and topology
    // rebuild when the backing store returns exactly the same records as
    // last time. Set-equality so record ordering doesn't matter.
    List<AffinityRecord> previous = this.lastLoadedRecords;
    if (previous != null
        && new HashSet<>(records).equals(new HashSet<>(previous))) {
      LOG.debug("DatanodeAffinityManager: records unchanged ({} record(s)), "
          + "skipping rebuild", records.size());
      return;
    }

    Collection<DatanodeDescriptor> allDatanodes = datanodeManager != null
        ? datanodeManager.getAllDatanodes()
        : Collections.emptyList();

    // CopyOnWriteArrayList values let onDatanodeRegistered() append a new
    // node without locking the whole map.
    Map<String, List<String>> newMap = new ConcurrentHashMap<>();

    // Per-group restricted topology, one per regexPattern key. Concurrent
    // map so onDatanodeRegistered() can locate the topology by file-regex
    // without a list scan.
    Map<String, NetworkTopology> newRegexToTopology = new ConcurrentHashMap<>();

    // Compiled (datanodePattern → fileRegex) pairs for incremental updates.
    List<AbstractMap.SimpleEntry<Pattern, String>> newDnPatterns =
        new ArrayList<>();

    for (AffinityRecord record : records) {
      try {
        Pattern datanodePattern = Pattern.compile(record.datanodesRegex);
        newDnPatterns.add(new AbstractMap.SimpleEntry<>(
            datanodePattern, record.regexPattern));

        // computeIfAbsent: multiple records with the same regexPattern share
        // one CopyOnWriteArrayList (their node sets are merged).
        List<String> nodeList = newMap.computeIfAbsent(
            record.regexPattern, k -> new CopyOnWriteArrayList<>());

        // Same regexPattern → same NetworkTopology instance (merged group).
        final NetworkTopology groupTopo = newRegexToTopology.computeIfAbsent(
            record.regexPattern, k -> {
              try {
                return datanodeManager != null
                    ? datanodeManager.createEmptyTopology()
                    : new NetworkTopology();
              } catch (IOException ioe) {
                LOG.warn(
                    "Could not create per-group network topology for '{}'; "
                        + "falling back to plain NetworkTopology",
                    record.regexPattern, ioe);
                return new NetworkTopology();
              }
            });

        for (DatanodeDescriptor dn : allDatanodes) {
          // getXferAddrWithHostname() returns "hostname:port".
          String xferAddr = dn.getXferAddrWithHostname();
          if (xferAddr == null || !datanodePattern.matcher(xferAddr).find()) {
            continue;
          }
          if (!nodeList.contains(xferAddr)) {
            nodeList.add(xferAddr);
          }
          // Real DatanodeDescriptors have a valid network location; mock
          // objects in unit tests may not, so swallow the failure.
          try {
            groupTopo.add(dn);
          } catch (Exception e) {
            LOG.debug("DatanodeAffinityManager: could not add {} to affinity "
                + "group topology (falling back to default placement): {}",
                xferAddr, e.getMessage());
          }
        }
      } catch (PatternSyntaxException e) {
        LOG.warn("DatanodeAffinityManager: skipping record with invalid "
            + "datanodesRegex in group '{}': {}",
            record.groupName, e.getMessage());
      }
    }

    // Build immutable affinityGroupTopologies in one pass over newMap entries.
    List<AffinityGroupTopology> newGroupTopologyList = new ArrayList<>();
    for (Map.Entry<String, List<String>> entry : newMap.entrySet()) {
      try {
        Pattern pathPattern = Pattern.compile(entry.getKey());
        NetworkTopology topo = newRegexToTopology.get(entry.getKey());
        if (topo != null) {
          newGroupTopologyList.add(
              new AffinityGroupTopology(pathPattern, topo));
        }
      } catch (PatternSyntaxException e) {
        LOG.warn("DatanodeAffinityManager: skipping invalid regexPattern "
            + "'{}': {}", entry.getKey(), e.getMessage());
      }
    }

    // Build the union of affinity-pool addresses for reverse exclusion.
    Set<String> newIsolated = ConcurrentHashMap.newKeySet();
    for (List<String> nodes : newMap.values()) {
      newIsolated.addAll(nodes);
    }

    // Atomic swap: all snapshot fields updated together.
    this.pathRegexToDataNodeMap =
        (Map<String, List<String>>) (Map<?, ?>) newMap;
    this.datanodePatterns = Collections.unmodifiableList(newDnPatterns);
    this.isolatedDatanodes = newIsolated;
    this.fileRegexToGroupTopology = newRegexToTopology;
    this.affinityGroupTopologies =
        Collections.unmodifiableList(newGroupTopologyList);
    this.lastLoadedRecords =
        Collections.unmodifiableList(new ArrayList<>(records));

    // Notify the DatanodeManager so it can remove newly isolated nodes from
    // the default NetworkTopology, re-add previously isolated nodes that are
    // no longer in any group, and rebuild per-group BlockPlacementPolicies.
    if (datanodeManager != null) {
      try {
        datanodeManager.postAffinityRefresh(newIsolated);
      } catch (Exception e) {
        LOG.warn("DatanodeAffinityManager: postAffinityRefresh failed; "
            + "topology / BPP rebuild skipped", e);
      }
    }

    LOG.info("DatanodeAffinityManager: built {} path-regex → datanode "
        + "mapping(s)", newMap.size());
    if (LOG.isDebugEnabled()) {
      newMap.forEach((r, datanodes) ->
          LOG.debug("DatanodeAffinityManager: '{}' → {} node(s): {}",
              r, datanodes.size(), datanodes));
    }
  }

  /**
   * Incrementally add a newly registered DataNode to the affinity map and
   * per-group restricted topology. Called by
   * {@link DatanodeManager#registerDatanode} so block-placement affinity is
   * effective immediately, without waiting for the next
   * {@link #refresh()}.
   *
   * <p>This method is a no-op when {@link #refresh()} has not yet been
   * called (empty {@link #datanodePatterns}).
   *
   * @param dn the DataNode descriptor that just completed registration
   * @return {@code true} if the DataNode was added to at least one affinity
   *         group's restricted topology — in which case the caller should
   *         remove the node from the default {@link NetworkTopology}.
   */
  public boolean onDatanodeRegistered(DatanodeDescriptor dn) {
    try {
      List<AbstractMap.SimpleEntry<Pattern, String>> patterns =
          this.datanodePatterns;
      if (patterns.isEmpty() || dn == null) {
        return false;
      }
      String xferAddr = dn.getXferAddrWithHostname();
      if (xferAddr == null) {
        return false;
      }
      boolean nodeAdded = false;
      for (AbstractMap.SimpleEntry<Pattern, String> entry : patterns) {
        if (!entry.getKey().matcher(xferAddr).find()) {
          continue;
        }
        String fileRegex = entry.getValue();
        List<String> list = pathRegexToDataNodeMap.get(fileRegex);
        if (list == null || list.contains(xferAddr)) {
          continue;
        }
        list.add(xferAddr);
        isolatedDatanodes.add(xferAddr);
        NetworkTopology topo = fileRegexToGroupTopology.get(fileRegex);
        if (topo != null) {
          try {
            topo.add(dn);
            LOG.info("DatanodeAffinityManager: registered DataNode {} added "
                + "to affinity group for '{}'", xferAddr, fileRegex);
            nodeAdded = true;
          } catch (Exception e) {
            LOG.warn("DatanodeAffinityManager: could not add {} to affinity "
                + "group topology on registration: {}",
                xferAddr, e.getMessage());
          }
        }
      }
      return nodeAdded;
    } catch (Exception e) {
      LOG.warn("Failed to add datanode {} to affinity map: {}",
          dn, e.getMessage());
    }
    return false;
  }

  /**
   * Return the current {@code regexPattern → List<host:port>} map.
   * Intended for monitoring, fsck and unit tests. The returned map's value
   * lists are live — incremental additions made by
   * {@link #onDatanodeRegistered(DatanodeDescriptor)} are immediately
   * visible.
   */
  public Map<String, List<String>> getFileRegexToDataNodeMap() {
    return pathRegexToDataNodeMap;
  }

  /**
   * Return the union of all {@code "hostname:port"} addresses that belong
   * to any affinity group.
   *
   * @return unmodifiable view of the isolated DataNode address set; empty
   *         when no affinity groups have been loaded
   */
  public Set<String> getIsolatedDatanodes() {
    return Collections.unmodifiableSet(isolatedDatanodes);
  }

  /**
   * Return the per-group restricted topologies for use by
   * {@link BlockManager}.
   *
   * @return immutable snapshot; empty when no affinity groups are loaded
   *         or the test-injection path
   *         ({@link #setFileRegexToDataNodeMap(Map)}) was used.
   */
  public List<AffinityGroupTopology> getAffinityGroupTopologies() {
    return affinityGroupTopologies;
  }

  /**
   * Directly overwrite {@link #pathRegexToDataNodeMap} and the isolated-set.
   * Intended for unit tests that want to inject a pre-built map without
   * going through a real backing store or {@link DatanodeManager}.
   *
   * <p>Note: this path intentionally does NOT populate the per-group
   * topologies or {@link #datanodePatterns}, so block placement and
   * incremental {@link #onDatanodeRegistered(DatanodeDescriptor)} are
   * disabled — tests that need those must drive the full
   * {@link #refresh()} path instead.
   *
   * @param map {@code regexPattern → List<"host:port">}
   */
  @SuppressWarnings("unchecked")
  @VisibleForTesting
  public void setFileRegexToDataNodeMap(Map<String, List<String>> map) {
    Map<String, List<String>> newMap = new ConcurrentHashMap<>();
    for (Map.Entry<String, List<String>> entry : map.entrySet()) {
      newMap.put(entry.getKey(),
          new CopyOnWriteArrayList<>(entry.getValue()));
    }
    Set<String> newIsolated = ConcurrentHashMap.newKeySet();
    for (List<String> nodes : newMap.values()) {
      newIsolated.addAll(nodes);
    }
    this.pathRegexToDataNodeMap =
        (Map<String, List<String>>) (Map<?, ?>) newMap;
    this.isolatedDatanodes = newIsolated;
    this.fileRegexToGroupTopology = Collections.emptyMap();
    this.affinityGroupTopologies = Collections.emptyList();
  }
}
