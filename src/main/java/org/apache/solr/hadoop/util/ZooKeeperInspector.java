/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.hadoop.util;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.*;

import org.shaded.apache.solr.cloud.ZkController;
import org.shaded.apache.solr.common.SolrException;
import org.shaded.apache.solr.common.cloud.Aliases;
import org.shaded.apache.solr.common.cloud.ClusterState;
import org.shaded.apache.solr.common.cloud.DocCollection;
import org.shaded.apache.solr.common.cloud.Replica;
import org.shaded.apache.solr.common.cloud.Slice;
import org.shaded.apache.solr.common.cloud.SolrZkClient;
import org.shaded.apache.solr.common.cloud.ZkConfigManager;
import org.shaded.apache.solr.common.cloud.ZkCoreNodeProps;
import org.shaded.apache.solr.common.cloud.ZkNodeProps;
import org.shaded.apache.solr.common.cloud.ZkStateReader;
import org.shaded.apache.solr.common.util.StrUtils;
import org.apache.solr.hadoop.IndexTool;
import org.apache.zookeeper.KeeperException;
import org.shaded.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

/**
 * Extracts SolrCloud information from ZooKeeper.
 */
public final class ZooKeeperInspector {
  
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public List<List<String>> extractShardUrls(String zkHost, String collection) {

    DocCollection docCollection = extractDocCollection(zkHost, collection);
    List<Slice> slices = getSortedSlices(docCollection.getSlices());
    List<List<String>> solrUrls = new ArrayList<>(slices.size());
    for (Slice slice : slices) {
      if (slice.getLeader() == null) {
        throw new IllegalArgumentException("Cannot find SolrCloud slice leader. " +
            "It looks like not all of your shards are registered in ZooKeeper yet");
      }
      Collection<Replica> replicas = slice.getReplicas();
      List<String> urls = new ArrayList<>(replicas.size());
      for (Replica replica : replicas) {
        ZkCoreNodeProps props = new ZkCoreNodeProps(replica);
        urls.add(props.getCoreUrl());
      }
      solrUrls.add(urls);
    }
    return solrUrls;
  }
  
  public DocCollection extractDocCollection(String zkHost, String collection) {
    if (collection == null) {
      throw new IllegalArgumentException("collection must not be null");
    }
    SolrZkClient zkClient = getZkClient(zkHost);
    
    try (ZkStateReader zkStateReader = new ZkStateReader(zkClient)) {
      try {
        // first check for alias
        collection = checkForAlias(zkClient, collection);
        zkStateReader.createClusterStateWatchersAndUpdate();
      } catch (KeeperException | InterruptedException e) {
        throw new IllegalArgumentException("Cannot find expected information for SolrCloud in ZooKeeper: " + zkHost, e);
      }
      
      try {
        return zkStateReader.getClusterState().getCollection(collection);
      } catch (SolrException e) {
        throw new IllegalArgumentException("Cannot find collection '" + collection + "' in ZooKeeper: " + zkHost, e);
      }
    } finally {
      zkClient.close();
    }    
  }

  public SolrZkClient getZkClient(String zkHost) {
    if (zkHost == null) {
      throw new IllegalArgumentException("zkHost must not be null");
    }

    SolrZkClient zkClient;
    try {
      zkClient = new SolrZkClient(zkHost, 30000);
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot connect to ZooKeeper: " + zkHost, e);
    }
    return zkClient;
  }
  
  public List<Slice> getSortedSlices(Collection<Slice> slices) {
    List<Slice> sorted = new ArrayList(slices);
    Collections.sort(sorted, (slice1, slice2) -> {
      Comparator c = new AlphaNumericComparator();
      return c.compare(slice1.getName(), slice2.getName());
    });
    LOG.trace("Sorted slices: {}", sorted);
    return sorted;
  }

  /**
   * Returns config value given collection name
   * Borrowed heavily from Solr's ZKController.
   */
  public String readConfigName(SolrZkClient zkClient, String collection)
  throws KeeperException, InterruptedException {
    if (collection == null) {
      throw new IllegalArgumentException("collection must not be null");
    }
    String configName = null;

    // first check for alias
    collection = checkForAlias(zkClient, collection);
    
    String path = ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection;
    if (LOG.isInfoEnabled()) {
      LOG.info("Load collection config from:" + path);
    }
    byte[] data = zkClient.getData(path, null, null, true);
    
    if(data != null) {
      ZkNodeProps props = ZkNodeProps.load(data);
      configName = props.getStr(ZkController.CONFIGNAME_PROP);
    }
    
    if (configName != null && !zkClient.exists(ZkConfigManager.CONFIGS_ZKNODE + "/" + configName, true)) {
      LOG.error("Specified config does not exist in ZooKeeper:" + configName);
      throw new IllegalArgumentException("Specified config does not exist in ZooKeeper:"
        + configName);
    }

    return configName;
  }

  private String checkForAlias(SolrZkClient zkClient, String collection)
      throws KeeperException, InterruptedException {
    byte[] aliasData = zkClient.getData(ZkStateReader.ALIASES, null, null, true);
    Aliases aliases = Aliases.fromJSON(aliasData, 0);
    List<String> aliasList = aliases.resolveAliases(collection);
    if (aliasList.size() > 1) {
      throw new IllegalArgumentException("collection cannot be an alias that maps to multiple collections");
    }
    return aliasList.get(0);
  }
  
  /**
   * Download and return the config directory from ZK
   */
  public File downloadConfigDir(SolrZkClient zkClient, String configName)
  throws IOException, InterruptedException, KeeperException {
    File dir = Files.createTempDir();
    dir.deleteOnExit();

    ZkConfigManager configManager = new ZkConfigManager(zkClient);
    configManager.downloadConfigDir(configName, dir.toPath());
    
    IndexTool.setupConfigDir(dir);
    
    return dir;

  }

}
