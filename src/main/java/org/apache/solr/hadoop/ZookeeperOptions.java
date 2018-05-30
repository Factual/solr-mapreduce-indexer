package org.apache.solr.hadoop;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.solr.hadoop.util.ZooKeeperInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;

public class ZookeeperOptions {
  
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public String zkHost;
  public String collection;
  public List<List<String>> shardUrls;
  
  public ZookeeperOptions(String zkHost, String collection, List<List<String>> shardUrls) {
    this.zkHost = zkHost;
    this.collection = collection;
    this.shardUrls = shardUrls;
  }
  
  public void verifyZKStructure(ArgumentParser parser) throws ArgumentParserException {
    if (this.zkHost != null) {
      assert this.collection != null;
      ZooKeeperInspector zki = new ZooKeeperInspector();
      try {
        this.shardUrls = zki.extractShardUrls(this.zkHost, this.collection);
      } catch (Exception e) {
        LOG.debug("Cannot extract SolrCloud shard URLs from ZooKeeper", e);
        throw new ArgumentParserException(e, parser);
      }
      assert this.shardUrls != null;
      if (this.shardUrls.isEmpty()) {
        throw new ArgumentParserException("--zk-host requires ZooKeeper " + this.zkHost
          + " to contain at least one SolrCore for collection: " + this.collection, parser);
      }
      LOG.debug("Using SolrCloud shard URLs: {}", this.shardUrls);
    }
  }
  
  public static List<List<String>> buildShardUrls(List<Object> urls, Integer numShards) {
    if (urls == null) return null;
    return  Lists.partition(
              urls.stream()
                  .map(u -> u.toString())
                  .collect(Collectors.toList()),numShards);   
  }

}
