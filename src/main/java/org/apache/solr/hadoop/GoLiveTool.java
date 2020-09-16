package org.apache.solr.hadoop;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.shaded.apache.solr.client.solrj.SolrServerException;
import org.shaded.apache.solr.client.solrj.impl.CloudSolrClient;
import org.shaded.apache.solr.client.solrj.impl.HttpSolrClient;
import org.shaded.apache.solr.client.solrj.request.CoreAdminRequest;
import org.shaded.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.hadoop.GoLiveToolArgumentParser.GoLiveOptions;
import org.apache.solr.hadoop.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoLiveTool extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new GoLiveTool(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    GoLiveOptions options = new GoLiveOptions();
    Integer exitCode = new GoLiveToolArgumentParser().parseArgs(args, getConf(), options);
    if (exitCode != null) {
      return exitCode;
    }
    
    GoLiveToolArgumentParser.verifyGoLiveArgs(options, null);
    options.zkOptions.verifyZKStructure(null);
    // auto update shard count
    if (options.zkOptions.zkHost != null) {
      options.shards = options.zkOptions.shardUrls.size();
    }
    
    if (!goLive(getConf(), options)) {
      return -1;
    }
    return 0;
  }

  // TODO: handle clusters with replicas
  // @bfs: This expects the same number of shards as there are shards on the server, otherwise, it just fails.
  public boolean goLive(Configuration conf, GoLiveOptions options) throws FileNotFoundException, IOException {
    FileStatus[] outDirs = Utils.listSortedOutputShardDirs(conf, options.inputDir);
    
    Integer maxShards = options.maxShards;
    
    LOG.info("Live merging of output shards into Solr cluster...");
    boolean success = false;
    long start = System.nanoTime();
    int concurrentMerges = options.goLiveThreads;
    ThreadPoolExecutor executor = new ExecutorUtil.MDCAwareThreadPoolExecutor(concurrentMerges,
        concurrentMerges, 1, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>());
    
    try {
      CompletionService<Request> completionService = new ExecutorCompletionService<>(executor);
      Set<Future<Request>> pending = new HashSet<>();
      int cnt = -1;
      for (int i=0;i<outDirs.length;i++) {
        if (maxShards != null && i >= maxShards) {
          break;
        }
        final FileStatus dir = outDirs[i];
        LOG.debug("processing: " + dir.getPath());

        cnt++;
        List<String> urls = options.zkOptions.shardUrls.get(cnt);
        
        for (String url : urls) {
          
          String baseUrl = url;
          if (baseUrl.endsWith("/")) {
            baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
          }
          
          int lastPathIndex = baseUrl.lastIndexOf("/");
          if (lastPathIndex == -1) {
            LOG.error("Found unexpected shardurl, live merge failed: " + baseUrl);
            return false;
          }
          
          final String name = baseUrl.substring(lastPathIndex + 1);
          baseUrl = baseUrl.substring(0, lastPathIndex);
          final String mergeUrl = baseUrl;
          
          Callable<Request> task = () -> {
            Request req = new Request();
            LOG.info("Live merge " + dir.getPath() + " into " + mergeUrl);
            try (final HttpSolrClient client = new HttpSolrClient.Builder(mergeUrl).build()) {
              CoreAdminRequest.MergeIndexes mergeRequest = new CoreAdminRequest.MergeIndexes();
              mergeRequest.setCoreName(name);
              mergeRequest.setIndexDirs(Arrays.asList(dir.getPath().toString() + "/data/index"));
              mergeRequest.process(client);
              req.success = true;
            } catch (SolrServerException | IOException e) {
              req.e = e;
            }
            return req;
          };
          pending.add(completionService.submit(task));
        }
      }
      
      while (pending != null && pending.size() > 0) {
        try {
          Future<Request> future = completionService.take();
          if (future == null) break;
          pending.remove(future);
          
          try {
            Request req = future.get();
            
            if (!req.success) {
              // failed
              LOG.error("A live merge command failed", req.e);
              return false;
            }
            
          } catch (ExecutionException e) {
            LOG.error("Error sending live merge command", e);
            return false;
          }
          
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOG.error("Live merge process interrupted", e);
          return false;
        }
      }
      
      cnt = -1;
      
      
      try {
        LOG.info("Committing live merge...");
        if (options.zkOptions.zkHost != null) {
          try (CloudSolrClient server = new CloudSolrClient.Builder().withZkHost(options.zkOptions.zkHost).build()) {
            server.setDefaultCollection(options.zkOptions.collection);
            server.commit();
          }
        } else {
          for (List<String> urls : options.zkOptions.shardUrls) {
            for (String url : urls) {
              // TODO: we should do these concurrently
              try (HttpSolrClient server = new HttpSolrClient.Builder(url).build()) {
                server.commit();
              }
            }
          }
        }
        LOG.info("Done committing live merge");
      } catch (IOException | SolrServerException e) {
        LOG.error("Error sending commits to live Solr cluster", e);
        return false;
      }

      success = true;
      return true;
    } finally {
      ExecutorUtil.shutdownAndAwaitTermination(executor);
      float secs = (System.nanoTime() - start) / (float)(10^9);
      LOG.info("Live merging of index shards into Solr cluster took " + secs + " secs");
      if (success) {
        LOG.info("Live merging completed successfully");
      } else {
        LOG.info("Live merging failed");
      }
    }
    
    // if an output dir does not exist, we should fail and do no merge?
  }
  
  private static final class Request {
    Exception e;
    boolean success = false;
  }
}
