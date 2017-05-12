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
package org.apache.solr.hadoop;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.time.Instant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.hadoop.MapReduceIndexerToolArgumentParser.Options;
import org.apache.solr.hadoop.util.Utils;
import org.apache.solr.hadoop.util.ZooKeeperInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class IndexTool extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String REDUCERS_DIR = "reducers";

  /**
   * API for command line clients
   *
   * @param args
   * @throws java.lang.Exception
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MorphlineEnabledIndexerTool(), args);
    System.exit(res);
  }

  public IndexTool() {
  }

  @Override
  public int run(String[] args) throws Exception {
    MapReduceIndexerToolArgumentParser.Options opts = new MapReduceIndexerToolArgumentParser.Options();
    Integer exitCode = new MapReduceIndexerToolArgumentParser().parseArgs(args, getConf(), opts);
    if (exitCode != null) {
      return exitCode;
    }
    return run(opts);
  }

  /**
   * API for Java clients; visible for testing; may become a public API
   * eventually
   */
  public int run(MapReduceIndexerToolArgumentParser.Options options) throws Exception {

    Instant programStart = Instant.now();
    
    if (options.fairSchedulerPool != null) {
      getConf().set("mapred.fairscheduler.pool", options.fairSchedulerPool);
    }
    getConf().setInt(SolrOutputFormat.SOLR_RECORD_WRITER_MAX_SEGMENTS, options.maxSegments);

    if (options.log4jConfigFile != null) {
      Utils.setLogConfigFile(options.log4jConfigFile, getConf());
      addDistributedCacheFile(options.log4jConfigFile, getConf());
    }

    Job job = Job.getInstance(getConf());
    job.setJarByClass(getClass());

    options.zkOptions.verifyZKStructure(null);
    // auto update shard count
    if (options.zkOptions.zkHost != null) {
      options.shards = options.zkOptions.shardUrls.size();
    }

    FileSystem fs = options.outputDir.getFileSystem(job.getConfiguration());
    if (fs.exists(options.outputDir)) { 
      // Do not auto delete previous output directory
      // && !Utils.delete(options.outputDir, true, fs)) {
      LOG.error("Output path already exists: " + options.outputDir);
      return -1;
    }
    
    Path outputReduceDir = new Path(options.outputDir, REDUCERS_DIR);

    int setupResult = setupIndexing(job, options);
    if (setupResult <= 0) {
      return setupResult;
    }
    
    FileOutputFormat.setOutputPath(job, outputReduceDir);
    
    if (job.getConfiguration().get(JobContext.REDUCE_CLASS_ATTR) == null) { // enable customization
      job.setReducerClass(SolrReducer.class);
    }
    if (options.updateConflictResolver == null) {
      throw new IllegalArgumentException("updateConflictResolver must not be null");
    }
    job.getConfiguration().set(SolrReducer.UPDATE_CONFLICT_RESOLVER, options.updateConflictResolver);

    if (options.zkOptions.zkHost != null) {
      assert options.zkOptions.collection != null;
      /*
       * MapReduce partitioner that partitions the Mapper output such that each
       * SolrInputDocument gets sent to the SolrCloud shard that it would have
       * been sent to if the document were ingested via the standard SolrCloud
       * Near Real Time (NRT) API.
       * 
       * In other words, this class implements the same partitioning semantics
       * as the standard SolrCloud NRT API. This enables to mix batch updates
       * from MapReduce ingestion with updates from standard NRT ingestion on
       * the same SolrCloud cluster, using identical unique document keys.
       */
      if (job.getConfiguration().get(JobContext.PARTITIONER_CLASS_ATTR) == null) { // enable customization
        job.setPartitionerClass(SolrCloudPartitioner.class);
      }
      job.getConfiguration().set(SolrCloudPartitioner.ZKHOST, options.zkOptions.zkHost);
      job.getConfiguration().set(SolrCloudPartitioner.COLLECTION, options.zkOptions.collection);
    }
    job.getConfiguration().setInt(SolrCloudPartitioner.SHARDS, options.shards);

    job.setOutputFormatClass(SolrOutputFormat.class);
    if (options.solrHomeDir != null) {
      SolrOutputFormat.setupSolrHomeCache(options.solrHomeDir, job);
    } else {
      assert options.zkOptions.zkHost != null;
      // use the config that this collection uses for the SolrHomeCache.
      ZooKeeperInspector zki = new ZooKeeperInspector();
      try (SolrZkClient zkClient = zki.getZkClient(options.zkOptions.zkHost)) {
        String configName = zki.readConfigName(zkClient, options.zkOptions.collection);
        File tmpSolrHomeDir = zki.downloadConfigDir(zkClient, configName);
        SolrOutputFormat.setupSolrHomeCache(tmpSolrHomeDir, job);
        options.solrHomeDir = tmpSolrHomeDir;
      }
    }

    attemptDryRun(job, options, programStart);

    job.setNumReduceTasks(options.reducers);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(SolrInputDocumentWritable.class);

    //messes with solr on hdfs
    job.setMapSpeculativeExecution(false);
    job.setReduceSpeculativeExecution(false);

    //crank up memory on reducers
    job.getConfiguration().setInt("mapreduce.reduce.memory.mb", 32768);
    job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx16384M");

    Instant startTime = Instant.now();
    if (!Utils.waitForCompletion(job, options.isVerbose)) {
      return -1; // job failed
    }

    Instant endTime = Instant.now();
    LOG.info("Done. Indexing into {} reducers took {}", new Object[]{options.reducers, Duration.between(startTime, endTime)});

    Utils.goodbye(job, programStart);
    return 0;
  }

  public abstract int setupIndexing(Job job, Options options) throws Exception;

  public abstract void attemptDryRun(Job job, Options options, Instant programStart) throws Exception;

  // do the same as if the user had typed 'hadoop ... --files <file>' 
  public static void addDistributedCacheFile(File file, Configuration conf) throws IOException {
    String HADOOP_TMP_FILES = "tmpfiles"; // see Hadoop's GenericOptionsParser
    String tmpFiles = conf.get(HADOOP_TMP_FILES, "");
    if (tmpFiles.length() > 0) { // already present?
      tmpFiles = tmpFiles + ",";
    }
    GenericOptionsParser parser = new GenericOptionsParser(
            new Configuration(conf),
            new String[]{"--files", file.getCanonicalPath()});
    String additionalTmpFiles = parser.getConfiguration().get(HADOOP_TMP_FILES);
    assert additionalTmpFiles != null;
    assert additionalTmpFiles.length() > 0;
    tmpFiles += additionalTmpFiles;
    conf.set(HADOOP_TMP_FILES, tmpFiles);
  }
  
  public static void calculateNumReducers(Job job, MapReduceIndexerToolArgumentParser.Options options, int realMappers) throws IOException {
    if (options.shards <= 0) {
      throw new IllegalStateException("Illegal number of shards: " + options.shards);
    }
    if (realMappers <= 0) {
      throw new IllegalStateException("Illegal realMappers: " + realMappers);
    }

    int reducers = new JobClient(job.getConfiguration()).getClusterStatus().getMaxReduceTasks();

    LOG.info("Cluster reports {} reduce slots", reducers);

    switch (options.reducers) {
      case -2:
        reducers = options.shards;
        break;
      case -1:
        reducers = Math.min(reducers, realMappers); // no need to use many reducers when using few mappers
        break;
      default:
        if (options.reducers == 0) {
          throw new IllegalStateException("Illegal zero reducers");
        }
        reducers = options.reducers;
        break;
    }
    
    if (options.fanout <= 1) {
      throw new IllegalStateException("Illegal fanout: " + options.fanout);
    }
    if (reducers != options.shards) {
      // Ensure fanout isn't misconfigured. fanout can't meaningfully be larger than what would be 
      // required to merge all leaf shards in one single tree merge iteration into root shards
      options.fanout = Math.min(options.fanout, (int) Utils.ceilDivide(reducers, options.shards));

      // Ensure invariant reducers == options.shards * (fanout ^ N) where N is an integer >= 1.
      // N is the number of mtree merge iterations.
      // This helps to evenly spread docs among root shards and simplifies the impl of the mtree merge algorithm.
      int s = options.shards;
      while (s < reducers) {
        s = s * options.fanout;
      }
      reducers = s;
      assert reducers % options.fanout == 0;
    }
    
    options.reducers = Math.max(reducers, options.shards);
  }

}
