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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.solr.hadoop.MapReduceIndexerToolArgumentParser.Options;
import org.apache.solr.hadoop.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

public abstract class IndexTool extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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
    } else {
      options.shards = options.reducers;
    }

    FileSystem fs = options.outputDir.getFileSystem(job.getConfiguration());
    if (fs.exists(options.outputDir)) { 
      // Do not auto delete previous output directory
      // && !Utils.delete(options.outputDir, true, fs)) {
      LOG.error("Output path already exists: " + options.outputDir);
      return -1;
    }
    
    Path outputReduceDir = options.outputDir;

    int setupResult = setupIndexing(job, options);
    if (setupResult <= 0) {
      return setupResult;
    }
    
    FileOutputFormat.setOutputPath(job, outputReduceDir);
    
    if (options.updateConflictResolver == null) {
      throw new IllegalArgumentException("updateConflictResolver must not be null");
    }
    job.getConfiguration().set(SolrReducer.UPDATE_CONFLICT_RESOLVER, options.updateConflictResolver);

    attemptDryRun(job, options, programStart);

    job.setNumReduceTasks(options.reducers);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(SolrInputDocumentWritable.class);

    //messes with solr on hdfs
    job.setMapSpeculativeExecution(false);
    job.setReduceSpeculativeExecution(false);

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


  public static File setupConfigDir(File dir) throws IOException {

    File confDir = new File(dir, "conf");
    if (!confDir.isDirectory()) {
      // create a temporary directory with "conf" subdir and mv the config in there.  This is
      // necessary because of CDH-11188; solrctl does not generate nor accept directories with e.g.
      // conf/solrconfig.xml which is necessary for proper solr operation.  This should work
      // even if solrctl changes.
      confDir = new File(Files.createTempDir().getAbsolutePath(), "conf");
      confDir.getParentFile().deleteOnExit();
      Files.move(dir, confDir);
      dir = confDir.getParentFile();
    }
  
    FileUtils.writeStringToFile(new File(dir, "solr.xml"), "<solr><solrcloud></solrcloud></solr>", "UTF-8");
    LOG.info("Wrote solr configs to: {}", confDir);
 
    verifyConfigDir(confDir);
    String confPath = confDir.getAbsolutePath();
    
    File newPath = new File(confPath.substring(0,confPath.length()-"conf".length()) + "/core1/conf");
    LOG.info("Copy solr configs to: {}", newPath);
    FileUtils.copyDirectory(confDir, newPath);
    
    return dir;
  }
  
  private static void verifyConfigDir(File confDir) throws IOException {
    File solrConfigFile = new File(confDir, "solrconfig.xml");
    if (!solrConfigFile.exists()) {
      throw new IOException("Detected invalid Solr config dir in ZooKeeper - Reason: File not found: "
          + solrConfigFile.getName());
    }
    if (!solrConfigFile.isFile()) {
      throw new IOException("Detected invalid Solr config dir in ZooKeeper - Reason: Not a file: "
          + solrConfigFile.getName());
    }
    if (!solrConfigFile.canRead()) {
      throw new IOException("Insufficient permissions to read file: " + solrConfigFile);
    }    
  }
}
