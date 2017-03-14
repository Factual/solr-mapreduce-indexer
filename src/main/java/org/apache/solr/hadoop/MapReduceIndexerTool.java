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


import org.apache.solr.hadoop.util.Utils;
import org.apache.solr.hadoop.util.ZooKeeperInspector;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import net.sourceforge.argparse4j.inf.ArgumentParserException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.hadoop.morphline.MorphlineMapRunner;
import org.apache.solr.hadoop.morphline.MorphlineMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;


/**
 * Public API for a MapReduce batch job driver that creates a set of Solr index shards from a set of
 * input files and writes the indexes into HDFS, in a flexible, scalable and fault-tolerant manner.
 * Also supports merging the output shards into a set of live customer facing Solr servers,
 * typically a SolrCloud.
 */
public class MapReduceIndexerTool extends Configured implements Tool {
  
  public static final String RESULTS_DIR = "results";
  static final int MAX_FILES_TO_RANDOMIZE_IN_MEMORY = 10000000;
  static final String MAIN_MEMORY_RANDOMIZATION_THRESHOLD =  MapReduceIndexerTool.class.getName() + ".mainMemoryRandomizationThreshold";
  public static final String FULL_INPUT_LIST = "full-input-list.txt"; 
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  Job job;
  
  
  /** API for command line clients
   * @param args
   * @throws java.lang.Exception */
  public static void main(String[] args) throws Exception  {
    int res = ToolRunner.run(new Configuration(), new MapReduceIndexerTool(), args);
    System.exit(res);
  }

  public MapReduceIndexerTool() {}

  @Override
  public int run(String[] args) throws Exception {
    MapReduceIndexerToolArgumentParser.Options opts = new MapReduceIndexerToolArgumentParser.Options();
    Integer exitCode = new MapReduceIndexerToolArgumentParser().parseArgs(args, getConf(), opts);
    if (exitCode != null) {
      return exitCode;
    }
    return run(opts);
  }
  
  /** API for Java clients; visible for testing; may become a public API eventually */
  int run(MapReduceIndexerToolArgumentParser.Options options) throws Exception {
    

  
    Instant programStart = Instant.now();
    
    if (options.fairSchedulerPool != null) {
      getConf().set("mapred.fairscheduler.pool", options.fairSchedulerPool);
    }
    getConf().setInt(SolrOutputFormat.SOLR_RECORD_WRITER_MAX_SEGMENTS, options.maxSegments);
    
  
    if (options.log4jConfigFile != null) {
      Utils.setLogConfigFile(options.log4jConfigFile, getConf());
      addDistributedCacheFile(options.log4jConfigFile, getConf());
    }

    job = Job.getInstance(getConf());
    job.setJarByClass(getClass());

    if (options.morphlineFile == null) {
      throw new ArgumentParserException("Argument --morphline-file is required", null);
    }
    MapReduceIndexerToolArgumentParser.verifyGoLiveArgs(options, null);
    MapReduceIndexerToolArgumentParser.verifyZKStructure(options, null);
    
    int mappers = new JobClient(job.getConfiguration()).getClusterStatus().getMaxMapTasks();
    
    LOG.info("Cluster reports {} mapper slots", mappers);
    
    if (options.mappers == -1) { 
      mappers = 8 * mappers; // better accomodate stragglers
    } else {
      mappers = options.mappers;
    }
    if (mappers <= 0) {
      throw new IllegalStateException("Illegal number of mappers: " + mappers);
    }
    options.mappers = mappers;
    
    FileSystem fs = options.outputDir.getFileSystem(job.getConfiguration());
    if (fs.exists(options.outputDir) && !Utils.delete(options.outputDir, true, fs)) {
      return -1;
    }
    Path outputResultsDir = new Path(options.outputDir, RESULTS_DIR);
    Path outputReduceDir = new Path(options.outputDir, "reducers");
    Path outputStep1Dir = new Path(options.outputDir, "tmp1");    
    Path outputStep2Dir = new Path(options.outputDir, "tmp2");    
    
    Path fullInputList = new Path(outputStep1Dir, FULL_INPUT_LIST);
    
    LOG.debug("Creating list of input files for mappers: {}", fullInputList);
    long numFiles = addInputFiles(options.inputFiles, options.inputLists, fullInputList, job.getConfiguration());
    if (numFiles == 0) {
      LOG.info("No input files found - nothing to process");
      return 0;
    }
    int numLinesPerSplit = (int) Utils.ceilDivide(numFiles, mappers);
    if (numLinesPerSplit < 0) { // numeric overflow from downcasting long to int?
      numLinesPerSplit = Integer.MAX_VALUE;
    }
    numLinesPerSplit = Math.max(1, numLinesPerSplit);

    int realMappers = Math.min(mappers, (int) Utils.ceilDivide(numFiles, numLinesPerSplit));
    calculateNumReducers(options, realMappers);
    int reducers = options.reducers;
    LOG.info("Using these parameters: " +
        "numFiles: {}, mappers: {}, realMappers: {}, reducers: {}, shards: {}, fanout: {}, maxSegments: {}",
        new Object[] {numFiles, mappers, realMappers, reducers, options.shards, options.fanout, options.maxSegments});
        
    
    LOG.info("Randomizing list of {} input files to spread indexing load more evenly among mappers", numFiles);
    Instant startTime = Instant.now();
    int maxFilesToRandomizeInMemory =  job.getConfiguration().getInt(MAIN_MEMORY_RANDOMIZATION_THRESHOLD, MAX_FILES_TO_RANDOMIZE_IN_MEMORY);
    if (numFiles < maxFilesToRandomizeInMemory) {
      // If there are few input files reduce latency by directly running main memory randomization 
      // instead of launching a high latency MapReduce job
      LOG.info("Randomizing list of input files in memory because there are fewer than the in-memory limit of {} files", maxFilesToRandomizeInMemory);
      Utils.randomizeFewInputFiles(fs, outputStep2Dir, fullInputList);
    } else {
      // Randomize using a MapReduce job. Use sequential algorithm below a certain threshold because there's no
      // benefit in using many parallel mapper tasks just to randomize the order of a few lines each
      int numLinesPerRandomizerSplit = Math.max(10 * 1000 * 1000, numLinesPerSplit);
      Job randomizerJob = Utils.randomizeManyInputFiles(getConf(), fullInputList, outputStep2Dir, numLinesPerRandomizerSplit);
      if (!Utils.waitForCompletion(randomizerJob, options.isVerbose)) {
        return -1; // job failed
      }
    }
    Instant endTime = Instant.now();
    LOG.info("Done. Randomizing list of {} input files took {}", numFiles, Duration.between(startTime, endTime));
    
    
    job.setInputFormatClass(NLineInputFormat.class);
    NLineInputFormat.addInputPath(job, outputStep2Dir);
    NLineInputFormat.setNumLinesPerSplit(job, numLinesPerSplit);    
    FileOutputFormat.setOutputPath(job, outputReduceDir);
    
    String mapperClass = job.getConfiguration().get(JobContext.MAP_CLASS_ATTR);
    if (mapperClass == null) { // enable customization
      Class clazz = MorphlineMapper.class;
      mapperClass = clazz.getName();
      job.setMapperClass(clazz);
    }
    job.setJobName(getClass().getName() + "/" + Utils.getShortClassName(mapperClass));
    
    if (job.getConfiguration().get(JobContext.REDUCE_CLASS_ATTR) == null) { // enable customization
      job.setReducerClass(SolrReducer.class);
    }
    if (options.updateConflictResolver == null) {
      throw new IllegalArgumentException("updateConflictResolver must not be null");
    }
    job.getConfiguration().set(SolrReducer.UPDATE_CONFLICT_RESOLVER, options.updateConflictResolver);
    
    if (options.zkHost != null) {
      assert options.collection != null;
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
      job.getConfiguration().set(SolrCloudPartitioner.ZKHOST, options.zkHost);
      job.getConfiguration().set(SolrCloudPartitioner.COLLECTION, options.collection);
    }
    job.getConfiguration().setInt(SolrCloudPartitioner.SHARDS, options.shards);

    job.setOutputFormatClass(SolrOutputFormat.class);
    if (options.solrHomeDir != null) {
      SolrOutputFormat.setupSolrHomeCache(options.solrHomeDir, job);
    } else {
      assert options.zkHost != null;
      // use the config that this collection uses for the SolrHomeCache.
      ZooKeeperInspector zki = new ZooKeeperInspector();
      try (SolrZkClient zkClient = zki.getZkClient(options.zkHost)) {
        String configName = zki.readConfigName(zkClient, options.collection);
        File tmpSolrHomeDir = zki.downloadConfigDir(zkClient, configName);
        SolrOutputFormat.setupSolrHomeCache(tmpSolrHomeDir, job);
        options.solrHomeDir = tmpSolrHomeDir;
      }
    }
    
    MorphlineMapRunner runner = setupMorphline(options);
    if (options.isDryRun && runner != null) {
      LOG.info("Indexing {} files in dryrun mode", numFiles);
      startTime = Instant.now();
      dryRun(runner, fs, fullInputList);
      endTime = Instant.now();
      LOG.info("Done. Indexing {} files in dryrun mode took {}", numFiles, Duration.between(startTime, endTime));
      Utils.goodbye(null, programStart);
      return 0;
    }          
    job.getConfiguration().set(MorphlineMapRunner.MORPHLINE_FILE_PARAM, options.morphlineFile.getName());

    job.setNumReduceTasks(reducers);  
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(SolrInputDocumentWritable.class);
    LOG.info("Indexing {} files using {} real mappers into {} reducers", new Object[] {numFiles, realMappers, reducers});
    
    
    //messes with solr on hdfs
    job.setMapSpeculativeExecution(false);
    job.setReduceSpeculativeExecution(false);
    
    //crank up memory on reducers
    job.getConfiguration().setInt("mapreduce.reduce.memory.mb", 32768);
    job.getConfiguration().set("mapreduce.reduce.java.opts","-Xmx16384M");

    startTime = Instant.now();
    if (!Utils.waitForCompletion(job, options.isVerbose)) {
      return -1; // job failed
    }

    endTime = Instant.now();
    LOG.info("Done. Indexing {} files using {} real mappers into {} reducers took {}", new Object[] {numFiles, realMappers, reducers, Duration.between(startTime, endTime)});

  
    if (reducers > options.shards) {
      LOG.info("The number of reducers is greater than the number of shards.  Invoking the tree merge process");
      TreeMergeRunner treeMergeRunner = new TreeMergeRunner();
      treeMergeRunner.run(outputReduceDir, options, getConf());
    }
    
    
    // normalize output shard dir prefix, i.e.
    // rename part-r-00000 to part-00000 (stems from zero tree merge iterations)
    // rename part-m-00000 to part-00000 (stems from > 0 tree merge iterations)
    for (FileStatus stats : fs.listStatus(outputReduceDir)) {
      String dirPrefix = SolrOutputFormat.getOutputName(job);
      Path srcPath = stats.getPath();
      if (stats.isDirectory() && srcPath.getName().startsWith(dirPrefix)) {
        String dstName = dirPrefix + srcPath.getName().substring(dirPrefix.length() + "-m".length());
        Path dstPath = new Path(srcPath.getParent(), dstName);
        if (!Utils.rename(srcPath, dstPath, fs)) {
          return -1;
        }        
      }
    }    
    
    // publish results dir    
    if (!Utils.rename(outputReduceDir, outputResultsDir, fs)) {
      return -1;
    }

    if (options.goLive && !new GoLive().goLive(options, Utils.listSortedOutputShardDirs(outputResultsDir, fs, job))) {
      return -1;
    }
    
    Utils.goodbye(job, programStart);    
    return 0;
  }

  private void calculateNumReducers(MapReduceIndexerToolArgumentParser.Options options, int realMappers) throws IOException {
    if (options.shards <= 0) {
      throw new IllegalStateException("Illegal number of shards: " + options.shards);
    }
    if (options.fanout <= 1) {
      throw new IllegalStateException("Illegal fanout: " + options.fanout);
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
        } reducers = options.reducers;
        break;
    }
    reducers = Math.max(reducers, options.shards);
    
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
    options.reducers = reducers;
  }
  
  private long addInputFiles(List<Path> inputFiles, List<Path> inputLists, Path fullInputList, Configuration conf) 
      throws IOException {
    
    long numFiles = 0;
    FileSystem fs = fullInputList.getFileSystem(conf);
    try (FSDataOutputStream out = fs.create(fullInputList); Writer writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))) {
      
      for (Path inputFile : inputFiles) {
        FileSystem inputFileFs = inputFile.getFileSystem(conf);
        if (inputFileFs.exists(inputFile)) {
          PathFilter pathFilter = (Path path) -> !(path.getName().startsWith(".") || path.getName().startsWith("_")) // ignore "hidden" files and dirs
          ;
          numFiles += addInputFilesRecursively(inputFile, writer, inputFileFs, pathFilter);
        }
      }

      for (Path inputList : inputLists) {
        InputStream in;
        if (inputList.toString().equals("-")) {
          in = System.in;
        } else if (inputList.isAbsoluteAndSchemeAuthorityNull()) {
          in = new BufferedInputStream(new FileInputStream(inputList.toString()));
        } else {
          in = inputList.getFileSystem(conf).open(inputList);
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
          String line;
          while ((line = reader.readLine()) != null) {
            writer.write(line + "\n");
            numFiles++;
          }
        } finally {
          in.close();
        }
      }
      
    }    
    return numFiles;
  }
  
  /**
   * Add the specified file to the input set, if path is a directory then
   * add the files contained therein.
   */
  private long addInputFilesRecursively(Path path, Writer writer, FileSystem fs, PathFilter pathFilter) throws IOException {
    long numFiles = 0;
    for (FileStatus stat : fs.listStatus(path, pathFilter)) {
      LOG.debug("Adding path {}", stat.getPath());
      if (stat.isDirectory()) {
        numFiles += addInputFilesRecursively(stat.getPath(), writer, fs, pathFilter);
      } else {
        writer.write(stat.getPath().toString() + "\n");
        numFiles++;
      }
    }
    return numFiles;
  }
  
  
  // do the same as if the user had typed 'hadoop ... --files <file>' 
  private void addDistributedCacheFile(File file, Configuration conf) throws IOException {
    String HADOOP_TMP_FILES = "tmpfiles"; // see Hadoop's GenericOptionsParser
    String tmpFiles = conf.get(HADOOP_TMP_FILES, "");
    if (tmpFiles.length() > 0) { // already present?
      tmpFiles = tmpFiles + ","; 
    }
    GenericOptionsParser parser = new GenericOptionsParser(
        new Configuration(conf), 
        new String[] { "--files", file.getCanonicalPath() });
    String additionalTmpFiles = parser.getConfiguration().get(HADOOP_TMP_FILES);
    assert additionalTmpFiles != null;
    assert additionalTmpFiles.length() > 0;
    tmpFiles += additionalTmpFiles;
    conf.set(HADOOP_TMP_FILES, tmpFiles);
  }
  
  private MorphlineMapRunner setupMorphline(MapReduceIndexerToolArgumentParser.Options options) throws IOException, URISyntaxException {
    if (options.morphlineId != null) {
      job.getConfiguration().set(MorphlineMapRunner.MORPHLINE_ID_PARAM, options.morphlineId);
    }
    addDistributedCacheFile(options.morphlineFile, job.getConfiguration());    
    if (!options.isDryRun) {
      return null;
    }
    
    /*
     * Ensure scripting support for Java via morphline "java" command works even in dryRun mode,
     * i.e. when executed in the client side driver JVM. To do so, collect all classpath URLs from
     * the class loaders chain that org.apache.hadoop.util.RunJar (hadoop jar xyz-job.jar) and
     * org.apache.hadoop.util.GenericOptionsParser (--libjars) have installed, then tell
     * FastJavaScriptEngine.parse() where to find classes that JavaBuilder scripts might depend on.
     * This ensures that scripts that reference external java classes compile without exceptions
     * like this:
     * 
     * ... caused by compilation failed: mfm:///MyJavaClass1.java:2: package
     * org.kitesdk.morphline.api does not exist
     */
    LOG.trace("dryRun: java.class.path: {}", System.getProperty("java.class.path"));
    String fullClassPath = "";
    ClassLoader loader = Thread.currentThread().getContextClassLoader(); // see org.apache.hadoop.util.RunJar
    while (loader != null) { // walk class loaders, collect all classpath URLs
      if (loader instanceof URLClassLoader) { 
        URL[] classPathPartURLs = ((URLClassLoader) loader).getURLs(); // see org.apache.hadoop.util.RunJar
        LOG.trace("dryRun: classPathPartURLs: {}", Arrays.asList(classPathPartURLs));
        StringBuilder classPathParts = new StringBuilder();
        for (URL url : classPathPartURLs) {
          File file = new File(url.toURI());
          if (classPathPartURLs.length > 0) {
            classPathParts.append(File.pathSeparator);
          }
          classPathParts.append(file.getPath());
        }
        LOG.trace("dryRun: classPathParts: {}", classPathParts);
        String separator = File.pathSeparator;
        if (fullClassPath.length() == 0 || classPathParts.length() == 0) {
          separator = "";
        }
        fullClassPath = classPathParts + separator + fullClassPath;
      }
      loader = loader.getParent();
    }
    
    // tell FastJavaScriptEngine.parse() where to find the classes that the script might depend on
    if (fullClassPath.length() > 0) {
      assert System.getProperty("java.class.path") != null;
      fullClassPath = System.getProperty("java.class.path") + File.pathSeparator + fullClassPath;
      LOG.trace("dryRun: fullClassPath: {}", fullClassPath);
      System.setProperty("java.class.path", fullClassPath); // see FastJavaScriptEngine.parse()
    }
    
    job.getConfiguration().set(MorphlineMapRunner.MORPHLINE_FILE_PARAM, options.morphlineFile.getPath());
    return new MorphlineMapRunner(
        job.getConfiguration(), new DryRunDocumentLoader(), options.solrHomeDir.getPath());
  }
  
  /*
   * Executes the morphline in the current process (without submitting a job to MR) for quicker
   * turnaround during trial & debug sessions
   */
  private void dryRun(MorphlineMapRunner runner, FileSystem fs, Path fullInputList) throws IOException {    
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(fullInputList), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        runner.map(line, job.getConfiguration(), null);
      }
      runner.cleanup();
    }
  }
  

  
}
