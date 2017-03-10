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


import org.apache.solr.hadoop.util.LineRandomizerReducer;
import org.apache.solr.hadoop.util.AlphaNumericComparator;
import org.apache.solr.hadoop.util.Utils;
import org.apache.solr.hadoop.util.ZooKeeperInspector;
import org.apache.solr.hadoop.util.LineRandomizerMapper;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import net.sourceforge.argparse4j.inf.ArgumentParserException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.hadoop.morphline.MorphlineMapRunner;
import org.apache.solr.hadoop.morphline.MorphlineMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
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
  private static final String FULL_INPUT_LIST = "full-input-list.txt"; 
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  Job job;
  
  static List<List<String>> buildShardUrls(List<Object> urls, Integer numShards) {
    if (urls == null) return null;
    List<List<String>> shardUrls = new ArrayList<>(urls.size());
    List<String> list = null;
    
    int sz;
    if (numShards == null) {
      numShards = urls.size();
    }
    sz = (int) Math.ceil(urls.size() / (float)numShards);
    for (int i = 0; i < urls.size(); i++) {
      if (i % sz == 0) {
        list = new ArrayList<>();
        shardUrls.add(list);
      }
      list.add((String) urls.get(i));
    }

    return shardUrls;
  }
  


  
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
    if (fs.exists(options.outputDir) && !delete(options.outputDir, true, fs)) {
      return -1;
    }
    Path outputResultsDir = new Path(options.outputDir, RESULTS_DIR);
    Path outputReduceDir = new Path(options.outputDir, "reducers");
    Path outputStep1Dir = new Path(options.outputDir, "tmp1");    
    Path outputStep2Dir = new Path(options.outputDir, "tmp2");    
    Path outputTreeMergeStep = new Path(options.outputDir, "mtree-merge-output");
    Path fullInputList = new Path(outputStep1Dir, FULL_INPUT_LIST);
    
    LOG.debug("Creating list of input files for mappers: {}", fullInputList);
    long numFiles = addInputFiles(options.inputFiles, options.inputLists, fullInputList, job.getConfiguration());
    if (numFiles == 0) {
      LOG.info("No input files found - nothing to process");
      return 0;
    }
    int numLinesPerSplit = (int) ceilDivide(numFiles, mappers);
    if (numLinesPerSplit < 0) { // numeric overflow from downcasting long to int?
      numLinesPerSplit = Integer.MAX_VALUE;
    }
    numLinesPerSplit = Math.max(1, numLinesPerSplit);

    int realMappers = Math.min(mappers, (int) ceilDivide(numFiles, numLinesPerSplit));
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
      randomizeFewInputFiles(fs, outputStep2Dir, fullInputList);
    } else {
      // Randomize using a MapReduce job. Use sequential algorithm below a certain threshold because there's no
      // benefit in using many parallel mapper tasks just to randomize the order of a few lines each
      int numLinesPerRandomizerSplit = Math.max(10 * 1000 * 1000, numLinesPerSplit);
      Job randomizerJob = randomizeManyInputFiles(getConf(), fullInputList, outputStep2Dir, numLinesPerRandomizerSplit);
      if (!waitForCompletion(randomizerJob, options.isVerbose)) {
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
      goodbye(null, programStart);
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
    
    
    startTime = Instant.now();
    if (!waitForCompletion(job, options.isVerbose)) {
      return -1; // job failed
    }

    endTime = Instant.now();
    LOG.info("Done. Indexing {} files using {} real mappers into {} reducers took {}", new Object[] {numFiles, realMappers, reducers, Duration.between(startTime, endTime)});

    int mtreeMergeIterations = 0;
    if (reducers > options.shards) {
      mtreeMergeIterations = (int) Math.round(log(options.fanout, reducers / options.shards));
    }
    LOG.debug("MTree merge iterations to do: {}", mtreeMergeIterations);
    int mtreeMergeIteration = 1;
    while (reducers > options.shards) { // run a mtree merge iteration
      Configuration conf = getConf();
//      conf.setInt("mapreduce.map.memory.mb", 32768);
//      conf.set("mapreduce.map.java.opts","-Xmx16384M");

      Job mergeTreeJob = Job.getInstance(conf);
         
      
      mergeTreeJob.setMapSpeculativeExecution(false);
      mergeTreeJob.setReduceSpeculativeExecution(false);
    
      mergeTreeJob.setJarByClass(getClass());
      mergeTreeJob.setJobName(getClass().getName() + "/" + Utils.getShortClassName(TreeMergeMapper.class));
      mergeTreeJob.setMapperClass(TreeMergeMapper.class);
      mergeTreeJob.setOutputFormatClass(TreeMergeOutputFormat.class);
      
      mergeTreeJob.setNumReduceTasks(0);  
      mergeTreeJob.setOutputKeyClass(Text.class);
      mergeTreeJob.setOutputValueClass(NullWritable.class);    
      mergeTreeJob.setInputFormatClass(NLineInputFormat.class);
      
      Path inputStepDir = new Path(options.outputDir, "mtree-merge-input-iteration" + mtreeMergeIteration);
      fullInputList = new Path(inputStepDir, FULL_INPUT_LIST);    
      LOG.debug("MTree merge iteration {}/{}: Creating input list file for mappers {}", new Object[] {mtreeMergeIteration, mtreeMergeIterations, fullInputList});
      numFiles = createTreeMergeInputDirList(outputReduceDir, fs, fullInputList);    
      if (numFiles != reducers) {
        throw new IllegalStateException("Not same reducers: " + reducers + ", numFiles: " + numFiles);
      }
      NLineInputFormat.addInputPath(mergeTreeJob, fullInputList);
      NLineInputFormat.setNumLinesPerSplit(mergeTreeJob, options.fanout);    
      FileOutputFormat.setOutputPath(mergeTreeJob, outputTreeMergeStep);
      
      LOG.info("MTree merge iteration {}/{}: Merging {} shards into {} shards using fanout {}", new Object[] { 
          mtreeMergeIteration, mtreeMergeIterations, reducers, (reducers / options.fanout), options.fanout});
      startTime = Instant.now();
      if (!waitForCompletion(mergeTreeJob, options.isVerbose)) {
        return -1; // job failed
      }
      if (!renameTreeMergeShardDirs(outputTreeMergeStep, mergeTreeJob, fs)) {
        return -1;
      }
      endTime = Instant.now();
      LOG.info("MTree merge iteration {}/{}: Done. Merging {} shards into {} shards using fanout {} took {}",
          new Object[] {mtreeMergeIteration, mtreeMergeIterations, reducers, (reducers / options.fanout), options.fanout, Duration.between(startTime, endTime)});
      
      if (!delete(outputReduceDir, true, fs)) {
        return -1;
      }
      if (!rename(outputTreeMergeStep, outputReduceDir, fs)) {
        return -1;
      }
      assert reducers % options.fanout == 0;
      reducers = reducers / options.fanout;
      mtreeMergeIteration++;
    }
    assert reducers == options.shards;
    
    // normalize output shard dir prefix, i.e.
    // rename part-r-00000 to part-00000 (stems from zero tree merge iterations)
    // rename part-m-00000 to part-00000 (stems from > 0 tree merge iterations)
    for (FileStatus stats : fs.listStatus(outputReduceDir)) {
      String dirPrefix = SolrOutputFormat.getOutputName(job);
      Path srcPath = stats.getPath();
      if (stats.isDirectory() && srcPath.getName().startsWith(dirPrefix)) {
        String dstName = dirPrefix + srcPath.getName().substring(dirPrefix.length() + "-m".length());
        Path dstPath = new Path(srcPath.getParent(), dstName);
        if (!rename(srcPath, dstPath, fs)) {
          return -1;
        }        
      }
    }    
    
    // publish results dir    
    if (!rename(outputReduceDir, outputResultsDir, fs)) {
      return -1;
    }

    if (options.goLive && !new GoLive().goLive(options, listSortedOutputShardDirs(outputResultsDir, fs))) {
      return -1;
    }
    
    goodbye(job, programStart);    
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
      options.fanout = Math.min(options.fanout, (int) ceilDivide(reducers, options.shards));
      
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
  
  private void randomizeFewInputFiles(FileSystem fs, Path outputStep2Dir, Path fullInputList) throws IOException {    
    List<String> lines = new ArrayList();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(fullInputList), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        lines.add(line);
      }
    }
    
    Collections.shuffle(lines, new Random(421439783L)); // constant seed for reproducability
    
    FSDataOutputStream out = fs.create(new Path(outputStep2Dir, FULL_INPUT_LIST));
    try (Writer writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))) {
      for (String line : lines) {
        writer.write(line + "\n");
      } 
    }
  }

  /**
   * To uniformly spread load across all mappers we randomize fullInputList
   * with a separate small Mapper & Reducer preprocessing step. This way
   * each input line ends up on a random position in the output file list.
   * Each mapper indexes a disjoint consecutive set of files such that each
   * set has roughly the same size, at least from a probabilistic
   * perspective.
   * 
   * For example an input file with the following input list of URLs:
   * 
   * A
   * B
   * C
   * D
   * 
   * might be randomized into the following output list of URLs:
   * 
   * C
   * A
   * D
   * B
   * 
   * The implementation sorts the list of lines by randomly generated numbers.
   */
  private Job randomizeManyInputFiles(Configuration baseConfig, Path fullInputList, Path outputStep2Dir, int numLinesPerSplit) 
      throws IOException {
    
    Job randomizeInputFilesJob = Job.getInstance(baseConfig);
    randomizeInputFilesJob.setJarByClass(getClass());
    randomizeInputFilesJob.setJobName(getClass().getName() + "/" + Utils.getShortClassName(LineRandomizerMapper.class));
    randomizeInputFilesJob.setInputFormatClass(NLineInputFormat.class);
    NLineInputFormat.addInputPath(randomizeInputFilesJob, fullInputList);
    NLineInputFormat.setNumLinesPerSplit(randomizeInputFilesJob, numLinesPerSplit);          
    randomizeInputFilesJob.setMapperClass(LineRandomizerMapper.class);
    randomizeInputFilesJob.setReducerClass(LineRandomizerReducer.class);
    randomizeInputFilesJob.setOutputFormatClass(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(randomizeInputFilesJob, outputStep2Dir);
    randomizeInputFilesJob.setNumReduceTasks(1);
    randomizeInputFilesJob.setOutputKeyClass(LongWritable.class);
    randomizeInputFilesJob.setOutputValueClass(Text.class);
    return randomizeInputFilesJob;
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
  
private int createTreeMergeInputDirList(Path outputReduceDir, FileSystem fs, Path fullInputList)
      throws FileNotFoundException, IOException {
    
    FileStatus[] dirs = listSortedOutputShardDirs(outputReduceDir, fs);
    int numFiles = 0;
    FSDataOutputStream out = fs.create(fullInputList);
    try {
      Writer writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
      for (FileStatus stat : dirs) {
        LOG.debug("Adding path {}", stat.getPath());
        Path dir = new Path(stat.getPath(), "data/index");
        if (!fs.isDirectory(dir)) {
          throw new IllegalStateException("Not a directory: " + dir);
        }
        writer.write(dir.toString() + "\n");
        numFiles++;
      }
      writer.close();
    } finally {
      out.close();
    }
    return numFiles;
  }


    private FileStatus[] listSortedOutputShardDirs(Path outputReduceDir, FileSystem fs) throws FileNotFoundException,
      IOException {
    
    final String dirPrefix = SolrOutputFormat.getOutputName(job);
    FileStatus[] dirs = fs.listStatus(outputReduceDir, new PathFilter() {      
      @Override
      public boolean accept(Path path) {
        return path.getName().startsWith(dirPrefix);
      }
    });
    for (FileStatus dir : dirs) {
      if (!dir.isDirectory()) {
        throw new IllegalStateException("Not a directory: " + dir.getPath());
      }
    }
    
    // use alphanumeric sort (rather than lexicographical sort) to properly handle more than 99999 shards
    Arrays.sort(dirs, (f1, f2) -> new AlphaNumericComparator().compare(f1.getPath().getName(), f2.getPath().getName()));

    return dirs;
  }

  /*
   * You can run MapReduceIndexerTool in Solrcloud mode, and once the MR job completes, you can use
   * the standard solrj Solrcloud API to send doc updates and deletes to SolrCloud, and those updates
   * and deletes will go to the right Solr shards, and it will work just fine.
   * 
   * The MapReduce framework doesn't guarantee that input split N goes to the map task with the
   * taskId = N. The job tracker and Yarn schedule and assign tasks, considering data locality
   * aspects, but without regard of the input split# withing the overall list of input splits. In
   * other words, split# != taskId can be true.
   * 
   * To deal with this issue, our mapper tasks write a little auxiliary metadata file (per task)
   * that tells the job driver which taskId processed which split#. Once the mapper-only job is
   * completed, the job driver renames the output dirs such that the dir name contains the true solr
   * shard id, based on these auxiliary files.
   * 
   * This way each doc gets assigned to the right Solr shard even with #reducers > #solrshards
   * 
   * Example for a merge with two shards:
   * 
   * part-m-00000 and part-m-00001 goes to outputShardNum = 0 and will end up in merged part-m-00000
   * part-m-00002 and part-m-00003 goes to outputShardNum = 1 and will end up in merged part-m-00001
   * part-m-00004 and part-m-00005 goes to outputShardNum = 2 and will end up in merged part-m-00002
   * ... and so on
   * 
   * Also see run() method above where it uses NLineInputFormat.setNumLinesPerSplit(job,
   * options.fanout)
   * 
   * Also see TreeMergeOutputFormat.TreeMergeRecordWriter.writeShardNumberFile()
   */
   private boolean renameTreeMergeShardDirs(Path outputTreeMergeStep, Job job, FileSystem fs) throws IOException {
    final String dirPrefix = SolrOutputFormat.getOutputName(job);
    FileStatus[] dirs = fs.listStatus(outputTreeMergeStep, new PathFilter() {      
      @Override
      public boolean accept(Path path) {
        return path.getName().startsWith(dirPrefix);
      }
    });
    
    for (FileStatus dir : dirs) {
      if (!dir.isDirectory()) {
        throw new IllegalStateException("Not a directory: " + dir.getPath());
      }
    }

    // Example: rename part-m-00004 to _part-m-00004
    for (FileStatus dir : dirs) {
      Path path = dir.getPath();
      Path renamedPath = new Path(path.getParent(), "_" + path.getName());
      if (!rename(path, renamedPath, fs)) {
        return false;
      }
    }
    
    // Example: rename _part-m-00004 to part-m-00002
    for (FileStatus dir : dirs) {
      Path path = dir.getPath();
      Path renamedPath = new Path(path.getParent(), "_" + path.getName());
      
      // read auxiliary metadata file (per task) that tells which taskId 
      // processed which split# aka solrShard
      Path solrShardNumberFile = new Path(renamedPath, TreeMergeMapper.SOLR_SHARD_NUMBER);
      InputStream in = fs.open(solrShardNumberFile);
      byte[] bytes = ByteStreams.toByteArray(in);
      in.close();
      Preconditions.checkArgument(bytes.length > 0);
      int solrShard = Integer.parseInt(new String(bytes, StandardCharsets.UTF_8));
      if (!delete(solrShardNumberFile, false, fs)) {
        return false;
      }
      
      // same as FileOutputFormat.NUMBER_FORMAT
      NumberFormat numberFormat = NumberFormat.getInstance(Locale.ENGLISH);
      numberFormat.setMinimumIntegerDigits(5);
      numberFormat.setGroupingUsed(false);
      Path finalPath = new Path(renamedPath.getParent(), dirPrefix + "-m-" + numberFormat.format(solrShard));
      
      LOG.info("MTree merge renaming solr shard: " + solrShard + " from dir: " + dir.getPath() + " to dir: " + finalPath);
      if (!rename(renamedPath, finalPath, fs)) {
        return false;
      }
    }
    return true;
  }


  private boolean waitForCompletion(Job job, boolean isVerbose) 
      throws IOException, InterruptedException, ClassNotFoundException {
    
    LOG.debug("Running job: " + getJobInfo(job));
    boolean success = job.waitForCompletion(isVerbose);
    if (!success) {
      LOG.error("Job failed! " + getJobInfo(job));
    }
    return success;
  }

  private void goodbye(Job job, Instant startTime) {
    Instant endTime = Instant.now();
    if (job != null) {
      LOG.info("Succeeded with job: " + getJobInfo(job));
    }
    LOG.info("Success. Done. Program took {}. Goodbye.", Duration.between(startTime, endTime));
  }

  private String getJobInfo(Job job) {
    return "jobName: " + job.getJobName() + ", jobId: " + job.getJobID();
  }
  
  private boolean rename(Path src, Path dst, FileSystem fs) throws IOException {
    boolean success = fs.rename(src, dst);
    if (!success) {
      LOG.error("Cannot rename " + src + " to " + dst);
    }
    return success;
  }
  
  private boolean delete(Path path, boolean recursive, FileSystem fs) throws IOException {
    boolean success = fs.delete(path, recursive);
    if (!success) {
      LOG.error("Cannot delete " + path);
    }
    return success;
  }

  // same as IntMath.divide(p, q, RoundingMode.CEILING)
  private long ceilDivide(long p, long q) {
    long result = p / q;
    if (p % q != 0) {
      result++;
    }
    return result;
  }
  
  /**
   * Returns <tt>log<sub>base</sub>value</tt>.
   */
  private double log(double base, double value) {
    return Math.log(value) / Math.log(base);
  }

  private String secondsToHumanReadable(float secs) {
    return String.format("%d:%02d", (int) secs/60, (int) secs%60);
  }

}
