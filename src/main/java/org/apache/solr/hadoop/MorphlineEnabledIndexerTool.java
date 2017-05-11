package org.apache.solr.hadoop;

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
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.solr.hadoop.MapReduceIndexerToolArgumentParser.Options;
import org.apache.solr.hadoop.morphline.MorphlineMapRunner;
import org.apache.solr.hadoop.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sourceforge.argparse4j.inf.ArgumentParserException;

public class MorphlineEnabledIndexerTool extends MapReduceIndexerTool {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String FULL_INPUT_LIST = "full-input-list.txt";
  static final int MAX_FILES_TO_RANDOMIZE_IN_MEMORY = 10000000;
  static final String MAIN_MEMORY_RANDOMIZATION_THRESHOLD = MapReduceIndexerTool.class.getName() + ".mainMemoryRandomizationThreshold";

  private long numFiles;
  private FileSystem fs;
  private Path fullInputList;
  private int realMappers;
  
  @Override
  public int setupIndexing(Job job, Options options) throws Exception {
    fs = options.outputDir.getFileSystem(job.getConfiguration());

    if (options.morphlineFile == null) {
      throw new ArgumentParserException("Argument --morphline-file is required", null);
    }
    int mappers = new JobClient(job.getConfiguration()).getClusterStatus().getMaxMapTasks();

    LOG.info("Cluster reports {} mapper slots", mappers);

    if (options.mappers == -1) {
      mappers = 8 * mappers; // better accommodate stragglers
    } else {
      mappers = options.mappers;
    }
    if (mappers <= 0) {
      throw new IllegalStateException("Illegal number of mappers: " + mappers);
    }
    options.mappers = mappers;
    
    Path outputStep1Dir = new Path(options.outputDir, "tmp1");
    Path outputStep2Dir = new Path(options.outputDir, "tmp2");

    fullInputList = new Path(outputStep1Dir, FULL_INPUT_LIST);

    LOG.debug("Creating list of input files for mappers: {}", fullInputList);
    numFiles = addInputFiles(options.inputFiles, options.inputLists, fullInputList, job.getConfiguration());
    if (numFiles == 0) {
      LOG.info("No input files found - nothing to process");
      return 0;
    }
    int numLinesPerSplit = (int) Utils.ceilDivide(numFiles, mappers);
    if (numLinesPerSplit < 0) { // numeric overflow from downcasting long to int?
      numLinesPerSplit = Integer.MAX_VALUE;
    }
    numLinesPerSplit = Math.max(1, numLinesPerSplit);

    realMappers = Math.min(mappers, (int) Utils.ceilDivide(numFiles, numLinesPerSplit));
    MapReduceIndexerTool.calculateNumReducers(job, options, realMappers);
    job.getConfiguration().set(MorphlineMapRunner.MORPHLINE_FILE_PARAM, options.morphlineFile.getName());

    int reducers = options.reducers;
    LOG.info("Using these parameters: "
            + "numFiles: {}, mappers: {}, realMappers: {}, reducers: {}, shards: {}, fanout: {}, maxSegments: {}",
            new Object[]{numFiles, mappers, realMappers, reducers, options.shards, options.fanout, options.maxSegments});

    LOG.info("Randomizing list of {} input files to spread indexing load more evenly among mappers", numFiles);
    Instant startTime = Instant.now();
    int maxFilesToRandomizeInMemory = job.getConfiguration().getInt(MAIN_MEMORY_RANDOMIZATION_THRESHOLD, MAX_FILES_TO_RANDOMIZE_IN_MEMORY);
    if (numFiles < maxFilesToRandomizeInMemory) {
      // If there are few input files reduce latency by directly running main memory randomization 
      // instead of launching a high latency MapReduce job
      LOG.info("Randomizing list of input files in memory because there are fewer than the in-memory limit of {} files", maxFilesToRandomizeInMemory);
      Utils.randomizeFewInputFiles(fs, outputStep2Dir, fullInputList);
    } else {
      // Randomize using a MapReduce job. Use sequential algorithm below a certain threshold because there's no
      // benefit in using many parallel mapper tasks just to randomize the order of a few lines each
      int numLinesPerRandomizerSplit = Math.max(10 * 1000 * 1000, numLinesPerSplit);
      Job randomizerJob = Utils.randomizeManyInputFiles(job.getConfiguration(), fullInputList, outputStep2Dir, numLinesPerRandomizerSplit);
      if (!Utils.waitForCompletion(randomizerJob, options.isVerbose)) {
        return -1; // job failed
      }
    }
    Instant endTime = Instant.now();
    LOG.info("Done. Randomizing list of {} input files took {}", numFiles, Duration.between(startTime, endTime));

    job.setInputFormatClass(NLineInputFormat.class);
    NLineInputFormat.addInputPath(job, outputStep2Dir);
    NLineInputFormat.setNumLinesPerSplit(job, numLinesPerSplit);

    LOG.info("Indexing {} files using {} real mappers into {} reducers", new Object[]{numFiles, realMappers, reducers});
    
    // continue
    return 1;
    
  }

  @Override
  public void attemptDryRun(Job job, Options options, Instant programStart) throws Exception {
    MorphlineMapRunner runner = setupMorphline(job, options);
    
    if (options.isDryRun && runner != null) {
      LOG.info("Indexing {} files in dryrun mode", numFiles);
      Instant startTime = Instant.now();
      dryRun(job, runner, fs, fullInputList);
      Instant endTime = Instant.now();
      LOG.info("Done. Indexing {} files in dryrun mode took {}", numFiles, Duration.between(startTime, endTime));
      Utils.goodbye(null, programStart);
    }
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
  
  @Override
  public void reportIndexingDone(Options options, Duration timeSpent) {
    LOG.info("Done. Indexing {} files using {} real mappers into {} reducers took {}", new Object[]{numFiles, realMappers, options.reducers, timeSpent});
  }

  /**
   * Add the specified file to the input set, if path is a directory then add
   * the files contained therein.
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

  private MorphlineMapRunner setupMorphline(Job job, MapReduceIndexerToolArgumentParser.Options options) throws IOException, URISyntaxException {
    if (options.morphlineId != null) {
      job.getConfiguration().set(MorphlineMapRunner.MORPHLINE_ID_PARAM, options.morphlineId);
    }
    MapReduceIndexerTool.addDistributedCacheFile(options.morphlineFile, job.getConfiguration());
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
  private void dryRun(Job job, MorphlineMapRunner runner, FileSystem fs, Path fullInputList) throws IOException {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(fullInputList), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        runner.map(line, job.getConfiguration(), null);
      }
      runner.cleanup();
    }
  }

}
