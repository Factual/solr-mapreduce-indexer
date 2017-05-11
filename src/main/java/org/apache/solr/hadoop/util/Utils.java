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

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.PropertyConfigurator;

import com.google.common.annotations.Beta;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.hadoop.MapReduceIndexerTool;
import org.apache.solr.hadoop.MorphlineEnabledIndexerTool;

import org.apache.solr.hadoop.SolrOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Beta
public final class Utils {

  private static final String LOG_CONFIG_FILE = "hadoop.log4j.configuration";
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static void setLogConfigFile(File file, Configuration conf) {
    conf.set(LOG_CONFIG_FILE, file.getName());
  }

  public static void getLogConfigFile(Configuration conf) {
    String log4jPropertiesFile = conf.get(LOG_CONFIG_FILE);
    configureLog4jProperties(log4jPropertiesFile);
  }

  @SuppressForbidden(reason = "method is specific to log4j")
  public static void configureLog4jProperties(String log4jPropertiesFile) {
    if (log4jPropertiesFile != null) {
      PropertyConfigurator.configure(log4jPropertiesFile);
    }
  }

  public static String getShortClassName(Class clazz) {
    return getShortClassName(clazz.getName());
  }

  public static String getShortClassName(String className) {
    int i = className.lastIndexOf('.'); // regular class
    int j = className.lastIndexOf('$'); // inner class
    return className.substring(1 + Math.max(i, j));
  }

  public static boolean waitForCompletion(Job job, boolean isVerbose)
          throws IOException, InterruptedException, ClassNotFoundException {

    LOG.debug("Running job: " + getJobInfo(job));
    boolean success = job.waitForCompletion(isVerbose);
    if (!success) {
      LOG.error("Job failed! " + getJobInfo(job));
    }
    return success;
  }

  public static void goodbye(Job job, Instant startTime) {
    Instant endTime = Instant.now();
    if (job != null) {
      LOG.info("Succeeded with job: " + getJobInfo(job));
    }
    LOG.info("Success. Done. Program took {}. Goodbye.", Duration.between(startTime, endTime));
  }

  public static String getJobInfo(Job job) {
    return "jobName: " + job.getJobName() + ", jobId: " + job.getJobID();
  }

  public static boolean rename(Path src, Path dst, FileSystem fs) throws IOException {
    boolean success = fs.rename(src, dst);
    if (!success) {
      LOG.error("Cannot rename " + src + " to " + dst);
    }
    return success;
  }

  public static boolean delete(Path path, boolean recursive, FileSystem fs) throws IOException {
    boolean success = fs.delete(path, recursive);
    if (!success) {
      LOG.error("Cannot delete " + path);
    }
    return success;
  }

  // same as IntMath.divide(p, q, RoundingMode.CEILING)
  public static long ceilDivide(long p, long q) {
    long result = p / q;
    if (p % q != 0) {
      result++;
    }
    return result;
  }

  public static FileStatus[] listSortedOutputShardDirs(Path outputReduceDir, FileSystem fs, Job job) throws FileNotFoundException,
          IOException {

    final String dirPrefix = SolrOutputFormat.getOutputName(job);
    FileStatus[] dirs = fs.listStatus(outputReduceDir, (Path path) -> path.getName().startsWith(dirPrefix));
    for (FileStatus dir : dirs) {
      if (!dir.isDirectory()) {
        throw new IllegalStateException("Not a directory: " + dir.getPath());
      }
    }

    // use alphanumeric sort (rather than lexicographical sort) to properly handle more than 99999 shards
    Arrays.sort(dirs, (f1, f2) -> new AlphaNumericComparator().compare(f1.getPath().getName(), f2.getPath().getName()));

    return dirs;
  }
  
  public static void randomizeFewInputFiles(FileSystem fs, Path outputStep2Dir, Path fullInputList) throws IOException {    
    List<String> lines = new ArrayList();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(fullInputList), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        lines.add(line);
      }
    }
    
    Collections.shuffle(lines, new Random(421439783L)); // constant seed for reproducability
    
    // TODO: figure out if this depends on a morphline workflow
    FSDataOutputStream out = fs.create(new Path(outputStep2Dir, MorphlineEnabledIndexerTool.FULL_INPUT_LIST));
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
  public static Job randomizeManyInputFiles(Configuration baseConfig, Path fullInputList, Path outputStep2Dir, int numLinesPerSplit) 
      throws IOException {
    
    Job randomizeInputFilesJob = Job.getInstance(baseConfig);
    randomizeInputFilesJob.setJarByClass(MapReduceIndexerTool.class);
    randomizeInputFilesJob.setJobName(MapReduceIndexerTool.class.getName() + "/" + Utils.getShortClassName(LineRandomizerMapper.class));
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



}
