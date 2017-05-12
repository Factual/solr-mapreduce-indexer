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

import java.io.IOException;
import java.lang.invoke.MethodHandles;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.solr.hadoop.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.solr.hadoop.GoLiveToolArgumentParser.GoLiveOptions;
import org.apache.solr.hadoop.IndexMergeToolArgumentParser.IndexMergeOptions;

/**
 * Public API for a MapReduce batch job driver that creates a set of Solr index
 * shards from a set of input files and writes the indexes into HDFS, in a
 * flexible, scalable and fault-tolerant manner. Also supports merging the
 * output shards into a set of live customer facing Solr servers, typically a
 * SolrCloud.
 */
public abstract class MapReduceIndexerTool extends Configured implements Tool {

  public static final String RESULTS_DIR = "results";
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

  public MapReduceIndexerTool() {
  }

  @Override
  public int run(String[] args) throws Exception {
    MapReduceIndexerToolArgumentParser.Options opts = new MapReduceIndexerToolArgumentParser.Options();
    Integer exitCode = new MapReduceIndexerToolArgumentParser().parseArgs(args, getConf(), opts);
    if (exitCode != null) {
      return exitCode;
    }
    return run(opts, new MorphlineEnabledIndexerTool());
  }

  /**
   * API for Java clients; visible for testing; may become a public API
   * eventually
   */
  public int run(MapReduceIndexerToolArgumentParser.Options options, IndexTool indexTool) throws Exception {

    // index
    int indexStatus = indexTool.run(options);
    if (indexStatus != 0) {
      return indexStatus;
    }

    FileSystem fs = options.outputDir.getFileSystem(getConf());
    Path outputReduceDir = new Path(options.outputDir, IndexTool.REDUCERS_DIR);
    Path outputResultsDir = new Path(options.outputDir, RESULTS_DIR);

    // attempt merge
    if (!new IndexMergeTool().mergeIfNeeded(getConf(), new IndexMergeOptions(outputReduceDir, options.outputDir, options.shards, options.fanout, options.zkOptions))) {
      return -1;
    }

    // rename files with -m and -r segments to drop those
    if (!renameIntermediateFiles(fs, outputReduceDir, SolrOutputFormat.getOutputName(Job.getInstance(getConf())))) {
      return -1;
    }

    // publish results dir    
    if (!Utils.rename(outputReduceDir, outputResultsDir, fs)) {
      return -1;
    }
    // publish to live solr server
    if (options.goLive && !new GoLiveTool().goLive(getConf(), new GoLiveOptions(outputResultsDir, options.shards, options.goLiveThreads, options.zkOptions))) {
      return -1;
    }
    
    return 0;
  }
  
  private boolean renameIntermediateFiles(FileSystem fs, Path path, String dirPrefix) throws IOException {
    // normalize output shard dir prefix, i.e.
    // rename part-r-00000 to part-00000 (stems from zero tree merge iterations)
    // rename part-m-00000 to part-00000 (stems from > 0 tree merge iterations)
    final int extraPartLength = "-m".length();
    for (FileStatus stats : fs.listStatus(path)) {
      Path srcPath = stats.getPath();
      if (stats.isDirectory() && srcPath.getName().startsWith(dirPrefix)) {
        String dstName = dirPrefix + srcPath.getName().substring(dirPrefix.length() + extraPartLength);
        Path dstPath = new Path(srcPath.getParent(), dstName);
        if (!Utils.rename(srcPath, dstPath, fs)) {
          return false;
        }
      }
    }
    return true;
  }

}
