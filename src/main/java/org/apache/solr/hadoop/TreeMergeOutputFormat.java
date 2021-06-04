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

import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.hadoop.util.HeartBeater;
import org.apache.solr.hadoop.util.Utils;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;

import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.misc.IndexMergeTool;
import org.apache.lucene.store.Directory;
import org.apache.solr.store.hdfs.HdfsDirectory;
import org.apache.solr.update.SolrIndexWriter;
import org.apache.solr.util.RTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * See {@link IndexMergeTool}.
 */
public class TreeMergeOutputFormat extends FileOutputFormat<Text, NullWritable> {


  @Override
  public RecordWriter getRecordWriter(TaskAttemptContext context) throws IOException {
    Utils.getLogConfigFile(context.getConfiguration());
    Path workDir = getDefaultWorkFile(context, "");
    return new TreeMergeRecordWriter(context, workDir);
  }

  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class TreeMergeRecordWriter extends RecordWriter<Text, NullWritable> {

    private final Path workDir;
    private final List<Path> shards = new ArrayList();
    private final HeartBeater heartBeater;
   

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public TreeMergeRecordWriter(TaskAttemptContext context, Path workDir) {
      this.workDir = new Path(workDir, "data/index");
      this.heartBeater = new HeartBeater(context);
      

      //without this setting, exceptions about the filesystem being closed will irredeemably rob you of hours of your life.  sad.
      context.getConfiguration().setBoolean("fs.hdfs.impl.disable.cache", true);
    }

    @Override
    public void write(Text key, NullWritable value) {
      LOG.info("map key: {}", key);
      heartBeater.needHeartBeat();
      try {
        Path path = new Path(key.toString());
        shards.add(path);
      } finally {
        heartBeater.cancelHeartBeat();
      }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException {

      LOG.debug("Task " + context.getTaskAttemptID() + " merging into dstDir: " + workDir + ", srcDirs: " + shards);

      writeShardNumberFile(context);

      heartBeater.needHeartBeat();
      try {
        Directory mergedIndex = new HdfsDirectory(workDir, context.getConfiguration());

        IndexWriterConfig writerConfig = new IndexWriterConfig(new WhitespaceAnalyzer())
                .setOpenMode(OpenMode.CREATE)
                .setUseCompoundFile(false)
                .setCommitOnClose(true)
                .setRAMBufferSizeMB(256);

        MergePolicy mergePolicy = writerConfig.getMergePolicy();

        LOG.debug("mergePolicy was: {}", mergePolicy);

        if (mergePolicy instanceof TieredMergePolicy) {
          ((TieredMergePolicy) mergePolicy).setNoCFSRatio(0.0);
          ((TieredMergePolicy) mergePolicy).setMaxMergeAtOnceExplicit(10000);
          ((TieredMergePolicy) mergePolicy).setMaxMergeAtOnce(10000);
          ((TieredMergePolicy) mergePolicy).setSegmentsPerTier(10000);
          ((TieredMergePolicy) mergePolicy).setSegmentsPerTier(10000);

        } else if (mergePolicy instanceof LogMergePolicy) {
          ((LogMergePolicy) mergePolicy).setNoCFSRatio(0.0);
        }
        LOG.info("Using mergePolicy: {}", mergePolicy);

        IndexWriter writer = new IndexWriter(mergedIndex, writerConfig);

        Directory[] indexes = new Directory[shards.size()];
        for (int i = 0; i < shards.size(); i++) {
          indexes[i] = new HdfsDirectory(shards.get(i), context.getConfiguration());
        }

        context.setStatus("Logically merging " + shards.size() + " shards into one shard");
        LOG.info("Logically merging " + shards.size() + " shards into one shard: " + workDir);
        RTimer timer = new RTimer();

        try {
          writer.addIndexes(indexes);
        } catch (Exception e) {

          LOG.error("Exploded during addIndexes with :" + e + "\n" + Arrays.toString(e.getStackTrace()));
          throw e;
        }

        LOG.info("Added Indexes: {}", Arrays.toString(indexes));

        // TODO: avoid intermediate copying of files into dst directory; rename the files into the dir instead (cp -> rename) 
        // This can improve performance and turns this phase into a true "logical" merge, completing in constant time.
        // See https://issues.apache.org/jira/browse/LUCENE-4746
        timer.stop();
        if (LOG.isDebugEnabled()) {
          context.getCounter(SolrCounters.class.getName(), SolrCounters.LOGICAL_TREE_MERGE_TIME.toString()).increment((long) timer.getTime());
        }
        LOG.info("Logical merge took {}ms", timer.getTime());
        int maxSegments = context.getConfiguration().getInt(TreeMergeMapper.MAX_SEGMENTS_ON_TREE_MERGE, Integer.MAX_VALUE);
        context.setStatus("Optimizing Solr: forcing mtree merge down to " + maxSegments + " segments");
        LOG.info("Optimizing Solr: forcing tree merge down to {} segments", maxSegments);
        timer = new RTimer();
        if (maxSegments < Integer.MAX_VALUE) {
          writer.forceMerge(maxSegments);
          // TODO: consider perf enhancement for no-deletes merges: bulk-copy the postings data 
          // see http://lucene.472066.n3.nabble.com/Experience-with-large-merge-factors-tp1637832p1647046.html
        }
        timer.stop();
        if (LOG.isDebugEnabled()) {
          context.getCounter(SolrCounters.class.getName(), SolrCounters.PHYSICAL_TREE_MERGE_TIME.toString()).increment((long) timer.getTime());
        }
        LOG.info("Optimizing Solr: done forcing tree merge down to {} segments in {}ms", maxSegments, timer.getTime());

        timer = new RTimer();

        try {

          // Set Solr's commit data so the created index is usable by SolrCloud. E.g. Currently SolrCloud relies on
          // commitTimeMSec in the commit data to do replication.
          LOG.info("Setting commitData");
          writer.commit();
          SolrIndexWriter.setCommitData(writer, 0);

          LOG.info("Optimizing Solr: Closing index writer");
          writer.close();

          Path inputShard = shards.get(0);
          FileSystem fs = inputShard.getFileSystem(context.getConfiguration());
          Path solrHomeSrc = new Path(inputShard.getParent().getParent(), SolrRecordWriter.SOLR_HOME_DIR);
          LOG.info("Checking solr home directory: " + solrHomeSrc);
          if (fs.exists(solrHomeSrc)) {
            Path solrHomeDst = new Path(workDir.getParent().getParent(),  SolrRecordWriter.SOLR_HOME_DIR);
            LOG.info("Copying: " + solrHomeSrc + " to " + solrHomeDst);
            FileUtil.copy(fs, solrHomeSrc, fs, solrHomeDst, false, context.getConfiguration());
            
            LOG.info("Reading back {}", workDir.getParent().getParent());
            
            final File tempSolrDir = Files.createTempDir();
            Runtime.getRuntime().addShutdownHook(new Thread() {
              @Override
              public void run() {
                  try {
                    FileUtils.deleteDirectory(tempSolrDir);
                  } catch (IOException e) {
                  }
              }
            });      
            Path solrHomeLocalPath = new Path(tempSolrDir.getAbsolutePath());
            fs.copyToLocalFile(solrHomeDst, solrHomeLocalPath);

            SolrRecordWriter.removeLocalCoreProperties(context.getConfiguration(), new Path(solrHomeLocalPath, SolrRecordWriter.SOLR_HOME_DIR));

            // Ensure this directory can be read back without altering hdfs index (cleans up some index files)
            EmbeddedSolrServer solr = SolrRecordWriter.createEmbeddedSolrServerWithHome(context.getConfiguration(), workDir.getParent().getParent(), new Path(solrHomeLocalPath, SolrRecordWriter.SOLR_HOME_DIR));
            solr.close();
            SolrRecordWriter.removeLocalCoreProperties(context.getConfiguration(), new Path(solrHomeLocalPath, SolrRecordWriter.SOLR_HOME_DIR));
            LOG.info("Successfully read back {}", workDir.getParent().getParent());
            
          }
          
        } catch (IOException iOException) {
          LOG.error("Error while setting commit data/closing: ", iOException);
        }

        LOG.info("Optimizing Solr: Done closing index writer in {}ms", timer.getTime());
        
        context.setStatus("Done");

      } catch (Exception e) {

        LOG.error("Failed in TreeMergeRecordWriter close: ", e);
        throw e;
      } finally {
        heartBeater.cancelHeartBeat();
        heartBeater.close();
      }
    }

    /*
     * For background see MapReduceIndexerTool.renameTreeMergeShardDirs()
     * 
     * Also see MapReduceIndexerTool.run() method where it uses
     * NLineInputFormat.setNumLinesPerSplit(job, options.fanout)
     */
    private void writeShardNumberFile(TaskAttemptContext context) throws IOException {
      LOG.info("Writing shard number file");
      Preconditions.checkArgument(shards.size() > 0);
      int outputShardNum = context.getTaskAttemptID().getTaskID().getId();
      Path shardNumberFile = new Path(workDir.getParent().getParent(), TreeMergeMapper.SOLR_SHARD_NUMBER);
      LOG.info(String.format("Merging %d shards in shard %d path: %s", shards.size(), outputShardNum, shardNumberFile));

      OutputStream out = shardNumberFile.getFileSystem(context.getConfiguration()).create(shardNumberFile);
      try (Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
        writer.write(String.valueOf(outputShardNum));
        writer.flush();
      }
    }
  }
}
