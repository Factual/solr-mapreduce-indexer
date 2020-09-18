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

import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.solr.hadoop.util.HeartBeater;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.shaded.apache.solr.client.solrj.SolrServerException;
import org.shaded.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.shaded.apache.solr.common.SolrException;
import org.shaded.apache.solr.common.SolrInputDocument;
import org.shaded.apache.solr.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrRecordWriter<K, V> extends RecordWriter<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String DEFAULT_CORE_NAME = "core1";
  
  public static final String SOLR_HOME_DIR = "solr_home";

  public static final String DATA_DIR = "data";

  public final static List<String> allowedConfigDirectories = new ArrayList<>(
          Arrays.asList("conf", "lib", "solr.xml", DEFAULT_CORE_NAME));

  public final static Set<String> requiredConfigDirectories = new HashSet<>();

  static {
    requiredConfigDirectories.add("conf");
  }

  /**
   * Return the list of directories names that may be included in the
   * configuration data passed to the tasks.
   *
   * @return an UnmodifiableList of directory names
   */
  public static List<String> getAllowedConfigDirectories() {
    return Collections.unmodifiableList(allowedConfigDirectories);
  }

  /**
   * check if the passed in directory is required to be present in the
   * configuration data set.
   *
   * @param directory The directory to check
   * @return true if the directory is required.
   */
  public static boolean isRequiredConfigDirectory(final String directory) {
    return requiredConfigDirectories.contains(directory);
  }

  /**
   * The path that the final index will be written to
   */
  /**
   * The location in a local temporary directory that the index is built in.
   */

  private final HeartBeater heartBeater;
  private final BatchWriter batchWriter;
  private final List<SolrInputDocument> batch;
  private final int batchSize;
  private long numDocsWritten = 0;
  private long nextLogTime = System.nanoTime();
  private final Path outputShardDir;
  private final Path solrHomeDir;

  private static final HashMap<TaskID, Reducer<?, ?, ?, ?>.Context> contextMap = new HashMap<>();

  public SolrRecordWriter(TaskAttemptContext context, Path outputShardDir, int batchSize) throws IOException {
    this.batchSize = batchSize;
    this.batch = new ArrayList<>(batchSize);
    Configuration conf = context.getConfiguration();

    // setLogLevel("org.apache.solr.core", "WARN");
    // setLogLevel("org.apache.solr.update", "WARN");
    heartBeater = new HeartBeater(context);
    try {
      heartBeater.needHeartBeat();

      this.outputShardDir = outputShardDir;
      FileSystem fs = outputShardDir.getFileSystem(conf);
      String outputId = outputShardDir.getParent().getName();
      solrHomeDir = findAndCopySolrConf(conf, outputId);
      EmbeddedSolrServer solr = createEmbeddedSolrServerWithHome(conf, outputShardDir, solrHomeDir);
      batchWriter = new BatchWriter(solr, batchSize,
              context.getTaskAttemptID().getTaskID(),
              SolrOutputFormat.getSolrWriterThreadCount(conf),
              SolrOutputFormat.getSolrWriterQueueSize(conf));

      Path outputSolrHomeDir = new Path(outputShardDir, SOLR_HOME_DIR);
      fs.copyFromLocalFile(new Path(solr.getCoreContainer().getSolrHome()), outputSolrHomeDir);

    } finally {
      heartBeater.cancelHeartBeat();
    }
  }

  public static EmbeddedSolrServer createEmbeddedSolrServerWithHome(Configuration conf, Path outputShardDir, Path solrHomeDir)
          throws IOException {
    FileSystem fs = outputShardDir.getFileSystem(conf);
    
    LOG.info("Creating embedded Solr server with solrHomeDir: " + solrHomeDir + ", fs: " + fs + ", outputShardDir: " + outputShardDir);

    debugLs(conf, solrHomeDir);

    Path solrDataDir = new Path(outputShardDir, DATA_DIR);
    
    String dataDirStr = solrDataDir.toUri().toString();

    SolrResourceLoader loader = new SolrResourceLoader(Paths.get(solrHomeDir.toString()), null, null);

    LOG.info(String
            .format(Locale.ENGLISH,
                    "Constructed instance information solr.home %s (%s), instance dir %s, conf dir %s, writing index to solr.data.dir %s, with permdir %s",
                    solrHomeDir, solrHomeDir.toUri(), loader.getInstancePath(),
                    loader.getConfigDir(), dataDirStr, outputShardDir));

    // TODO: This is fragile and should be well documented
    System.setProperty("solr.directoryFactory", HdfsDirectoryFactory.class.getName());
    System.setProperty("solr.lock.type", DirectoryFactory.LOCK_TYPE_HDFS);
    System.setProperty("solr.hdfs.nrtcachingdirectory.enable", "false");
    System.setProperty("solr.hdfs.blockcache.enabled", "false");
    System.setProperty("solr.hdfs.blockcache.read.enabled", "false");
    System.setProperty("solr.autoCommit.maxTime", "600000");
    System.setProperty("solr.autoSoftCommit.maxTime", "-1");

    CoreContainer container = new CoreContainer(loader);
    container.load();
    SolrCore core;
    core = container.create(DEFAULT_CORE_NAME, ImmutableMap.of(CoreDescriptor.CORE_DATADIR, dataDirStr));

    if (!(core.getDirectoryFactory() instanceof HdfsDirectoryFactory)) {
      throw new UnsupportedOperationException(
              "Invalid configuration with : " + core.getDirectoryFactory().getClass().getName() + ". Currently, the only DirectoryFactory supported is "
              + HdfsDirectoryFactory.class.getSimpleName());
    }

    return new EmbeddedSolrServer(container, DEFAULT_CORE_NAME);
  }

  public static void removeLocalCoreProperties(Configuration conf, Path solrHomeDir) throws IOException {
    // clean up core.properties so CoreContainer can load it
    LocalFileSystem lfs = FileSystem.getLocal(conf);
    Path coreProperties = new Path(new Path(solrHomeDir, DEFAULT_CORE_NAME), CorePropertiesLocator.PROPERTIES_FILENAME);
    if (lfs.delete(coreProperties, false)) {
      LOG.info("deleted {}", CorePropertiesLocator.PROPERTIES_FILENAME);
    }
  }

  private static void debugLs(Configuration conf, Path path) throws IOException {
    LocalFileSystem fs = FileSystem.getLocal(conf);
    RemoteIterator<LocatedFileStatus> files = fs.listFiles(path, true);
    while (files.hasNext()) {
      LocatedFileStatus file = files.next();
      LOG.info("ls: {}", file.getPath());
    }
  }

  private static Path findAndCopySolrConf(Configuration conf, String outputId) throws IOException {
    LocalFileSystem fs = FileSystem.getLocal(conf);
    Path solrHomeDir = new Path("./solrHome-" + outputId);

    // DistributedCache.getLocalCacheArchives path seems write protected now
    Path solrHomeCache = SolrRecordWriter.findSolrConfig(conf, outputId);
    LOG.info("copy solr config from {} to {}", solrHomeCache, solrHomeDir);
    fs.copyFromLocalFile(false, solrHomeCache, solrHomeDir);

    return solrHomeDir;
  }

  public static void incrementCounter(TaskID taskId, String groupName, String counterName, long incr) {
    Reducer<?, ?, ?, ?>.Context context = contextMap.get(taskId);
    if (context != null) {
      context.getCounter(groupName, counterName).increment(incr);
    }
  }

  public static void incrementCounter(TaskID taskId, Enum<?> counterName, long incr) {
    Reducer<?, ?, ?, ?>.Context context = contextMap.get(taskId);
    if (context != null) {
      context.getCounter(counterName).increment(incr);
    }
  }

  public static void addReducerContext(Reducer<?, ?, ?, ?>.Context context) {
    TaskID taskID = context.getTaskAttemptID().getTaskID();
    contextMap.put(taskID, context);
  }

  public static Path findSolrConfig(Configuration conf, String outputId) throws IOException {
    // FIXME when mrunit supports the new cache apis
    //URI[] localArchives = context.getCacheArchives();
    Path[] localArchives = DistributedCache.getLocalCacheArchives(conf);
    for (Path unpackedDir : localArchives) {
      if (outputId == null) {
        if (unpackedDir.getName().equals(SolrOutputFormat.getZipName(conf))) {
          LOG.info("Using this unpacked directory as solr home: {}", unpackedDir);
          return unpackedDir;
        }
      } else {
        if (unpackedDir.getName().split("\\.", 2)[0].equals(outputId)) {
          LOG.info("Using this unpacked directory as solr home: {}", unpackedDir);
          return unpackedDir;
        }
      }
    }
    throw new IOException(String.format(Locale.ENGLISH,
        "No local cache archives, where is setupOk:%s zipName:%s localArchives:%s outputId:%s",
        SolrOutputFormat.getSetupOk(),
        SolrOutputFormat.getZipName(conf),
        String.join(",", conf.get(MRJobConfig.CACHE_LOCALARCHIVES)),
        outputId));
  }

  /**
   * Write a record. This method accumulates records in to a batch, and when
   * {@link #batchSize} items are present flushes it to the indexer. The writes
   * can take a substantial amount of time, depending on {@link #batchSize}. If
   * there is heavy disk contention the writes may take more than the 600 second
   * default timeout.
   */
  @Override
  public void write(K key, V value) throws IOException {
    if (value == null) {
      return;
    }
    heartBeater.needHeartBeat();
    try {
      try {
        SolrInputDocumentWritable sidw = (SolrInputDocumentWritable) value;

        batch.add(sidw.getSolrInputDocument());
        if (batch.size() >= batchSize) {
          batchWriter.queueBatch(batch);
          numDocsWritten += batch.size();
          if (System.nanoTime() >= nextLogTime) {
            LOG.info("docsWritten: {}", numDocsWritten);
            nextLogTime += TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
          }
          batch.clear();
        }
      } catch (SolrServerException e) {
        throw new IOException(e);
      }
    } finally {
      heartBeater.cancelHeartBeat();
    }

  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    if (context != null) {
      heartBeater.setProgress(context);
    }
    try {
      heartBeater.needHeartBeat();
      if (batch.size() > 0) {
        batchWriter.queueBatch(batch);
        numDocsWritten += batch.size();
        batch.clear();
      }
      LOG.info("docsWritten: {}", numDocsWritten);
      batchWriter.close(context);

      if (context != null) {
        Configuration conf = context.getConfiguration();

        removeLocalCoreProperties(conf, solrHomeDir);

        LOG.info("reload solr at {}", solrHomeDir);
        // Ensure this directory can be read back without altering hdfs index (cleans up some index files)
        try (EmbeddedSolrServer solr = createEmbeddedSolrServerWithHome(conf, outputShardDir, solrHomeDir)) {
          LOG.info("solr loaded 2nd time");
        } catch (SolrException e) {
          // TODO make this configurable to cause failure
          LOG.error("failed to reload solr", e);
        }
        removeLocalCoreProperties(conf, solrHomeDir);
      } else {
        LOG.error("no context found to validate solr");
      }

    } catch (SolrServerException | InterruptedException e) {
      throw new IOException(e);
    } finally {
      heartBeater.cancelHeartBeat();
      heartBeater.close();
    }

    context.setStatus("Done");
  }
}
