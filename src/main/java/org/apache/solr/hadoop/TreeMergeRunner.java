package org.apache.solr.hadoop;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.text.NumberFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.solr.hadoop.MapReduceIndexerToolArgumentParser.Options;
import org.apache.solr.hadoop.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TreeMergeRunner {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public TreeMergeRunner() {
  }

  public int run(Path outputReduceDir, Options options, Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
    Path outputTreeMergeStep = new Path(options.outputDir, "mtree-merge-output");
    FileSystem fs = options.outputDir.getFileSystem(conf);
   

    int mtreeMergeIterations = 0;
    int reducers = options.reducers;

    if (reducers > options.shards) {
      mtreeMergeIterations = (int) Math.round(log(options.fanout, reducers / options.shards));
    }
    LOG.debug("MTree merge iterations to do: {}", mtreeMergeIterations);
    int mtreeMergeIteration = 1;
    //conf.reloadConfiguration();
    
    while (reducers > options.shards) { // run a mtree merge iteration
      Instant startTime = Instant.now();
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
      Path fullInputList = new Path(inputStepDir, MapReduceIndexerTool.FULL_INPUT_LIST);
      LOG.debug("MTree merge iteration {}/{}: Creating input list file for mappers {}", new Object[]{mtreeMergeIteration, mtreeMergeIterations, fullInputList});
      long numFiles = createTreeMergeInputDirList(outputReduceDir, fs, fullInputList, mergeTreeJob);
      if (numFiles != reducers) {
        throw new IllegalStateException("Not same reducers: " + reducers + ", numFiles: " + numFiles);
      }
      NLineInputFormat.addInputPath(mergeTreeJob, fullInputList);
      NLineInputFormat.setNumLinesPerSplit(mergeTreeJob, options.fanout);
      FileOutputFormat.setOutputPath(mergeTreeJob, outputTreeMergeStep);

      LOG.info("MTree merge iteration {}/{}: Merging {} shards into {} shards using fanout {}", new Object[]{
        mtreeMergeIteration, mtreeMergeIterations, reducers, (reducers / options.fanout), options.fanout});
   
      if (!Utils.waitForCompletion(mergeTreeJob, options.isVerbose)) {
        return -1; // job failed
      }
      if (!renameTreeMergeShardDirs(outputTreeMergeStep, mergeTreeJob, fs)) {
        return -1;
      }
      Instant endTime = Instant.now();
      LOG.info("MTree merge iteration {}/{}: Done. Merging {} shards into {} shards using fanout {} took {}",
              new Object[]{mtreeMergeIteration, mtreeMergeIterations, reducers, (reducers / options.fanout), options.fanout, Duration.between(startTime, endTime)});

      if (!Utils.delete(outputReduceDir, true, fs)) {
        return -1;
      }
      if (!Utils.rename(outputTreeMergeStep, outputReduceDir, fs)) {
        return -1;
      }
      assert reducers % options.fanout == 0;
      reducers = reducers / options.fanout;
      mtreeMergeIteration++;
    }
    assert reducers == options.shards;
    return mtreeMergeIterations;
  }

  private double log(double base, double value) {
    return Math.log(value) / Math.log(base);
  }

  private int createTreeMergeInputDirList(Path outputReduceDir, FileSystem fs, Path fullInputList, Job job)
          throws FileNotFoundException, IOException {

    FileStatus[] dirs = Utils.listSortedOutputShardDirs(outputReduceDir, fs, job);
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
      if (!Utils.rename(path, renamedPath, fs)) {
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
      if (!Utils.delete(solrShardNumberFile, false, fs)) {
        return false;
      }

      // same as FileOutputFormat.NUMBER_FORMAT
      NumberFormat numberFormat = NumberFormat.getInstance(Locale.ENGLISH);
      numberFormat.setMinimumIntegerDigits(5);
      numberFormat.setGroupingUsed(false);
      Path finalPath = new Path(renamedPath.getParent(), dirPrefix + "-m-" + numberFormat.format(solrShard));

      LOG.info("MTree merge renaming solr shard: " + solrShard + " from dir: " + dir.getPath() + " to dir: " + finalPath);
      if (!Utils.rename(renamedPath, finalPath, fs)) {
        return false;
      }
    }
    return true;
  }

}
