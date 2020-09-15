package org.apache.solr.hadoop;

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
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.solr.hadoop.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;

import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.hadoop.IndexMergeToolArgumentParser.IndexMergeOptions;;

public class IndexMergeTool extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final boolean isVerbose = true;

    public IndexMergeTool() {
    }
    
    public static void main(String[] args) throws Exception {
      int res = ToolRunner.run(new Configuration(), new IndexMergeTool(), args);
      System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
      IndexMergeOptions options = new IndexMergeOptions();
      Integer exitCode = new IndexMergeToolArgumentParser().parseArgs(args, getConf(), options);
      if (exitCode != null) {
        return exitCode;
      }
      options.zkOptions.verifyZKStructure(null);
      // auto update shard count
      if (options.zkOptions.zkHost != null) {
        options.shards = options.zkOptions.shardUrls.size();
      }
      return mergeIfNeeded(getConf(), options) ? 0 : -1;
    }
    
    public boolean mergeIfNeeded(Configuration conf, IndexMergeOptions options) throws IOException, InterruptedException, ClassNotFoundException {
      int numInputShards = Utils.listSortedOutputShardDirs(conf, options.inputDir).length;
      // should shards be taken from zookeeper?
      if (numInputShards > options.shards) {
        options.fanout = Math.min(options.fanout, (int) Utils.ceilDivide(numInputShards, options.shards));
        return merge(conf, options, numInputShards);
      } else {
        LOG.info("Merging {} input shards into {} shards has no effect.", numInputShards, options.shards);
        return true;
      }
    }
    
    public boolean merge(Configuration conf, IndexMergeOptions options, int numInputShards) throws IOException, InterruptedException, ClassNotFoundException {
      // TODO: make configurable and ensure it works with globStatus.  however, not sure we want to enable this as it auto-deletes the original inputs.
      boolean mergeInPlace = false;
      
      LOG.info("The number of reducers is greater than the number of shards.  Invoking the tree merge process");
      
      Path inputDir = options.inputDir;
      Path outputDir = options.outputDir;
      int targetShards = options.shards;
      int fanout = options.fanout;

      LOG.info("outputDir: {}", outputDir);
      LOG.info("inputDir: {}", inputDir);

      FileSystem fs = outputDir.getFileSystem(conf);

      int mtreeMergeIterations = 0;

      if (numInputShards > targetShards) {
          mtreeMergeIterations = (int) Math.round(log(fanout, numInputShards / targetShards));
      }
      LOG.debug("MTree merge iterations to do: {}", mtreeMergeIterations);
      int mtreeMergeIteration = 1;
      
      while (numInputShards > targetShards) { // run a mtree merge iteration

          Path outputTreeMergeStep = new Path(outputDir, "mtree-merge-output-" + mtreeMergeIteration);

          Instant startTime = Instant.now();
          Job mergeTreeJob = Job.getInstance(conf);

          mergeTreeJob.setMapSpeculativeExecution(false);
          mergeTreeJob.setReduceSpeculativeExecution(false);

          mergeTreeJob.setJarByClass(getClass());
          mergeTreeJob.setJobName("solr-merge | " + options.inputDir + " -> " + options.outputDir);
          mergeTreeJob.setMapperClass(TreeMergeMapper.class);
          mergeTreeJob.setOutputFormatClass(TreeMergeOutputFormat.class);

          mergeTreeJob.setNumReduceTasks(0);
          mergeTreeJob.setOutputKeyClass(Text.class);
          mergeTreeJob.setOutputValueClass(NullWritable.class);
          mergeTreeJob.setInputFormatClass(NLineInputFormat.class);

          Path inputStepDir = new Path(outputDir, "mtree-merge-input-iteration" + mtreeMergeIteration);
          // TODO: figure out if this works without morphlines
          Path fullInputList = new Path(inputStepDir, MorphlineEnabledIndexerTool.FULL_INPUT_LIST);
          LOG.debug("MTree merge iteration {}/{}: Creating input list file for mappers {}", mtreeMergeIteration, mtreeMergeIterations, fullInputList);
          long numFiles = createTreeMergeInputDirList(inputDir, fs, fullInputList, mergeTreeJob);
          if (numFiles != numInputShards) {
              throw new IllegalStateException("Not same reducers: " + numInputShards + ", numFiles: " + numFiles);
          }
          NLineInputFormat.addInputPath(mergeTreeJob, fullInputList);
          NLineInputFormat.setNumLinesPerSplit(mergeTreeJob, fanout);
          FileOutputFormat.setOutputPath(mergeTreeJob, outputTreeMergeStep);

          LOG.info("MTree merge iteration {}/{}: Merging {} shards into {} shards using fanout {}", mtreeMergeIteration, mtreeMergeIterations, numInputShards, (numInputShards / fanout), fanout);

          if (!Utils.waitForCompletion(mergeTreeJob, isVerbose)) {
              return false; // job failed
          }
          if (!renameTreeMergeShardDirs(outputTreeMergeStep, mergeTreeJob, fs)) {
              return false;
          }
          Instant endTime = Instant.now();
          LOG.info("MTree merge iteration {}/{}: Done. Merging {} shards into {} shards using fanout {} took {}",
              mtreeMergeIteration, mtreeMergeIterations, numInputShards, (numInputShards / fanout), fanout, Duration.between(startTime, endTime));
          
          if (mergeInPlace) {
            if (!Utils.delete(inputDir, true, fs)) {
              return false;
            }
            if (!Utils.rename(outputTreeMergeStep, inputDir, fs)) {
                return false;
            }            
          } else {
            inputDir = new Path(outputTreeMergeStep, "part-*");
          }

          assert numInputShards % fanout == 0;
          numInputShards = numInputShards / fanout;
          mtreeMergeIteration++;
          
          if (!mergeInPlace && numInputShards == targetShards) {
            if (!Utils.rename(outputTreeMergeStep, new Path(outputDir, "final"), fs)) {
              return false;
            }
          }
      }
      assert numInputShards == targetShards;

      LOG.info("Completed {} merge iterations.", mtreeMergeIterations);

      return true;
    }

    private double log(double base, double value) {
        return Math.log(value) / Math.log(base);
    }

    private int createTreeMergeInputDirList(Path inputDir, FileSystem fs, Path fullInputList, Job job)
            throws FileNotFoundException, IOException {

        FileStatus[] dirs = Utils.listSortedOutputShardDirs(job.getConfiguration(), inputDir);
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
     * This way each doc gets assigned to the right Solr shard even with #numInputShards > #solrshards
     *
     * Example for a merge with two shards:
     *
     * part-m-00000 and part-m-00001 goes to outputShardNum = 0 and will end up in merged part-m-00000
     * part-m-00002 and part-m-00003 goes to outputShardNum = 1 and will end up in merged part-m-00001
     * part-m-00004 and part-m-00005 goes to outputShardNum = 2 and will end up in merged part-m-00002
     * ... and so on
     *
     * Also see run() method above where it uses NLineInputFormat.setNumLinesPerSplit(job,
     * fanout)
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
