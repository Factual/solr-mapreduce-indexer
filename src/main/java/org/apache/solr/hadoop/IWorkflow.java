package org.apache.solr.hadoop;

import java.time.Duration;
import java.time.Instant;

import org.apache.hadoop.mapreduce.Job;
import org.apache.solr.hadoop.MapReduceIndexerToolArgumentParser.Options;

public interface IWorkflow {
  public int setupIndexing(Job job, Options options) throws Exception;

  public void attemptDryRun(Job job, Options options, Instant programStart) throws Exception;

  public void reportIndexingDone(Options options, Duration timeSpent);
}
