package org.apache.solr.hadoop;


import java.io.File;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.solr.hadoop.dedup.RetainMostRecentUpdateConflictResolver;
import org.apache.solr.hadoop.morphline.MorphlineMapRunner;
import org.apache.solr.hadoop.util.PathArgumentType;
import org.apache.solr.hadoop.util.ToolRunnerHelpFormatter;
import org.apache.solr.hadoop.util.Utils;
import org.kitesdk.morphline.base.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.impl.action.HelpArgumentAction;
import net.sourceforge.argparse4j.impl.choice.RangeArgumentChoice;
import net.sourceforge.argparse4j.impl.type.FileArgumentType;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentGroup;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.FeatureControl;
import net.sourceforge.argparse4j.inf.Namespace;


public final class MapReduceIndexerToolArgumentParser {

  private static final String SHOW_NON_SOLR_CLOUD = "--show-non-solr-cloud";
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private boolean showNonSolrCloud = false;

  /**
   * Parses the given command line arguments.
   *
   * @param args
   * @param conf
   * @param opts
   * @return exitCode null indicates the caller shall proceed with processing,
   * non-null indicates the caller shall exit the program with the given exit
   * status code.
   */
  public Integer parseArgs(String[] args, Configuration conf, Options opts) {
    assert args != null;
    assert conf != null;
    assert opts != null;

    if (args.length == 0) {
      args = new String[]{"--help"};
    }

    showNonSolrCloud = Arrays.asList(args).contains(SHOW_NON_SOLR_CLOUD); // intercept it first

    ArgumentParser parser = ArgumentParsers
            .newArgumentParser("hadoop [GenericOptions]... jar solr-map-reduce-*.jar ", false)
            .defaultHelp(true)
            .description(
                    "MapReduce batch job driver that takes a morphline and creates a set of Solr index shards from a set of input files "
                    + "and writes the indexes into HDFS, in a flexible, scalable and fault-tolerant manner. "
                    + "It also supports merging the output shards into a set of live customer facing Solr servers, "
                    + "typically a SolrCloud. The program proceeds in several consecutive MapReduce based phases, as follows:"
                    + "\n\n"
                    + "1) Randomization phase: This (parallel) phase randomizes the list of input files in order to spread "
                    + "indexing load more evenly among the mappers of the subsequent phase."
                    + "\n\n"
                    + "2) Mapper phase: This (parallel) phase takes the input files, extracts the relevant content, transforms it "
                    + "and hands SolrInputDocuments to a set of reducers. "
                    + "The ETL functionality is flexible and "
                    + "customizable using chains of arbitrary morphline commands that pipe records from one transformation command to another. "
                    + "Commands to parse and transform a set of standard data formats such as Avro, CSV, Text, HTML, XML, "
                    + "PDF, Word, Excel, etc. are provided out of the box, and additional custom commands and parsers for additional "
                    + "file or data formats can be added as morphline plugins. "
                    + "This is done by implementing a simple Java interface that consumes a record (e.g. a file in the form of an InputStream "
                    + "plus some headers plus contextual metadata) and generates as output zero or more records. "
                    + "Any kind of data format can be indexed and any Solr documents for any kind of Solr schema can be generated, "
                    + "and any custom ETL logic can be registered and executed.\n"
                    + "Record fields, including MIME types, can also explicitly be passed by force from the CLI to the morphline, for example: "
                    + "hadoop ... -D " + MorphlineMapRunner.MORPHLINE_FIELD_PREFIX + Fields.ATTACHMENT_MIME_TYPE + "=text/csv"
                    + "\n\n"
                    + "3) Reducer phase: This (parallel) phase loads the mapper's SolrInputDocuments into one EmbeddedSolrServer per reducer. "
                    + "Each such reducer and Solr server can be seen as a (micro) shard. The Solr servers store their "
                    + "data in HDFS."
                    + "\n\n"
                    + "4) Mapper-only merge phase: This (parallel) phase merges the set of reducer shards into the number of solr "
                    + "shards expected by the user, using a mapper-only job. This phase is omitted if the number "
                    + "of shards is already equal to the number of shards expected by the user. "
                    + "\n\n"
                    + "5) Go-live phase: This optional (parallel) phase merges the output shards of the previous phase into a set of "
                    + "live customer facing Solr servers, typically a SolrCloud. "
                    + "If this phase is omitted you can explicitly point each Solr server to one of the HDFS output shard directories."
                    + "\n\n"
                    + "Fault Tolerance: Mapper and reducer task attempts are retried on failure per the standard MapReduce semantics. "
                    + "On program startup all data in the --output-dir is deleted if that output directory already exists. "
                    + "If the whole job fails you can retry simply by rerunning the program again using the same arguments."
            );

    parser.addArgument("--help", "-help", "-h")
            .help("Show this help message and exit")
            .action(new HelpArgumentAction() {
              @Override
              public void run(ArgumentParser parser, Argument arg, Map<String, Object> attrs, String flag, Object value) throws ArgumentParserException {
                parser.printHelp();
                System.out.println();
                System.out.print(ToolRunnerHelpFormatter.getGenericCommandUsage());
                //ToolRunner.printGenericCommandUsage(System.out);
                System.out.println(
                        "Examples: \n\n"
                        + "# (Re)index an Avro based Twitter tweet file:\n"
                        + "sudo -u hdfs hadoop \\\n"
                        + "  --config /etc/hadoop/conf.cloudera.mapreduce1 \\\n"
                        + "  jar target/solr-map-reduce-*.jar \\\n"
                        + "  -D 'mapred.child.java.opts=-Xmx500m' \\\n"
                        + //            "  -D 'mapreduce.child.java.opts=-Xmx500m' \\\n" + 
                        "  --log4j src/test/resources/log4j.properties \\\n"
                        + "  --morphline-file ../search-core/src/test/resources/test-morphlines/tutorialReadAvroContainer.conf \\\n"
                        + "  --solr-home-dir src/test/resources/solr/minimr \\\n"
                        + "  --output-dir hdfs://c2202.mycompany.com/user/$USER/test \\\n"
                        + "  --shards 1 \\\n"
                        + "  hdfs:///user/$USER/test-documents/sample-statuses-20120906-141433.avro\n"
                        + "\n"
                        + "# Go live by merging resulting index shards into a live Solr cluster\n"
                        + "# (explicitly specify Solr URLs - for a SolrCloud cluster see next example):\n"
                        + "sudo -u hdfs hadoop \\\n"
                        + "  --config /etc/hadoop/conf.cloudera.mapreduce1 \\\n"
                        + "  jar target/solr-map-reduce-*.jar \\\n"
                        + "  -D 'mapred.child.java.opts=-Xmx500m' \\\n"
                        + //            "  -D 'mapreduce.child.java.opts=-Xmx500m' \\\n" + 
                        "  --log4j src/test/resources/log4j.properties \\\n"
                        + "  --morphline-file ../search-core/src/test/resources/test-morphlines/tutorialReadAvroContainer.conf \\\n"
                        + "  --solr-home-dir src/test/resources/solr/minimr \\\n"
                        + "  --output-dir hdfs://c2202.mycompany.com/user/$USER/test \\\n"
                        + "  --shard-url http://solr001.mycompany.com:8983/solr/collection1 \\\n"
                        + "  --shard-url http://solr002.mycompany.com:8983/solr/collection1 \\\n"
                        + "  --go-live \\\n"
                        + "  hdfs:///user/foo/indir\n"
                        + "\n"
                        + "# Go live by merging resulting index shards into a live SolrCloud cluster\n"
                        + "# (discover shards and Solr URLs through ZooKeeper):\n"
                        + "sudo -u hdfs hadoop \\\n"
                        + "  --config /etc/hadoop/conf.cloudera.mapreduce1 \\\n"
                        + "  jar target/solr-map-reduce-*.jar \\\n"
                        + "  -D 'mapred.child.java.opts=-Xmx500m' \\\n"
                        + //            "  -D 'mapreduce.child.java.opts=-Xmx500m' \\\n" + 
                        "  --log4j src/test/resources/log4j.properties \\\n"
                        + "  --morphline-file ../search-core/src/test/resources/test-morphlines/tutorialReadAvroContainer.conf \\\n"
                        + "  --output-dir hdfs://c2202.mycompany.com/user/$USER/test \\\n"
                        + "  --zk-host zk01.mycompany.com:2181/solr \\\n"
                        + "  --collection collection1 \\\n"
                        + "  --go-live \\\n"
                        + "  hdfs:///user/foo/indir\n"
                );
                throw new FoundHelpArgument(); // Trick to prevent processing of any remaining arguments
              }
            });

    ArgumentGroup requiredGroup = parser.addArgumentGroup("Required arguments");

    Argument outputDirArg = requiredGroup.addArgument("--output-dir")
            .metavar("HDFS_URI")
            .type(new PathArgumentType(conf) {
              @Override
              public Path convert(ArgumentParser parser, Argument arg, String value) throws ArgumentParserException {
                Path path = super.convert(parser, arg, value);
                if ("hdfs".equals(path.toUri().getScheme()) && path.toUri().getAuthority() == null) {
                  // TODO: consider defaulting to hadoop's fs.default.name here or in SolrRecordWriter.createEmbeddedSolrServer()
                  throw new ArgumentParserException("Missing authority in path URI: " + path, parser);
                }
                return path;
              }
            }.verifyHasScheme().verifyIsAbsolute().verifyCanWriteParent())
            .required(true)
            .help("HDFS directory to write Solr indexes to. Inside there one output directory per shard will be generated. "
                    + "Example: hdfs://c2202.mycompany.com/user/$USER/test");

    Argument inputListArg = parser.addArgument("--input-list")
            .action(Arguments.append())
            .metavar("URI")
            //      .type(new PathArgumentType(fs).verifyExists().verifyCanRead())
            .type(Path.class)
            .help("Local URI or HDFS URI of a UTF-8 encoded file containing a list of HDFS URIs to index, "
                    + "one URI per line in the file. If '-' is specified, URIs are read from the standard input. "
                    + "Multiple --input-list arguments can be specified.");

    Argument morphlineFileArg = requiredGroup.addArgument("--morphline-file")
            .metavar("FILE")
            .type(new FileArgumentType().verifyExists().verifyIsFile().verifyCanRead())
            .required(false)
            .help("Relative or absolute path to a local config file that contains one or more morphlines. "
                    + "The file must be UTF-8 encoded. Example: /path/to/morphline.conf");

    Argument morphlineIdArg = parser.addArgument("--morphline-id")
            .metavar("STRING")
            .type(String.class)
            .help("The identifier of the morphline that shall be executed within the morphline config file "
                    + "specified by --morphline-file. If the --morphline-id option is ommitted the first (i.e. "
                    + "top-most) morphline within the config file is used. Example: morphline1");

    Argument solrHomeDirArg = nonSolrCloud(parser.addArgument("--solr-home-dir")
            .metavar("DIR")
            .type(new FileArgumentType() {
              @Override
              public File convert(ArgumentParser parser, Argument arg, String value) throws ArgumentParserException {
                File solrHomeDir = super.convert(parser, arg, value);
                File solrConfigFile = new File(new File(solrHomeDir, "conf"), "solrconfig.xml");
                new FileArgumentType().verifyExists().verifyIsFile().verifyCanRead().convert(
                        parser, arg, solrConfigFile.getPath());
                return solrHomeDir;
              }
            }.verifyIsDirectory().verifyCanRead())
            .required(false)
            .help("Relative or absolute path to a local dir containing Solr conf/ dir and in particular "
                    + "conf/solrconfig.xml and optionally also lib/ dir. This directory will be uploaded to each MR task. "
                    + "Example: src/test/resources/solr/minimr"));

    Argument updateConflictResolverArg = parser.addArgument("--update-conflict-resolver")
            .metavar("FQCN")
            .type(String.class)
            .setDefault(RetainMostRecentUpdateConflictResolver.class.getName())
            .help("Fully qualified class name of a Java class that implements the UpdateConflictResolver interface. "
                    + "This enables deduplication and ordering of a series of document updates for the same unique document "
                    + "key. For example, a MapReduce batch job might index multiple files in the same job where some of the "
                    + "files contain old and new versions of the very same document, using the same unique document key.\n"
                    + "Typically, implementations of this interface forbid collisions by throwing an exception, or ignore all but "
                    + "the most recent document version, or, in the general case, order colliding updates ascending from least "
                    + "recent to most recent (partial) update. The caller of this interface (i.e. the Hadoop Reducer) will then "
                    + "apply the updates to Solr in the order returned by the orderUpdates() method.\n"
                    + "The default RetainMostRecentUpdateConflictResolver implementation ignores all but the most recent document "
                    + "version, based on a configurable numeric Solr field, which defaults to the file_last_modified timestamp");

    Argument mappersArg = parser.addArgument("--mappers")
            .metavar("INTEGER")
            .type(Integer.class)
            .choices(new RangeArgumentChoice(-1, Integer.MAX_VALUE)) // TODO: also support X% syntax where X is an integer
            .setDefault(-1)
            .help("Tuning knob that indicates the maximum number of MR mapper tasks to use. -1 indicates use all map slots "
                    + "available on the cluster.");

    Argument reducersArg = parser.addArgument("--reducers")
            .metavar("INTEGER")
            .type(Integer.class)
            .choices(new RangeArgumentChoice(-2, Integer.MAX_VALUE)) // TODO: also support X% syntax where X is an integer
            .setDefault(-1)
            .help("Tuning knob that indicates the number of reducers to index into. "
                    + "0 is reserved for a mapper-only feature that may ship in a future release. "
                    + "-1 indicates use all reduce slots available on the cluster. "
                    + "-2 indicates use one reducer per output shard, which disables the mtree merge MR algorithm. "
                    + "The mtree merge MR algorithm improves scalability by spreading load "
                    + "(in particular CPU load) among a number of parallel reducers that can be much larger than the number "
                    + "of solr shards expected by the user. It can be seen as an extension of concurrent lucene merges "
                    + "and tiered lucene merges to the clustered case. The subsequent mapper-only phase "
                    + "merges the output of said large number of reducers to the number of shards expected by the user, "
                    + "again by utilizing more available parallelism on the cluster.");

    Argument fanoutArg = parser.addArgument("--fanout")
            .metavar("INTEGER")
            .type(Integer.class)
            .choices(new RangeArgumentChoice(2, Integer.MAX_VALUE))
            .setDefault(Integer.MAX_VALUE)
            .help(FeatureControl.SUPPRESS);

    Argument maxSegmentsArg = parser.addArgument("--max-segments")
            .metavar("INTEGER")
            .type(Integer.class)
            .choices(new RangeArgumentChoice(1, Integer.MAX_VALUE))
            .setDefault(1)
            .help("Tuning knob that indicates the maximum number of segments to be contained on output in the index of "
                    + "each reducer shard. After a reducer has built its output index it applies a merge policy to merge segments "
                    + "until there are <= maxSegments lucene segments left in this index. "
                    + "Merging segments involves reading and rewriting all data in all these segment files, "
                    + "potentially multiple times, which is very I/O intensive and time consuming. "
                    + "However, an index with fewer segments can later be merged faster, "
                    + "and it can later be queried faster once deployed to a live Solr serving shard. "
                    + "Set maxSegments to 1 to optimize the index for low query latency. "
                    + "In a nutshell, a small maxSegments value trades indexing latency for subsequently improved query latency. "
                    + "This can be a reasonable trade-off for batch indexing systems.");

    Argument fairSchedulerPoolArg = parser.addArgument("--fair-scheduler-pool")
            .metavar("STRING")
            .help("Optional tuning knob that indicates the name of the fair scheduler pool to submit jobs to. "
                    + "The Fair Scheduler is a pluggable MapReduce scheduler that provides a way to share large clusters. "
                    + "Fair scheduling is a method of assigning resources to jobs such that all jobs get, on average, an "
                    + "equal share of resources over time. When there is a single job running, that job uses the entire "
                    + "cluster. When other jobs are submitted, tasks slots that free up are assigned to the new jobs, so "
                    + "that each job gets roughly the same amount of CPU time. Unlike the default Hadoop scheduler, which "
                    + "forms a queue of jobs, this lets short jobs finish in reasonable time while not starving long jobs. "
                    + "It is also an easy way to share a cluster between multiple of users. Fair sharing can also work with "
                    + "job priorities - the priorities are used as weights to determine the fraction of total compute time "
                    + "that each job gets.");

    Argument dryRunArg = parser.addArgument("--dry-run")
            .action(Arguments.storeTrue())
            .help("Run in local mode and print documents to stdout instead of loading them into Solr. This executes "
                    + "the morphline in the client process (without submitting a job to MR) for quicker turnaround during "
                    + "early trial & debug sessions.");

    Argument log4jConfigFileArg = parser.addArgument("--log4j")
            .metavar("FILE")
            .type(new FileArgumentType().verifyExists().verifyIsFile().verifyCanRead())
            .help("Relative or absolute path to a log4j.properties config file on the local file system. This file "
                    + "will be uploaded to each MR task. Example: /path/to/log4j.properties");

    Argument verboseArg = parser.addArgument("--verbose", "-v")
            .action(Arguments.storeTrue())
            .help("Turn on verbose output.");

    parser.addArgument(SHOW_NON_SOLR_CLOUD)
            .action(Arguments.storeTrue())
            .help("Also show options for Non-SolrCloud mode as part of --help.");

    ArgumentGroup clusterInfoGroup = parser
            .addArgumentGroup("Cluster arguments")
            .description(
                    "Arguments that provide information about your Solr cluster. "
                    + nonSolrCloud("If you are building shards for a SolrCloud cluster, pass the --zk-host argument. "
                            + "If you are building shards for "
                            + "a Non-SolrCloud cluster, pass the --shard-url argument one or more times. To build indexes for "
                            + "a replicated Non-SolrCloud cluster with --shard-url, pass replica urls consecutively and also pass --shards. "
                            + "Using --go-live requires either --zk-host or --shard-url."));

    Argument zkHostArg = clusterInfoGroup.addArgument("--zk-host")
            .metavar("STRING")
            .type(String.class)
            .help("The address of a ZooKeeper ensemble being used by a SolrCloud cluster. "
                    + "This ZooKeeper ensemble will be examined to determine the number of output "
                    + "shards to create as well as the Solr URLs to merge the output shards into when using the --go-live option. "
                    + "Requires that you also pass the --collection to merge the shards into.\n"
                    + "\n"
                    + "The --zk-host option implements the same partitioning semantics as the standard SolrCloud "
                    + "Near-Real-Time (NRT) API. This enables to mix batch updates from MapReduce ingestion with "
                    + "updates from standard Solr NRT ingestion on the same SolrCloud cluster, "
                    + "using identical unique document keys.\n"
                    + "\n"
                    + "Format is: a list of comma separated host:port pairs, each corresponding to a zk "
                    + "server. Example: '127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183' If "
                    + "the optional chroot suffix is used the example would look "
                    + "like: '127.0.0.1:2181/solr,127.0.0.1:2182/solr,127.0.0.1:2183/solr' "
                    + "where the client would be rooted at '/solr' and all paths "
                    + "would be relative to this root - i.e. getting/setting/etc... "
                    + "'/foo/bar' would result in operations being run on "
                    + "'/solr/foo/bar' (from the server perspective).\n"
                    + nonSolrCloud("\n"
                            + "If --solr-home-dir is not specified, the Solr home directory for the collection "
                            + "will be downloaded from this ZooKeeper ensemble."));

    Argument shardUrlsArg = nonSolrCloud(clusterInfoGroup.addArgument("--shard-url")
            .metavar("URL")
            .type(String.class)
            .action(Arguments.append())
            .help("Solr URL to merge resulting shard into if using --go-live. "
                    + "Example: http://solr001.mycompany.com:8983/solr/collection1. "
                    + "Multiple --shard-url arguments can be specified, one for each desired shard. "
                    + "If you are merging shards into a SolrCloud cluster, use --zk-host instead."));

    Argument shardsArg = nonSolrCloud(clusterInfoGroup.addArgument("--shards")
            .metavar("INTEGER")
            .type(Integer.class)
            .choices(new RangeArgumentChoice(1, Integer.MAX_VALUE))
            .help("Number of output shards to generate."));

    ArgumentGroup goLiveGroup = parser.addArgumentGroup("Go live arguments")
            .description("Arguments for merging the shards that are built into a live Solr cluster. "
                    + "Also see the Cluster arguments.");

    Argument goLiveArg = goLiveGroup.addArgument("--go-live")
            .action(Arguments.storeTrue())
            .help("Allows you to optionally merge the final index shards into a live Solr cluster after they are built. "
                    + "You can pass the ZooKeeper address with --zk-host and the relevant cluster information will be auto detected. "
                    + nonSolrCloud("If you are not using a SolrCloud cluster, --shard-url arguments can be used to specify each SolrCore to merge "
                            + "each shard into."));

    Argument collectionArg = goLiveGroup.addArgument("--collection")
            .metavar("STRING")
            .help("The SolrCloud collection to merge shards into when using --go-live and --zk-host. Example: collection1");

    Argument goLiveThreadsArg = goLiveGroup.addArgument("--go-live-threads")
            .metavar("INTEGER")
            .type(Integer.class)
            .choices(new RangeArgumentChoice(1, Integer.MAX_VALUE))
            .setDefault(1000)
            .help("Tuning knob that indicates the maximum number of live merges to run in parallel at one time.");

    // trailing positional arguments
    Argument inputFilesArg = parser.addArgument("input-files")
            .metavar("HDFS_URI")
            .type(new PathArgumentType(conf).verifyHasScheme())
            .nargs("*")
            .setDefault()
            .help("HDFS URI of file or directory tree to index.");

    Namespace ns;
    try {
      ns = parser.parseArgs(args);
    } catch (FoundHelpArgument e) {
      return 0;
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      return 1;
    }

    opts.log4jConfigFile = (File) ns.get(log4jConfigFileArg.getDest());
    if (opts.log4jConfigFile != null) {
      Utils.configureLog4jProperties(opts.log4jConfigFile.getPath());
    }
    LOG.debug("Parsed command line args: {}", ns);

    opts.inputLists = ns.getList(inputListArg.getDest());
    if (opts.inputLists == null) {
      opts.inputLists = Collections.EMPTY_LIST;
    }
    opts.inputFiles = ns.getList(inputFilesArg.getDest());    
    opts.outputDir = (Path) ns.get(outputDirArg.getDest());
    opts.mappers = ns.getInt(mappersArg.getDest());
    opts.reducers = ns.getInt(reducersArg.getDest());
    opts.updateConflictResolver = ns.getString(updateConflictResolverArg.getDest());
    opts.fanout = ns.getInt(fanoutArg.getDest());
    opts.maxSegments = ns.getInt(maxSegmentsArg.getDest());
    opts.morphlineFile = (File) ns.get(morphlineFileArg.getDest());
    opts.morphlineId = ns.getString(morphlineIdArg.getDest());
    opts.solrHomeDir = (File) ns.get(solrHomeDirArg.getDest());
    opts.fairSchedulerPool = ns.getString(fairSchedulerPoolArg.getDest());
    opts.isDryRun = ns.getBoolean(dryRunArg.getDest());
    opts.isVerbose = ns.getBoolean(verboseArg.getDest());
    opts.shards = ns.getInt(shardsArg.getDest());
    opts.goLive = ns.getBoolean(goLiveArg.getDest());
    opts.goLiveThreads = ns.getInt(goLiveThreadsArg.getDest());
    opts.zkOptions = new ZookeeperOptions(ns.getString(zkHostArg.getDest()), ns.getString(collectionArg.getDest()), ZookeeperOptions.buildShardUrls(ns.getList(shardUrlsArg.getDest()), opts.shards));

    try {
      if (opts.reducers == 0) {
        throw new ArgumentParserException("--reducers must not be zero", parser);
      }
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      return 1;
    }

    if (opts.inputLists.isEmpty() && opts.inputFiles.isEmpty()) {
      LOG.info("No input files specified - nothing to process");
      return 0; // nothing to process
    }
    return null;
  }

  // make it a "hidden" option, i.e. the option is functional and enabled but not shown in --help output
  private Argument nonSolrCloud(Argument arg) {
    return showNonSolrCloud ? arg : arg.help(FeatureControl.SUPPRESS);
  }

  private String nonSolrCloud(String msg) {
    return showNonSolrCloud ? msg : "";
  }

  /**
   * Marker trick to prevent processing of any remaining arguments once --help
   * option has been parsed
   */
  private static final class FoundHelpArgument extends RuntimeException {
  }

  public static final class Options {

    boolean goLive;
    Integer goLiveThreads;
    public List<Path> inputLists;
    public List<Path> inputFiles;
    public Path outputDir;
    public int mappers;
    public int reducers;
    String updateConflictResolver;
    int fanout;
    Integer shards;
    int maxSegments;
    File morphlineFile;
    String morphlineId;
    public File solrHomeDir;
    String fairSchedulerPool;
    boolean isDryRun;
    File log4jConfigFile;
    boolean isVerbose;
    public ZookeeperOptions zkOptions;

  }

}
