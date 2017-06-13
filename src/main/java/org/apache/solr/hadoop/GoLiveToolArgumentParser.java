package org.apache.solr.hadoop;


import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.solr.hadoop.util.PathArgumentType;
import org.apache.solr.hadoop.util.ToolRunnerHelpFormatter;
import org.apache.solr.hadoop.util.ZooKeeperInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.impl.action.HelpArgumentAction;
import net.sourceforge.argparse4j.impl.choice.RangeArgumentChoice;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentGroup;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class GoLiveToolArgumentParser {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public static class GoLiveOptions {

    public Integer goLiveThreads;
    public Path inputDir;
    public Integer shards;
    public ZookeeperOptions zkOptions;
    public Integer maxShards;
    
    public GoLiveOptions() {
    }
    
    public GoLiveOptions(Path inputDir, Integer shards, Integer goLiveThreads, ZookeeperOptions zkOptions) {
      this.inputDir = inputDir;
      this.shards = shards;
      this.goLiveThreads = goLiveThreads;
      this.zkOptions = zkOptions;
    }
    
  }
  
  public Integer parseArgs(String[] args, Configuration conf, GoLiveOptions opts) throws ArgumentParserException {
    assert args != null;
    assert conf != null;
    assert opts != null;

    if (args.length == 0) {
      args = new String[]{"--help"};
    }

    ArgumentParser parser = ArgumentParsers
            .newArgumentParser("hadoop [GenericOptions]... jar *.jar ", false)
            .defaultHelp(true)
            .description(
              "Make hdfs solr index live"
            );
    parser.addArgument("--help", "-help", "-h")
            .help("Show this help message and exit")
            .action(new HelpArgumentAction() {
              @Override
              public void run(ArgumentParser parser, Argument arg, Map<String, Object> attrs, String flag, Object value) throws ArgumentParserException {
                parser.printHelp();
                System.out.println();
                System.out.print(ToolRunnerHelpFormatter.getGenericCommandUsage());
                throw new FoundHelpArgument(); // Trick to prevent processing of any remaining arguments
              }
            });

    ArgumentGroup requiredGroup = parser.addArgumentGroup("Required arguments");

    Argument inputDirArg = requiredGroup.addArgument("--input-dir")
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
            .help("HDFS directory of input Solr indexes to make live. Inside there one output directory per shard will be generated. "
                    + "Example: hdfs://c2202.mycompany.com/user/$USER/test");

    ArgumentGroup clusterInfoGroup = parser
            .addArgumentGroup("Cluster arguments")
            .description(
                    "Arguments that provide information about your Solr cluster. "
                    + "If you are building shards for a SolrCloud cluster, pass the --zk-host argument. "
                            + "If you are building shards for "
                            + "a Non-SolrCloud cluster, pass the --shard-url argument one or more times. To build indexes for "
                            + "a replicated Non-SolrCloud cluster with --shard-url, pass replica urls consecutively and also pass --shards. "
                            + "Using --go-live requires either --zk-host or --shard-url.");

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
                    + "\n"
                            + "If --solr-home-dir is not specified, the Solr home directory for the collection "
                            + "will be downloaded from this ZooKeeper ensemble.");

    Argument shardUrlsArg = clusterInfoGroup.addArgument("--shard-url")
            .metavar("URL")
            .type(String.class)
            .action(Arguments.append())
            .help("Solr URL to merge resulting shard into if using --go-live. "
                    + "Example: http://solr001.mycompany.com:8983/solr/collection1. "
                    + "Multiple --shard-url arguments can be specified, one for each desired shard. "
                    + "If you are merging shards into a SolrCloud cluster, use --zk-host instead.");

    Argument shardsArg = clusterInfoGroup.addArgument("--shards")
        .metavar("INTEGER")
        .type(Integer.class)
        .choices(new RangeArgumentChoice(1, Integer.MAX_VALUE))
        .help("Number of output shards to generate.");

    ArgumentGroup goLiveGroup = parser.addArgumentGroup("Go live arguments")
            .description("Arguments for merging the shards that are built into a live Solr cluster. "
                    + "Also see the Cluster arguments.");

    Argument collectionArg = goLiveGroup.addArgument("--collection")
            .metavar("STRING")
            .help("The SolrCloud collection to merge shards into when using --go-live and --zk-host. Example: collection1");

    Argument goLiveThreadsArg = goLiveGroup.addArgument("--go-live-threads")
            .metavar("INTEGER")
            .type(Integer.class)
            .choices(new RangeArgumentChoice(1, Integer.MAX_VALUE))
            .setDefault(1000)
            .help("Tuning knob that indicates the maximum number of live merges to run in parallel at one time.");

    Namespace ns;
    try {
      ns = parser.parseArgs(args);
    } catch (FoundHelpArgument e) {
      return 0;
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      return 1;
    }

    LOG.debug("Parsed command line args: {}", ns);
    opts.inputDir = (Path) ns.get(inputDirArg.getDest());
    opts.shards = ns.getInt(shardsArg.getDest());
    opts.goLiveThreads = ns.getInt(goLiveThreadsArg.getDest());
    opts.zkOptions = new ZookeeperOptions(ns.getString(zkHostArg.getDest()), ns.getString(collectionArg.getDest()), buildShardUrls(ns.getList(shardUrlsArg.getDest()), opts.shards));
    
    return null;
  }

  static List<List<String>> buildShardUrls(List<Object> urls, Integer numShards) {
    if (urls == null) return null;
    return  Lists.partition(
              urls.stream()
                  .map(u -> u.toString())
                  .collect(Collectors.toList()),numShards);   
  }
  
  
  public static void verifyGoLiveArgs(GoLiveOptions opts, ArgumentParser parser) throws ArgumentParserException {
    if (opts.zkOptions.zkHost == null && opts.zkOptions.shardUrls == null) {
      throw new ArgumentParserException("--go-live requires that you also pass --shard-url or --zk-host", parser);
    }
    
    if (opts.zkOptions.zkHost != null && opts.zkOptions.collection == null) {
      throw new ArgumentParserException("--zk-host requires that you also pass --collection", parser);
    }
    
    if (opts.zkOptions.zkHost != null) {
      return;
      // verify structure of ZK directory later, to avoid checking run-time errors during parsing.
    } else if (opts.zkOptions.shardUrls != null) {
      if (opts.zkOptions.shardUrls.isEmpty()) {
        throw new ArgumentParserException("--shard-url requires at least one URL", parser);
      }
    } else if (opts.shards != null) {
      if (opts.shards <= 0) {
        throw new ArgumentParserException("--shards must be a positive number: " + opts.shards, parser);
      }
    } else {
      throw new ArgumentParserException("You must specify one of the following (mutually exclusive) arguments: "
          + "--zk-host or --shard-url or --shards", parser);
    }

    if (opts.zkOptions.shardUrls != null) {
      opts.shards = opts.zkOptions.shardUrls.size();
    }
    
    assert opts.shards != null;
    assert opts.shards > 0;
  }

  /**
   * Marker trick to prevent processing of any remaining arguments once --help
   * option has been parsed
   */
  private static final class FoundHelpArgument extends RuntimeException {
  }

}
