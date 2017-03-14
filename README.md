# solr mapreduce indexer

A copy of the lucene-solr Solr MapReduce contrib project.

It's repackaged as a maven project using the shade plugin to avoid package and manifest conflicts.  This allows us to run it on a hadoop cluster which will typically have conflicting older solr and lucene jars on the classpath.

This one is built with solr 6.4.2, which is, at the moment, current.

## known issues

- The tree merge functionality, even in its original state, fails and should be avoided.  To do this, specify the same number of reducers as you want in final shards.  This makes it take longer but without the pesky mysterious failure.




