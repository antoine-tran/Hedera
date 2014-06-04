Working with Wikipedia
======

(This documentation is still in progress and will be incrementally added soon)

Working with Wikipedia in Hedera is fairly easy. To load the datasets in Pig, simply register the library and call its various methods in package <code>org.hedera.pig.eval</code>. A new EvalFunc can also be developed by inheriting the <code>PageFunc</code> abstract class.

###### Example code in Pig######

<pre>
`REGISTER '$LIBDIR/hedera-0.1-SNAPSHOT.jar'
%DECLARE BASEDIR 'hdfs://master.ib:8020'
wiki = LOAD '$BASEDIR/$INPUT' USING org.hedera.pig.load.LiteWikipediaLoader
ewiki = FOREACH wiki GENERATE page_id, org.hedera.pig.ExtractTemplate(page_id,page_title,text);`
</pre>

Copyright
=============
Hedera uses the following library for working with Wikipedia

+ [Cloud9](https://github.com/antoine-tran/Cloud9) : Hedera uses the WikipediaPage input formats and Writable implementations from Cloud9 tools
+ [tuan4j-distributed](https://github.com/antoine-tran/maven-repo): Tuan4j-distributed is a set of utilities for handling customized Java objects and setting up jobs in Hadoop environment. It also supports bz2 compression-decompression in reading mapper inputs streams.
+ [pignlproc](https://github.com/ogrisel/pignlproc): PigNLPRoc is an execellent tool for working with Wikipedia article dumps in Pig. Hedera inherits the parsing code from PigNLPRoc and provides the same functionalities in Wikipedia revision dumps.
+ [wikihadoop](https://github.com/whym/wikihadoop): WikiHadoop provides a fast and simple way to work with Wikipedia revision in plain text manner. Unfortunately, it is poorly-documented, no longer active developed (not compatible with Pig 0.11 and CDH4.3.0 and higher), and does not provide fast and approximated reading. Hedera inherits the splitting techniques from Wikihadoop when handling XML files.
