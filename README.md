Hedera
======

##### Efficient Processing Tool for Versioned Textual Documents ###

Hedera is a framework to facilitate the rapid development of
processing methods on big versioned
document collections  using the Hadoop framework. It provides
customized InputFormat for accessing data in parallel in standard
MapReduce workflows. It also has new mechanism of extracting and
transforming while loading (versus the traditional Extraction,
Transformation, Loading or ETL) to reduce the IO burden when passing
large amounts of data through a cluster. Hedera can be
used in Hadoop Streaming to enable rapid development using different
languages (Java, Python, C, etc.) , and it also supports Pig with
a number of User-defined functions (UDFs). At the moment, the
framework has been tested in Hadoop CDH 4.x and Pig 0.11.x. It
supports both Hadoop YARN and non-YARN models. Free for research and
educational purpose under GNU and Common Creative License.

### Why Hedea ?##

While there are several frameworks built to support
processing big textual data (both in MapReduce and non-MapReduce
fashion), little has been focused on efficient processing of versioned
documents 
