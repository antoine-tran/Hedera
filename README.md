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
document collections such as Web archives, revisions of collaborated
documents (news articles or encyclopedia pages such as Wikipedia). As
compared to traditional corpora, versioned documents have some following
special characteristics: 

- One documents have several snapshots, each typically associated with
one specific timestamps indicating the publication time. Snapshots
should not be considered independent documents, but rather siblings
connected to one document identity.

- Document snapshots' contents are highly redundant. In practice, most
  of consecutive snapshots are generated to fix meticulous
  details, typos, etc. from the previous ones.

- Big changes in content of a document often come in narrow time
  period (minutes or within a day) as a response to the addition of one truly new
  information or the revising of important text snippets. This bursty
  behaviour should be exploited for efficient processing of text

- In traditional text corpora, document distribution is skew, some
  have big chunks of text while other can contain a few words. In
  versioned documents, the degree of skewness goes even higher. For
  example, in Wikipedia revisions, some snapshots of a page can amount
  up to 10 GB of texts, and some (such as redirects) just have a few
  Kilobytes. Any parallel framework must take this into account to
  support better load balancing without sacrifying the
  inter-dependences of snapshots within one document.


Hedera was built with those questions in mind. It uses Hadoop
frameworks to support the scalable and parallel processing of data in
high level programming languages. It optimizes
the operations and APIs to address the above challenges, while still
conforms to the MapReduce standards and support traditional
workflows. Hedera aims to support rapid development of experimental
models, and to this extent it tries to rely less on heavy
general-purpose frameworks built for enterprise environments (such as
ElasticSearch, etc.). The philosophy of Hedera is 

>* Get dirty things done, and get them done quick, bro !! *


- *Two-level Load Balancing*: As a typical Hadoop setting

  
  
