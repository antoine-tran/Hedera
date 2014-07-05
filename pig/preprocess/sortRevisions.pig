%DECLARE LIBDIR 'Your libraries at HDFS'
REGISTER '$LIBDIR/guava-15.0.jar'
REGISTER '$LIBDIR/javax.inject-1.jar'
REGISTER '$LIBDIR/bliki-core-3.0.19.jar'
REGISTER '$LIBDIR/cloud9-1.5.0.jar'
REGISTER '$LIBDIR/diffutils-1.2.1.jar'
REGISTER '$LIBDIR/stanford-corenlp-1.3.4.jar'
REGISTER '$LIBDIR/hedera-0.1-SNAPSHOT.jar'
REGISTER '$LIBDIR/jsoup-1.7.3.jar'
REGISTER '$LIBDIR/joda-time-2.3.jar'
REGISTER '$LIBDIR/commons-cli-1.2.jar'
REGISTER '$LIBDIR/ahocorasick-0.2.2.jar'
REGISTER '$LIBDIR/dsiutils-2.0.15.jar'
REGISTER '$LIBDIR/fastutil-6.5.15.jar'
REGISTER '$LIBDIR/piggybank-0.12.1.jar'
SET default_parallel 999
SET mapred.output.compress 'true';
SET mapred.output.compression.codec 'org.apache.hadoop.io.compress.BZip2Codec';

-- Sort the revisions by timestamp
-- DECLARE INPUT to be the directory of the xml dump files

-- wiki = LOAD '$BASEDIR/$INPUT' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (page_id:long,page_title:chararray,page_namespace:int,rev_id:long,parent_id:long,timestamp:long,user:chararray,user_id:long,comment:chararray,text:chararray);

wiki = LOAD '$BASEDIR/$INPUT' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad');

DESCRIBE wiki;

ewiki = FILTER wiki BY $0#'timestamp' > $BEGIN AND $0#'timestamp' < $END;

swiki = ORDER ewiki BY $0#'timestamp' PARALLEL 200;

STORE wiki
    INTO '$OUTPUT$YEAR.json
    USING JsonStorage();