%DECLARE LIBDIR '/share/lib'
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

%DECLARE BASEDIR 'hdfs://master.ib:8020/user/tuan.tran'

SET default_parallel 24
SET mapred.output.compress 'true';
SET mapred.output.compression.codec 'org.apache.hadoop.io.compress.BZip2Codec';

idmap = LOAD '$BASEDIR/$inputmap' AS (pageid:long,title:chararray);

anchor = LOAD '$BASEDIR/$inputanchor' AS (ts:long,pid:long,revid:long,parent:long,anchor:chararray,target:chararray);

-- reduce the size of anchors
-- sanchor = FOREACH anchor GENERATE (ts,pid,target);

matchanchor = JOIN idmap BY title, anchor BY target;
pmatchanchor = FOREACH matchanchor GENERATE ts,pid,pageid;

STORE pmatchanchor INTO '$BASEDIR/$output' USING PigStorage('\t');