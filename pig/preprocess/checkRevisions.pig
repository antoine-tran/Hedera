/*
 * Copyright (c) 2014 Anh Tuan Tran - ttran@L3S.de
 * Pig scripts to examine Wikipedia revision history. Project details: http://antoine-tran.github.io/hedera
 *
 *=================================================
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/


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
REGISTER '$LIBDIR/elephant-bird-core-4.6.jar';
REGISTER '$LIBDIR/elephant-bird-pig-4.6.jar';
REGISTER '$LIBDIR/elephant-bird-hadoop-compat-4.6.jar';
REGISTER '$LIBDIR/json-simple-1.1.1.jar';

SET default_parallel 1
SET mapred.output.compress 'true';
SET mapred.output.compression.codec 'org.apache.hadoop.io.compress.BZip2Codec';

wiki = LOAD '$BASEDIR/$INPUT' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (m:map[{(page_id:long,page_title:chararray,page_namespace:int,rev_id:long,parent_id:long,timestamp:long,user:chararray,user_id:long,comment:chararray,text:chararray)}]);

-- Sort the revisions by timestamp
-- DECLARE INPUT to be the directory of the xml dump files
-- DESCRIBE wiki;
-- ewiki = FILTER wiki BY $0#'timestamp' > $BEGIN AND $0#'timestamp' < $END;
-- swiki = ORDER ewiki BY $0#'timestamp' PARALLEL 200;
-- STORE wiki INTO '$OUTPUT$YEAR.json USING JsonStorage();

-- Get the maximum timestamp from old revisions dataset
gwiki = GROUP wiki ALL;
wiki_max = FOREACH gwiki GENERATE MAX(wiki.m#'timestamp');

DUMP wiki_max;