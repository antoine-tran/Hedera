/*
 * Copyright (c) 2015 Anh Tuan Tran - ttran@L3S.de
 * Pig scripts to extract various information from Wikipedia revision history. Project details: http://antoine-tran.github.io/hedera
 *
 *=================================================
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/

%DECLARE LIBDIR '/share/lib'
REGISTER '$LIBDIR/elephant-bird-core-4.6.jar';
REGISTER '$LIBDIR/elephant-bird-pig-4.6.jar';
REGISTER '$LIBDIR/elephant-bird-hadoop-compat-4.6.jar';
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
REGISTER '$LIBDIR/json-simple-1.1.1.jar'

SET default_parallel 10;
SET job.name 'Merge revisions to make Philipp happy';

wiki = LOAD '$BASEDIR/$INPUT' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad'); -- AS (page_id:long,page_title:chararray,page_namespace:int,rev_id:long,parent_id:long,timestamp:long,user:chararray,user_id:long,comment:chararray,text:chararray);

DESCRIBE wiki;

STORE wiki INTO '$OUTPUT' USING JsonStorage();

