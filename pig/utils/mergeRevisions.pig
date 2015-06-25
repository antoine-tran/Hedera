/*
 * Copyright (c) 2015 Anh Tuan Tran - ttran@L3S.de
 * Pig scripts to extract various information from Wikipedia revision history. Project details: http://antoine-tran.github.io/hedera
 *
 *=================================================
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/

%DECLARE LIBDIR '/share/lib'
REGISTER '$LIBDIR/elephant-bird-core-4.1.jar';
REGISTER '$LIBDIR/elephant-bird-pig-4.1.jar';
REGISTER '$LIBDIR/elephant-bird-hadoop-compat-4.1.jar';
REGISTER '$LIBDIR/hedera-0.1-SNAPSHOT.jar'

set default_parallel 10;
set job.name 'Merge revisions to make Philipp happy';

wiki = LOAD '$BASEDIR/$INPUT' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad');

DESC wiki;



STORE wiki INTO '$OUTPUT' USING JsonStorage();

