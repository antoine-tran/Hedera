#!/bin/sh

# A work flow of normalizing, sorting, aggregating, matching wikistats

# Input 1 is the wikistat file (pagecount-ez)
# Input 2 is the month of wiki stat in "YYYY-mm" format
# input 3 is the id-title map file [ format: title TAB id ]

# Example sh match_wikistats.sh pagecounts-2013-12-ge.bz2 2013-12 title2id.20140502

# Normalize the EZ page view
# sh $(pwd)/etc/run-local-jars.sh org.hedera.LocalEZPageviewDay $1 $1.out $2
sh $(pwd)/etc/run-local-jars.sh org.hedera.LocalEZPageviewHour $1 $1.out $2

# Sort by title
LANG=en_EN sort -k1,1 $1.out > $1.sort

# Aggregate the view counts
# sh $(pwd)/etc/run-local-jars.sh org.hedera.AggregateEZPageview $1.sort $1.aggr $2

# Match against the title-id mapping
# LANG=en_EN join $3 $1.aggr | awk '{for(i=2;i<=NF;i++)printf "%s", $i (i==NF?ORS:OFS)}' > $1.ts

# Remove the temporary file
# rm -rf $1.out
# rm -rf $1.sort
# rm -rf $1.aggr
