#!/bin/sh

# A work flow of normalizing, sorting, aggregating, matching wikistats

# Input 1 is the wikistat file (pagecount-ez)
# Input 2 is the month of wiki stat in "YYYY-mm" format
# input 3 is the id-title map file [ format: title TAB id ]

# Example sh match_wikistats.sh pagecounts-2013-12-ge.bz2 2013-12 31 title2id.20140502 EN en_EN
# (parse the pagecount of Dec 2013, which has 31 days, and convert into the time series 
# key-ed by page id, using the dictionary of title-pageid stored in the file title2id.20140502. 
# The dictionary is for Wikipedia English (EN) version

# Normalize the EZ page view
sh $(pwd)/etc/run-local-jars.sh org.hedera.LocalEZPageviewDay $1 $2 $3 $1.out $5
# sh $(pwd)/etc/run-local-jars.sh org.hedera.LocalEZPageviewHour $1 $2 $3 $1.out $5

## DEPRECATED: This version of mixed Java-Shell code does not work for other languages than English.

# Sort by title
### LANG=$6 sort -k1,1 $1.out > $1.sort

# Aggregate the view counts
### sh $(pwd)/etc/run-local-jars.sh org.hedera.AggregateEZPageview $1.sort $1.aggr $3

# Match against the title-id mapping
### LANG=$6 join $4 $1.aggr | awk '{for(i=2;i<=NF;i++)printf "%s", $i (i==NF?ORS:OFS)}' > $1.ts

# Another round of aggregating to resolve the redirects
### LANG=$6 sort -k1,1 $1.ts > $1.sort

# Aggregate the view counts
### sh $(pwd)/etc/run-local-jars.sh org.hedera.AggregateEZPageview $1.sort $1.ts $3

###################################################################################
echo "aggregating..."
python $(pwd)/python/AggregateEZPageview.py $1.out $1.aggr $3
echo "joining..."
python $(pwd)/python/JoinEZPageview.py $1.aggr $4 $1.ts1
echo "aggregating..."
python $(pwd)/python/AggregateEZPageview.py $1.ts1 $1.ts $3

# Remove the temporary file
### rm -rf $1.out
### rm -rf $1.sort
### rm -rf $1.aggr
### rm -rf $1.ts1


