#!/bin/sh
LIB=$(pwd)/libhedera

# path to your jar file here. Default is the maven artifact in your local repo
export CLPA="target/hedera-0.1-SNAPSHOT.jar"

for jarf in $LIB/*.jar
do
CLPA=$CLPA:$jarf
HCLPA=$HCLPA,$jarf
done
# CLPA=${CLPA:1:${#CLPA}-1}
# HCLPA=${HCLPA:1:${#HCLPA}-1}
# CLPD=$CLPA:$JAR_PATH
# HCLPDA=$HCLPA,$JAR_PATH
export HADOOP_CLASSPATH="$CLPD:$HADOOP_CLASSPATH"

java -Xmx2g -cp $CLPA $1 $2 $3 $4 $5 $6 $7 $8 $9
