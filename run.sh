#!/bin/bash

if [[ $# -ne 4 ]] ; then
    echo 'arg1: compile  -c'
    echo '      run      -r'
    echo 'arg2: idealized 0'
    echo '      taxation  1'
    echo '      wiki bomb 2'
    echo 'arg3: links hdfs pwd'
    echo 'arg4: names hdfs pwd'
    exit 0
fi

if [[ $1 == '-c' ]] ; then
    ${HOME}/misc/sbt/bin/sbt compile \
    && ${HOME}/misc/sbt/bin/sbt package
fi

/usr/local/hadoop/bin/hadoop fs -rm -R /tmp/out_$2 ||: \
&& $SPARK_HOME/bin/spark-submit --class PageRank --master spark://salem.cs.colostate.edu:30136 --deploy-mode cluster ./target/scala-2.11/pagerank_2.11-1.0.jar $2 $3 $4 \
&& /usr/local/hadoop/bin/hadoop fs -head /tmp/out_$2/part-00000 ||:
