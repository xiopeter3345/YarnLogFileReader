# /bin/bash

if [ ! -f "./target/YarnLogFileReader-1.0-SNAPSHOT-dependencies.jar" ]; then
    mvn package assembly:single
fi

export CLASSPATH=/etc/hadoop/conf:./target/YarnLogFileReader-1.0-SNAPSHOT-dependencies.jar:/usr/hdp/current/hadoop-hdfs-client/lib/adls2-oauth2-token-provider.jar:/usr/hdp/current/failover-controller/lib/*

java YarnLogFileReader.YarnLogFileReader $1