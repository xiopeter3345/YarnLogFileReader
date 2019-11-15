# /bin/bash

if [ -d "/usr/hdp/current"]; then
    export CLASSPATH=/usr/hdp/current/hadoop-client/*:/usr/hdp/current/hadoop-client/client/*:/usr/hdp/current/hadoop-hdfs-client/lib/*:/usr/hdp/current/hadoop-yarn-client/*:/etc/hadoop/conf:./target/YarnLogFileReader-1.0-SNAPSHOT.jar
else
    export CLASSPATH=./target/YarnLogFileReader-1.0-SNAPSHOT.jar
fi
java YarnLogFileReader.YarnLogFileReader $1