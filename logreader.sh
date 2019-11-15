# /bin/bash

export CLASSPATH=/usr/hdp/current/hadoop-client/*:/usr/hdp/current/hadoop-client/client/*:/usr/hdp/current/hadoop-hdfs-client/lib/*:/usr/hdp/current/hadoop-yarn-client/*:/etc/hadoop/conf:./target/YarnLogFileReader-1.0-SNAPSHOT.jar

java YarnLogFileReader.YarnLogFileReader $1
