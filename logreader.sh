# /bin/bash

export CLASSPATH=/etc/hadoop/conf:./target/YarnLogFileReader-1.0-SNAPSHOT-dependencies.jar

java YarnLogFileReader.YarnLogFileReader $1