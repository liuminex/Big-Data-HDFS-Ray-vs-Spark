#!/bin/bash

# Run this script in the master VM


### Configure Spark


echo 'spark.eventLog.enabled          true
spark.eventLog.dir              hdfs://o-master:54310/spark.eventLog
spark.history.fs.logDirectory   hdfs://o-master:54310/spark.eventLog
spark.master                    yarn
spark.submit.deployMode         client
spark.driver.memory             1g
spark.executor.memory           1g
spark.executor.cores            1' > $SPARK_HOME/conf/spark-defaults.conf && hadoop fs -mkdir /spark.eventLog && $SPARK_HOME/sbin/start-history-server.sh && echo "Access Spark History Server at http://$MASTER_PUBLIC_IP:18080"




```bash
nano $SPARK_HOME/conf/spark-defaults.conf
```
add the following:
```bash
spark.eventLog.enabled          true
spark.eventLog.dir              hdfs://o-master:54310/spark.eventLog
spark.history.fs.logDirectory   hdfs://o-master:54310/spark.eventLog
spark.master                    yarn
spark.submit.deployMode         client
spark.driver.memory             1g
spark.executor.memory           1g
spark.executor.cores            1
```
```bash
hadoop fs -mkdir /spark.eventLog
$SPARK_HOME/sbin/start-history-server.sh
```

Επιβεβαιώνουμε ότι η διαδικασία έχει επιτύχει προσπελαύνοντας με ένα browser το web interface στην public ip του master: `http://[83.212.xxx.xxx]::18080`

Εκτέλεση παραδείγματος εφαρμογής Spark για επιβεβαίωση ορθότητας:
```bash
spark-submit --class org.apache.spark.examples.SparkPi ~/opt/spark/examples/jars/spark-examples_2.12-3.5.0.jar 100
```

## Useful links
- [http://???:8088](http://83.212.xxx.xxx:8088)
- [http://???::18080](http://83.212.xxx.xxx::18080)



