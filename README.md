# Data playground


Code examples for big data tools, analytics and frameworks.

## Prerequisites

Before you begin you might need the following:

-  [Kafka 3.6.1](https://kafka.apache.org/downloads)
-  [Spark 3.5](https://spark.apache.org/downloads.html)
-  [Flink 1.17](https://flink.apache.org/downloads/)

## Local development

### Kafka

**Standalone Kafka cluster using Kraft**

```bash
# cd to the kafka
KAFKA_CLUSTER_ID="$(./bin/kafka-storage.sh random-uuid)"
./bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c ./config/kraft/server.properties
./bin/kafka-server-start.sh ./config/kraft/server.properties
```

**Topics**

```
./bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092
./bin/kafka-topics.sh --describe --topic quickstart-events --bootstrap-server localhost:9092
./bin/kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092
./bin/kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092
```

### Flink

**Update task slots to allow parallelism**

```conf
[label ./conf/flink-conf.yaml]
# The number of task slots that each TaskManager offers. Each slot runs one parallel pipeline.
taskmanager.numberOfTaskSlots: 1
```

**Start standalone cluster**

```bash
./bin/start-cluster.sh
```

Open the web UI on [http://localhost:8081](http://localhost:8081)

### Spark

> Spark slave port conflict with Flink's port 8081. You may need to update one or the other if you need to run the two clusters **in the same machine at the same time**.

```
./sbin/start-master.sh
./sbin/start-worker.sh <master-spark-url>
```

Open the web UI on [http://localhost:8080/](http://localhost:8080/)

## Implementation samples

### Kafka-Spark-Streaming

**Perform aggregation from a file(s)**

```
./bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --master spark://aljresuento1.arcanys.com:7077 {path to}/load_csv.py
```

**Perform windowed aggregations from a streaming topic**

```
# create kafka topic
./bin/kafka-topics.sh --create --topic source-events --bootstrap-server localhost:9092

# submit the job
./bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --master spark://aljresuento1.arcanys.com:7077 {path to}/streaming.py

# run the playback script
python playback.py
```

### Kafka-Flink-Streaming

**Stateful streaming from/to kafka source/sink**

```
# create kafka topics
./bin/kafka-topics.sh --create --topic my-source-topic --bootstrap-server localhost:9092
./bin/kafka-topics.sh --create --topic my-sink-topic --bootstrap-server localhost:9092

# submit streaming app
./bin/flink run \
  -py {path to}/big-data-git/kafka-flink-streaming/app/streaming_app.py \
  --pyFiles {path to}/big-data-git/kafka-flink-streaming/app/deps

# start the client
python client.py

# start the playback
python playback.py
```

