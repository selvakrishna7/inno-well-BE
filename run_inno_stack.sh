#!/bin/bash

# Start ZooKeeper
echo "Starting ZooKeeper..."
gnome-terminal -- bash -c "cd ~/kafka && bin/zookeeper-server-start.sh config/zookeeper.properties; exec bash"

sleep 5

# Start Kafka broker
echo "Starting Kafka broker..."
gnome-terminal -- bash -c "cd ~/kafka && bin/kafka-server-start.sh config/server.properties; exec bash"

sleep 10

# Start Spark job
echo "Starting Spark streaming job..."
gnome-terminal -- bash -c "/home/selva/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.0 /home/selva/inno-well/innowell/spark_stream_kafka.py; exec bash"

