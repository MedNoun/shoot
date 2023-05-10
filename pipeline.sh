#!/bin/bash
echo "Big Data Shoot Pipeline"
echo "---------------------------"
sleep 3

echo ""
echo "0-Initializing"
sleep 2

echo ""
echo "**************** Starting Hadoop ****************"
./start-hadoop.sh & sleep 10

echo ""
echo "**************** Starting Zookeeper ****************"
./start-kafka-zookeeper.sh & sleep 10

echo ""
echo "**************** Creating Kafka Topics ****************"
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic faul &
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic goal &
kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic card
sleep 5

echo ""
echo "**************** Starting Brokers ****************"
kafka-server-start.sh $KAFKA_HOME/config/server.properties & kafka-server-start.sh $KAFKA_HOME/config/server-one.properties & kafka-server-start.sh $KAFKA_HOME/config/server-two.properties & sleep 10

echo ""
echo "**************** Starting Hbase ****************"
start-hbase.sh & sleep 5

echo ""
echo "**************** Creating tables ****************"
java CreateTable goal
java CreateTable faul
java CreateTable card

echo ""
echo "1-Starting Consumers"
sleep 3
spark-submit --class com.shoot.kafka.GoalConsumer --master local[2] stream-kafka-spark-1-jar-with-dependencies.jar localhost:2181 test goal 1 > goal_out &
spark-submit --class com.shoot.kafka.GoalConsumer --master local[2] stream-kafka-spark-1-jar-with-dependencies.jar localhost:2181 test faul 1 > faul_out &
spark-submit --class com.shoot.kafka.GoalConsumer --master local[2] stream-kafka-spark-1-jar-with-dependencies.jar localhost:2181 test card 1 > card_out

