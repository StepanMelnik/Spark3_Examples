# Examples in Spark 3 

The project contains unit tests to work with Core Spark, Spark Sql and Spark Streaming.

## Description

### Sql

### Stream


## IDE
Eclipse does not work well with Scala (let's say i cannot set breakpoint in Scala sources).
So better to use IntelliJ IDEA to work with the project.

## Kafka

Integration Unit tests work with Spark and Kafka.

You can start Kafka server by directly shell commands or by docker-compose.

### Start Kafka by shell commands

Do the following steps to start kafka by hand:
  * sudo bin/zookeeper-server-start.sh config/zookeeper.properties
  * sudo bin/kafka-server-start.sh config/server.properties

### Docker compose 

Do the following steps to start zookeeper and kafka servers by docker: 
  * Check kafka images: sudo docker images -a | grep kafka
  * Stop docker if needed: sudo docker stop --time=30 kafka  
  * Check all running containers: sudo docker container ls | grep kafka
  * Check current services in docker-compose.yml: sudo docker-compose ps --services
  * Check if services are running: sudo docker-compose ps --services --filter "status=running"
  * Check if services up: sudo docker-compose ps | awk '$4 == "Up" {print $1}' | grep kafka
  * Start compose: sudo docker-compose -f docker-compose.yml up -d
  * Add more brokers sudo docker-compose scale kafka=3
  * Stop a cluster: sudo docker-compose stop
  * Make kafka.sh executable: sudo chmod +x kafka-shell.sh
  * Connect to kafka by sh: sudo sh ./kafka-shell.sh 192.168.0.109 192.168.0.109:2181

### Create Topic

You can create a topic by shell:
  * cd bin/
  * sudo ./kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic Topic
  * sudo ./kafka-topics.sh --list --bootstrap-server localhost:9092
  
Or you can use java api. See example in <a href="https://github.com/StepanMelnik/Kafka_Examples/blob/master/kafka-plain-java/src/test/java/com/sme/kafka/plain/admin/AdminTopicTest.java">AdminTopicTest</a> code.

## Docker
**docker-compose.yml** compose describes how to start Zookeeper cluster and Kafka server.  

## Jenkins
**Jenkinsfile** config used in Jenkins CI.   


## Build
    mvn clean install
    mvn test

