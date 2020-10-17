# Examples in Spark 3 

The project contains unit tests to work with Core Spark, Spark Sql and Spark Streaming modules.

## Description

The project works with unit tests to initialize spark context with different RDD objects and assert data.

Pay attention that maven surefire plugin excludes ***IntegrationTest.java** tests, because they need client notifications.
See description how to run integration tests below.   

### Core RDD objects

Unit tests in <a href="https://github.com/StepanMelnik/Spark3_Examples/tree/master/src/test/java/com/sme/spark/rdd">JavaRDD</a> folder cover transformation and action functionalities of Core spark.

**WordOffsetTest** test prepares the same data as described in LargeFileParser project and asserts the same result.

### Load-Save data

Unit tests in <a href="https://github.com/StepanMelnik/Spark3_Examples/tree/master/src/test/java/com/sme/spark/loadsave">Load-Save</a> folder cover functionalities to load and save data in different sources.


### Sql

Unit tests in <a href="https://github.com/StepanMelnik/Spark3_Examples/tree/master/src/test/java/com/sme/spark/sql">SQL</a> folder cover functionalities to work with Spark Sql module and Hive, Parquet data sources.

### Stream

Unit tests in <a href="https://github.com/StepanMelnik/Spark3_Examples/tree/master/src/test/java/com/sme/spark/structuredstream">Structured Stream</a> folder cover functionalities to work with structured stream:
  * Socket stream
  * Kafka Stream 

**Integration tests**

  * <a href="https://github.com/StepanMelnik/Spark3_Examples/blob/master/src/test/java/com/sme/spark/structuredstream/StructuredSocketStreamIntegrationTest.java">StructuredSocketStreamIntegrationTest</a> integration test covers socket stream and socket stream with window duration functionalities:
    * <a href="https://github.com/StepanMelnik/Spark3_Examples/blob/master/src/test/java/com/sme/spark/structuredstream/StructuredSocketStreamIntegrationTest.java#L77">testSocketStreamToConsole</a> describes a detailed test plan how to run Spark stream and send a client message by a socket;
    *  <a href="https://github.com/StepanMelnik/Spark3_Examples/blob/master/src/test/java/com/sme/spark/structuredstream/StructuredSocketStreamIntegrationTest.java#L186">testSocketStreamWithWindowDuration</a> describes a detailed test plan how to run Spark stream in window duration and send a client message by a socket;
  * <a href="https://github.com/StepanMelnik/Spark3_Examples/blob/master/src/test/java/com/sme/spark/structuredstream/StructuredKafkaStreamIntegrationTest.java">StructuredKafkaStreamIntegrationTest</a> integration test covers Kafka load stream functionality:
       * <a href="https://github.com/StepanMelnik/Spark3_Examples/blob/master/src/test/java/com/sme/spark/structuredstream/StructuredKafkaStreamIntegrationTest.java#L46">testConsumeKafkaWordCount</a> describes a detailed test plan how to run Kafka, Spark test and send a message by kafka to a topic.    
    


Unit tests in <a href="https://github.com/StepanMelnik/Spark3_Examples/tree/master/src/test/java/com/sme/spark/dsstream">DS Stream</a> folder cover functionalities to work with batch stream:
  * Socket stream
  * Kafka Stream 
  
**Integration tests**

  * <a href="https://github.com/StepanMelnik/Spark3_Examples/blob/master/src/test/java/com/sme/spark/dsstream/BatchSocketStreamIntegrationTest.java">BatchSocketStreamIntegrationTest</a> test covers JavaDStream to work with socket source:
     * <a href="https://github.com/StepanMelnik/Spark3_Examples/blob/master/src/test/java/com/sme/spark/dsstream/BatchSocketStreamIntegrationTest.java#L50">testDStreamSocketStream</a> test describes a detailed test plan how to run unit test and send a message by socket
  * <a href="https://github.com/StepanMelnik/Spark3_Examples/blob/master/src/test/java/com/sme/spark/dsstream/DirectBatchKafkaStreamIntegrationTest.java">DirectBatchKafkaStreamIntegrationTest</a> covers Kafka direct stream functionaity:
      * <a href="https://github.com/StepanMelnik/Spark3_Examples/blob/master/src/test/java/com/sme/spark/dsstream/DirectBatchKafkaStreamIntegrationTest.java#L57">testConsumeKafkaWordCount</a> describes a plan how to run Spark test and send a message to spark by Kafka producer.     


## IDE
Eclipse does not work well with Scala (let's say I cannot set breakpoint in Scala sources).
So better to use IntelliJ IDEA to work with the project.

## Kafka

Integration Unit tests work with Spark and Kafka.

You can start Kafka server directly by shell commands or by docker-compose.

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

Clone and install <a href="https://github.com/StepanMelnik/Parent.git">Parent</a> project before building.

Clone current project and run:

    mvn clean install
    mvn test

