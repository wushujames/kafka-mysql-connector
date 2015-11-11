Kafka MySQL Connector

kafka-mysql-connector is a plugin that allows you to easily replicate MySQL changes to Apache Kafka. It uses the fantastic [Maxwell](https://github.com/zendesk/maxwell) project to read MySQL binary logs in near-real time. It runs as a plugin within the [Kafka Connect](http://kafka.apache.org/090/documentation.html#connect) framework, which provides a standard way to ingest data into Kafka.

By using Maxwell, we are able to:
- replicate date from an unpatched MySQL server
- Parses ALTER/CREATE/DROP table statements, which allows us to always have a correct view of the MySQL schema

By plugging in to the Kafka Connect framework, we are able to:
- benefit from standardized best practices for Kafka producers and consumers
- run in distributed or standalone mode
- manage the MySQL Kafka Connector via REST interfaces
- manage offsets in Kafka

Status
------
This code is a work-in-progress.

Instructions for building and running
-------------------------------------
For now, it uses a [forked](https://github.com/wushujames/maxwell/tree/wushujames/libraryize) version of Maxwell. I have spoken to Ben Osheroff, the author of Maxwell, and I will be contributing my changes back to the Maxwell project.

And it currently is built off of an older commit of Kafka trunk, so you will need to download and build kafka as well. I will update this to be based off of Kafka trunk soon. When Apache Kafka 0.9.0 is released, then I will use the published jars and will not need to build the tree ourselves.

1.  Pull down Kafka trunk
    ```
    $ git clone https://github.com/apache/kafka kafka
    $ (cd kafka && git checkout 6f2f1f9)
    ```

2.  Build kafka and place jars in local maven repo
    ```
    $ (cd kafka && gradle wrapper && ./gradlew install)
    ```

3.  Pull down my Maxwell fork.
    ```
    $ git clone https://github.com/wushujames/maxwell.git maxwell
    ```

4.  Build my Maxwell fork (which is in a branch called wushujames/libraryize) and place jars in local maven repo
    ```
    $ (cd maxwell && git checkout wushujames/libraryize && mvn install)
    ```

5.  Pull this repo down and build it and "install" it within the build directory.
    ```
    $ git clone https://github.com/wushujames/mysql-kafka-connector mysql-kafka-connector
    $ (cd mysql-kafka-connector && ./gradlew build installDist)
    ```

6.  Run zookeeper somehow.
    ```
    Here are instructions on how to run it using docker.
    $ docker run -d --name zookeeper -p 2181:2181 confluent/zookeeper
    ```
    
7.  Run the trunk version of Kafka.
    ```
    $ (cd kafka && ./bin/kafka-server-start.sh config/server.properties)
    ```

8.  XXX Start up and configure mysql properly.


9.  XXX Configure connector, pointing to your mysql instance


10. Run Copycat with this connector, with the connector's files in the CLASSPATH.
    ```
    $ export CLASSPATH=`pwd`/mysql-kafka-connector/build/install/mysql-kafka-connector/copycat-file.jar:`pwd`/mysql-kafka-connector/build/install/mysql-kafka-connector/lib/*
    $ kafka/bin/copycat-standalone.sh mysql-kafka-connector/copycat-standalone.properties  mysql-kafka-connector/copycat-standalone/copycat-file-source.properties
    ```

11. XXX Insert into mysql

12. XXX Read out of kafka
