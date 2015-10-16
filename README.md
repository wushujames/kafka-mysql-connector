This is a simple copycat connector. This repo is intended to be a simple skeleton, that you can copy/paste to get started with developing your own copycat connector.

This repo contains a fork of the copycat-file plugin from Kafka trunk. The code itself has not changed. However, I renamed the package, so that there is no chance that you will accidentally get the copycat-file plugin from trunk.

This things specific to this repo are the build files, as well as the instructions in this README that tell you how to get started.

As of today (2015-10-15), Copycat has not been released in any official version of Kafka, so in order to run it and develop against it, we will need to work off of Kafka trunk.

Instructions for building
-------------------------
1. Pull down Kafka trunk.
    $ git clone https://github.com/apache/kafka kafka
2. Build kafka and place jars in local maven repo 
    $ (cd kafka && ./gradlew install)
3. Pull this repo down and build it
    $ git clone https://[path] copycat-file
    $ (cd copycat-file && ./gradlew build)

Instructions for running
------------------------
1. Run zookeeper somehow.
    Here are instructions on how to run it using docker.
    $ docker run -d --name zookeeper -p 2181:2181 confluent/zookeeper
    
2. Run the trunk version of Kafka.
    $ (cd kafka && ./bin/kafka-server-start.sh config/server.properties)
    
3. Run your copycat plugin
    $ export CLASSPATH=/path/to/copycat-file/build/libs/copycat-file.jar
    $ kafka/bin/copycat-standalone.sh copycat-file/copycat-standalone.properties  copycat-file/copycat-file-source.properties
    
4. Write stuff to test.txt (that is the that this connector will read from, as configured in copycat-file-source.properties)
    $ echo `date` >> test.txt
    
5. Read the data out from the kafka topic named 'test' (that is the that this connector will write to, as configured in copycat-file-source.properties)
    $ kafka/bin/kafka-console-consumer.sh  --zookeeper 10.100.222.31:2181 --topic test
    {"schema":{"type":"string","optional":false},"payload":"Thu Oct 15 23:03:15 PDT 2015"}
