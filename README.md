Kafka MySQL Connector

**I am stopping development on this connector.**

**For a Kafka Connect based solution, check out the excellent [Debezium](http://debezium.io) project.**

**For a standalone MySQL->Kafka daemon solution, check out the excellent [Maxwell](http://maxwells-daemon.io) project, upon which this connector was based.**

---

kafka-mysql-connector is a plugin that allows you to easily replicate MySQL changes to Apache Kafka. It uses the fantastic [Maxwell](https://github.com/zendesk/maxwell) project to read MySQL binary logs in near-real time. It runs as a plugin within the [Kafka Connect](http://kafka.apache.org/090/documentation.html#connect) framework, which provides a standard way to ingest data into Kafka.

By using Maxwell, we are able to:
- replicate data from an unpatched MySQL server
- Parse ALTER/CREATE/DROP table statements, which allows us to always have a correct view of the MySQL schema

By plugging in to the Kafka Connect framework, we are able to:
- benefit from standardized best practices for Kafka producers and consumers
- run in distributed or standalone mode
- manage the Kafka MySQL Connector via REST interfaces
- manage offsets in Kafka

Status
------
This code is a work-in-progress.

What's done:
* Offsets stored in Kafka by the Kafka Connect framework
* Data format is a string... which contains JSON. Which is stored within a JSON structure by Kafka Connect. Which means tons of escaped quotes.
* Each table is written to its own topic. The Kafka primary key is the row's primary key.
* It supports primary keys which are ints.

What needs to be done:
* Support primary keys of any SQL type
* Add schema support for the rows, so that it isn't a JSON string
* Testing.
* Packaging
* Logging

Instructions for building and running
-------------------------------------
For now, it uses a [forked](https://github.com/wushujames/maxwell/tree/wushujames/libraryize) version of Maxwell. I have spoken to Ben Osheroff, the author of Maxwell, and I will be contributing my changes back to the Maxwell project.

1.  Pull down my Maxwell fork.
    ```
    $ git clone https://github.com/wushujames/maxwell.git maxwell
    ```

2.  Build my Maxwell fork (which is in a branch called wushujames/libraryize) and place jars in local maven repo
    ```
    $ (cd maxwell && git checkout wushujames/libraryize && mvn install)
    ```

3.  Pull this repo down and build it and "install" it within the build directory.
    ```
    $ git clone https://github.com/wushujames/kafka-mysql-connector kafka-mysql-connector
    $ (cd kafka-mysql-connector && ./gradlew build installDist)
    ```

4.  Run zookeeper somehow.
    ```
    Here are instructions on how to run it using docker.
    $ docker run -d --name zookeeper -p 2181:2181 confluent/zookeeper
    ```
    
5.  Download and run kafka 0.9.0 locally by following the instructions at http://kafka.apache.org/documentation.html#quickstart
    ```
    $ curl -o kafka_2.11-0.9.0.0.tgz http://www.us.apache.org/dist/kafka/0.9.0.0/kafka_2.11-0.9.0.0.tgz
    $ tar xvfz kafka_2.11-0.9.0.0.tgz
    $ cd kafka_2.11-0.9.0.0
    $ ./bin/kafka-server-start.sh config/server.properties
    ```

6.  Start up and configure mysql properly. (This is horribly ugly. I will clean it up at some point, but wanted to get something usable out).
    ```
    docker run --name mariadb -e MYSQL_ROOT_PASSWORD=passwd -p 3306:3306 -d mariadb:5.5
    docker exec -it mariadb bash

    cat << EOF > /etc/mysql/conf.d/skip-name-resolve.cnf
    [mysqld]
    skip-host-cache
    skip-name-resolve
    EOF

    cat << EOF > /etc/mysql/conf.d/binlog.cnf
    [mysqld]
    server-id=1
    log-bin=master
    binlog_format=row
    EOF

    exit

    docker stop mariadb
    docker start mariadb

    mysql -v --protocol=tcp --host=192.168.59.103 --user=root --password=passwd

    GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE on *.* to 'maxwell'@'%' identified by 'XXXXXX';
    GRANT ALL on maxwell.* to 'maxwell'@'%';
    ```

7.  Configure connector, pointing to your mysql instance. (See connect-mysql-source.properties)

8.  Run Copycat with this connector, with the connector's files in the CLASSPATH.
    ```
    $ export CLASSPATH=`pwd`/kafka-mysql-connector/build/install/kafka-mysql-connector/connect-mysql-source.jar:`pwd`/kafka-mysql-connector/build/install/kafka-mysql-connector/lib/*
    $ kafka_2.11-0.9.0.0/bin/connect-standalone.sh kafka-mysql-connector/copycat-standalone.properties  kafka-mysql-connector/connect-mysql-source.properties
    ```

9.  Insert into mysql
    ```
    $ mysql -v --protocol=tcp --host=192.168.59.103 --user=root --password=passwd
    MariaDB [(none)]> create database test;
    MariaDB [(none)]> create table test.users (userId int auto_increment primary key, name char(128));
    MariaDB [(none)]> insert into test.users (name) values ("James");
    ```


10. Read the data out from the kafka topic named 'test.users'. The name of the topic correponds to the name of the database.table you inserted into.
    ```
    $ kafka_2.11-0.9.0.0/bin/kafka-console-consumer.sh  --zookeeper localhost:2181 --topic test.users --from-beginning
    {"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"userid"},{"type":"string","optional":false,"field":"name"}],"optional":false,"name":"test.users"},"payload":{"userid":1,"name":"James"}}
    ```
