# Architecture

How to startup [Apache-Zookeeper](https://github.com/daniellj/containers/tree/main/CoordinationDistributedEnvironments/ApacheZookeeper) in docker.
- Server_01 + JDK 8: port 2180
- Server_02 + JDK 8: port 2180
- Server_03 + JDK 8: port 2180

How to startup [Apache-Kafka](https://github.com/daniellj/containers/tree/main/messaging-layer/ApacheKafka) in docker.
- Apache-Kafka with 3 brokers:
    - broker 00: port 9092
    - broker 01: port 9093
    - broker 02: port 9094

All Kafka agents working on a topic (name = topic_01), with replication factor = 3 and partitioning = 1.

How to startup [PostgreSQL + pgAdmin + Python 3.10](https://github.com/daniellj/containers/tree/main/rdbm/PostgreSQL) in docker.
- PostgreSQL: port 5432
- pgAdmin: port 80

The Producer and Consumers are running on the database server (PostgreSQL), as a client. All machines are on the same network.

All servers are running on Linux Ubuntu Operational System.

# Install driver dependency on server where application is running (Python)
sudo apt-get install libcppdb-postgresql0 odbc-postgresql