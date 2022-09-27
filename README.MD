# Architecture

Dockers:

- Apache-Zookeeper_01 + JDK 8: port 2180
- Apache-Zookeeper_02 + JDK 8: port 2180
- Apache-Zookeeper_03 + JDK 8: port 2180

- Apache-Kafka + JDK 8 with 3 brokers:
    - broker 00: port 9092
    - broker 01: port 9093
    - broker 02: port 9094

All Kafka agents working on a topic (name = topic_01), with replication factor = 3 and partitioning = 1.

- PostgreSQL + Python: 3.10: port 5432

The Producer and Consumers are running on the database server (PostgreSQL), as a client. All machines are on the same network.

All server are running in Linux Ubuntu Operational System.

# Install driver dependency on server where application is running (Python)
sudo apt-get install libcppdb-postgresql0 odbc-postgresql