from lib.config.database import close_cursor, close_engine, close_connection, get_sqlalchemy_engine_conn_cursor
from lib.helpers.logging import get_logger
import pandas as pd
from datetime import datetime
from confluent_kafka import Producer, Consumer
from json import loads as json_loads

logger = get_logger('BaseRepository')


class Database():

    def __init__(self, cursor, engine, conn, connection_string: str, reconnection_attempts: int=5):
        self.reconnection_attempts = reconnection_attempts
        self.connection_string = connection_string
        self.cursor = cursor
        self.engine = engine
        self.conn = conn

    @property
    def cursor(self):
        return self._cursor

    @property
    def engine(self):
        return self._engine

    @property
    def conn(self):
        return self._conn

    @cursor.setter
    def cursor(self, value):
         self._cursor = value

    @engine.setter
    def engine(self, value):
         self._engine = value

    @conn.setter
    def conn(self, value):
         self._conn = value

    @cursor.deleter
    def cursor(self):
        del self._cursor

    @engine.deleter
    def engine(self):
        del self._engine

    @conn.deleter
    def conn(self):
        del self._conn

    def close_cursor(self):
        close_cursor(self.cursor)

    def close_engines(self):
        close_engine(self.engine)

    def close_connection(self):
        close_connection(self.engine)

    def retrieve_data(self, query, params=None, dtype=None, query_name='Query-Name'):
        for attempt in range(self.reconnection_attempts):
            try:
                df = pd.read_sql_query(query, self.conn, params=params, dtype=dtype)
                if df.empty: logger.error(f"WARNING: No data | Query: {query_name} | Params: {params}")
                return df
            except BaseException as err:
                logger.error(f"ERROR: Failed attempt {attempt} to retrieve data | Query: {query_name} | Params: {params}")
                logger.error(str(err))
                self.close_engines()
                self.engine, self.conn, self.cursor = get_sqlalchemy_engine_conn_cursor(self.connection_string)
        return pd.DataFrame()

    def update_data(self, query, params=None, dtype=None, query_name='Query-Name'):
        for attempt in range(self.reconnection_attempts):
            try:
                self.cursor.execute(query, params)
                self.conn.commit()

            except BaseException as err:
                logger.error(f"ERROR: Failed update data | Query: {query_name} | Params: {params}")
                logger.error(str(err))
                self.close_engines()
                self.engine, self.conn, self.cursor = get_sqlalchemy_engine_conn_cursor(self.connection_string)
        return pd.DataFrame()

class MessageQueue():

    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers

    def producer_delivery_report(err, msg):
        """
        Reports the success or failure of a message delivery.
        Args:
            err (KafkaError): The error that occurred on None on success.
            msg (Message): The message that was produced or failed.
        """

        if err is not None:
            print("Delivery failed for record {}: {}".format(msg.key(), err))
            return
        print('Record {} successfully produced to {} [{}] at offset {}'.format(msg.key(), msg.topic(), msg.partition(), msg.offset()))

    def producer(self, topic, message, key, **kwargs):
        try:
            conf = {
            "bootstrap.servers": self.bootstrap_servers
            }

            # Create Producer instance
            producer = Producer(**conf)

            if len(str(message))>0:
               # Send it to our 'messages' topic
                print(f'INFO: Producing message @ {datetime.now()} | Message = {str(message)}')
                producer.produce(    topic=topic
                                    ,key=key
                                    ,value=message
                                    ,on_delivery=self.producer_delivery_report
                                )
            else:
                logger.error(f"WARNING: Empty message! @ {datetime.now()} | Message = {str(message)}")
 
        except BaseException as err:
            logger.error("ERROR: Failed attempt to send message!")
            logger.error(str(err))

    def consumer(self, group_id, **kwargs):
        """
        kwargs: {"auto.offset.reset": "earliest"}
        """
        try:
            conf = {
            "bootstrap.servers": self.bootstrap_servers
            ,"group.id": group_id
            ,"auto.offset.reset": kwargs.get("auto.offset.reset") if "auto.offset.reset" in kwargs else "earliest"
            }

            # Create Consumer instance
            consumer = Consumer(**conf)
            data = consumer.consume()

            if len(data)>0:
                for message in data:
                    if len(str(message.value))>0:
                        message_value = json_loads(message.value)
                        #message_value = message.value
                        print(
                                "\n" +
                                f"Header: {message.headers} | " +
                                f"Key: {message.key} | " +
                                f"Topic: {message.topic} | " +
                                f"Len (bytes): {message.len} | " +
                                f"Value: {message_value}"
                            )
                        # HERE: do something with the message_value

                    else:
                        logger.error(f"WARNING: Empty message! @ {datetime.now()} | Message = {str(message.value)}")

        except BaseException as err:
            logger.error(f"ERROR: Failed attempt to consume message!")
            logger.error(str(err))         
