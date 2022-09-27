import time 
from lib.helpers.logging import get_logger
from lib.config.envs import DATABASE_DRIVER, DATABASE_HOST, DATABASE_NAME, DATABASE_PORT, DATABASE_USER, DATABASE_PASSWORD, SECONDS_DB_CONN, RECONNECTION_ATTEMPTS, BOOTSTRAP_SERVERS, TOPIC_NAME, MESSAGE_SLEEP_TIME, MESSAGE_GET_ATTEMPTS
from lib.sql.dql import QUERY_RETRIEVE_LATEST_SALES, QUERY_RETRIEVE_LAST_ID_BUSINESS_TABLE, QUERY_RETRIEVE_LAST_ID_CONTROL_DATA_FLOW_TABLE
from lib.sql.dml import UPDATE_LAST_SALES_ID
from lib.respositories.base import MessageQueue, Database
from json import dumps as json_dumps

from confluent_kafka.serialization import StringSerializer
from uuid import uuid4
from lib.config.database import get_sqlalchemy_engine_conn_cursor

logger = get_logger('pub-service')


# Messages will be serialized as JSON 
def serializer(message):
    return json_dumps(message).encode('utf-8')

if __name__ == "__main__":
    try:
        conn_postgresql_str = f"postgresql+pyodbc://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}?driver={DATABASE_DRIVER}"
        db_conn = Database(connection_string = conn_postgresql_str, reconnection_attempts = RECONNECTION_ATTEMPTS)

        message_sleep_time = int(MESSAGE_SLEEP_TIME)
        message_get_attempts = int(MESSAGE_GET_ATTEMPTS)
        max_message_get_attempts = int(int(SECONDS_DB_CONN) / message_sleep_time)

        string_serializer = StringSerializer(codec='utf_8')
        queue = MessageQueue(bootstrap_servers=BOOTSTRAP_SERVERS)

        # Infinite loop - runs until you kill the program
        while True:
                if message_get_attempts == 1:
                    # the first dabatase connection or renew after a long time
                    engine, conn, cursor = get_sqlalchemy_engine_conn_cursor(conn_postgresql_str)
                    db_conn.cursor = cursor
                    db_conn.engine = engine
                    db_conn.conn = conn

                # get the last Id sent to control flow table
                data_last_id_processed_control_flow = (db_conn.retrieve_data(query=QUERY_RETRIEVE_LAST_ID_CONTROL_DATA_FLOW_TABLE, query_name='Retrieve Last Sales Id in control flow table')).to_json(orient="records")[0]
                last_id_processed_control_flow = int(data_last_id_processed_control_flow['Id'])

                # get the last Id sent to queue in the previous process
                data_last_id_business_table = (db_conn.retrieve_data(query=QUERY_RETRIEVE_LAST_ID_BUSINESS_TABLE, query_name='Retrieve Last Sales Id')).to_json(orient="records")[0]
                last_id_business_table = int(data_last_id_business_table['Id'])

                if last_id_business_table > last_id_processed_control_flow:
                    df = db_conn.retrieve_data(query=QUERY_RETRIEVE_LATEST_SALES, params={"Id": last_id_business_table}, query_name='Retrieve Last Sales')
                    data = df.to_json(orient="records")

                    for message in data:
                        # send the data to producer
                        queue.producer(  topic=TOPIC_NAME
                                        ,value=serializer(message=message)
                                        ,key=string_serializer(str(uuid4()))
                                    )

                    # Synchronous writes
                    queue.producer.flush()

                    # get the last sales id in data flow
                    sales_most_recent_date = df.iloc[df["sales_date"].argmax()]
                    last_sales_id = sales_most_recent_date['Id']

                    # update last sales Id on control table
                    db_conn.update_data(query=UPDATE_LAST_SALES_ID, params={"LastSalesId": last_sales_id})

                # wait for the next process
                time.sleep(message_sleep_time)

                # close dabatase connection after "N" retrieves
                if message_get_attempts < max_message_get_attempts:
                    message_get_attempts = message_get_attempts + 1
                elif message_get_attempts == max_message_get_attempts:
                    # disconnect db connection
                    db_conn.close_cursor()
                    db_conn.close_engines()
                    db_conn.close_connection()
                    # delete objects
                    del db_conn.cursor
                    del db_conn.engine
                    del db_conn.conn
                    # reset counter
                    message_get_attempts = 1

    except BaseException as err:
        logger.error("ERROR: Failed attempt to send message!")
        logger.error(str(err))

    finally:
        if db_conn is not None: db_conn.close_cursor()
        if db_conn is not None: db_conn.close_engines()
        if db_conn is not None: db_conn.close_connection()