### Set the PROJECT_PATH ###
from os import environ
from json import load as json_load

src_path = ( json_load( open("./.env") ) )["OsEnv"]["ProjectPath"]
PROJECT_PATH = environ.get("PROJECT_PATH", src_path)

from sys import path as sys_path
sys_path.append(PROJECT_PATH) if PROJECT_PATH not in sys_path else True
############################

import time
from lib.helpers.logging import get_logger
from lib.config.envs import DATABASE_ENGINE, DATABASE_HOST, DATABASE_NAME, DATABASE_PORT, DATABASE_USER, DATABASE_PASSWORD, SECONDS_DB_CONN, DATABASE_RECONNECTION_ATTEMPTS, BOOTSTRAP_SERVERS, TOPIC_NAME, MESSAGE_SLEEP_TIME, MESSAGE_GET_ATTEMPTS, SECURITY_PROTOCOL, AUTHENTICATION_MECHANISM, USER_PRINCIPAL, USER_SECRET
from lib.sql.dql import QUERY_RETRIEVE_LATEST_SALES, QUERY_RETRIEVE_LAST_ID_BUSINESS_TABLE, QUERY_RETRIEVE_LAST_ID_CONTROL_DATA_FLOW_TABLE
from lib.sql.dml import UPDATE_LAST_SALES_ID
from lib.respositories.base import MessageQueue, Database
from json import dumps as json_dumps, loads as json_loads

from uuid import uuid4
from lib.config.database import get_sqlalchemy_engine_conn_cursor
from confluent_kafka import Producer


logger = get_logger('pub-service')


if __name__ == "__main__":
    try:
        conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "sasl.mechanism": AUTHENTICATION_MECHANISM,
        "security.protocol": SECURITY_PROTOCOL
        }

        if AUTHENTICATION_MECHANISM != 'GSSAPI':
            conf.update({"sasl.username": USER_PRINCIPAL, "sasl.password": USER_SECRET})

        # Instantiate Producer
        queue = MessageQueue()

        # Instantiate Database
        conn_db_str = f"{DATABASE_ENGINE}://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}"
        db_conn = Database(db_engine = DATABASE_ENGINE, connection_string = conn_db_str, reconnection_attempts = int(DATABASE_RECONNECTION_ATTEMPTS))

        message_sleep_time = int(MESSAGE_SLEEP_TIME)
        message_get_attempts = int(MESSAGE_GET_ATTEMPTS)
        max_message_get_attempts = int(int(SECONDS_DB_CONN) / message_sleep_time)
 
        # delete sensible variables
        del conn_db_str
        del DATABASE_ENGINE
        del DATABASE_USER
        del DATABASE_PASSWORD
        del DATABASE_HOST
        del DATABASE_PORT
        del DATABASE_NAME
        del USER_PRINCIPAL
        del USER_SECRET

        # Infinite loop - runs until you kill the program
        while True:
            if message_get_attempts == 1:
                # the first producer connection or renew after a long time
                queue.producer = Producer(**conf)
                # the first dabatase connection or renew after a long time
                engine, conn, cursor = get_sqlalchemy_engine_conn_cursor(db_engine = db_conn.db_engine, connection_string = db_conn.connection_string)
                db_conn.cursor = cursor
                db_conn.engine = engine
                db_conn.conn = conn

            # get the last Id sent to control flow table
            data_last_id_processed_control_flow = json_loads( (db_conn.retrieve_data(query=QUERY_RETRIEVE_LAST_ID_CONTROL_DATA_FLOW_TABLE, query_name='Retrieve Last Sales Id in control flow table')).to_json(orient="records") )
            last_id_processed_control_flow = int(data_last_id_processed_control_flow[0]['id'])

            # get the last Id sent to queue in the previous process
            data_last_id_business_table = json_loads( (db_conn.retrieve_data(query=QUERY_RETRIEVE_LAST_ID_BUSINESS_TABLE, query_name='Retrieve Last Sales Id')).to_json(orient="records") )
            last_id_business_table = int(data_last_id_business_table[0]['id'])

            if last_id_business_table > last_id_processed_control_flow:
                df = db_conn.retrieve_data(query=QUERY_RETRIEVE_LATEST_SALES, params={"id": last_id_processed_control_flow}, query_name='Retrieve Last Sales')
                data = json_loads( df.to_json(orient="records") )

                for msg in data:
                    # send the data to producer
                    queue.publisher (
                                         topic=TOPIC_NAME
                                        ,message=json_dumps(msg).encode('utf-8')
                                        ,key=str(uuid4())
                                    )

                # Synchronous writes
                #queue.producer.flush()

                # get the last sales id in data flow
                sales_most_recent_date = df.iloc[df["id"].argmax()]
                last_sales_id = sales_most_recent_date['id']

                # update last sales Id on control table
                db_conn.update_data(query=UPDATE_LAST_SALES_ID, params={"id": str(last_sales_id)})

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
                # db objects
                db_conn.cursor = None
                db_conn.engine = None
                db_conn.conn = None
                # queuue objects
                queue.producer = None
                # reset counter
                message_get_attempts = 1
            
            queue.producer.poll(1)
    except BaseException as err:
        logger.error("(ERROR) Failed attempt to send message!")
        logger.error(str(err))

    finally:
        if db_conn is not None: db_conn.close_cursor()
        if db_conn is not None: db_conn.close_engines()
        if db_conn is not None: db_conn.close_connection()