import pyodbc
pyodbc.pooling = False

from typing import Optional
from sqlalchemy import create_engine as sqlalchemy_create_engine
from urllib.parse import quote_plus


def get_connection(server: Optional[str]='',
                   database: Optional[str]='',
                   user_name: Optional[str]='',
                   password: Optional[str]='',
                   authentication: Optional[str]=None,
                   conn_str: Optional[str]=None):
    """[Connect to DB]
    The connectin can be made using separate parameters or using a connection string.
    

    Args:
        server ([type]): [DB server name]
        database ([type]): [DB name]
        user_name ([type]): [DB user]
        password ([type]): [DB password]
        authentication (optional string): [DB Authentication method]
        conn_str (optional string): [DB connection string]
        
    Returns:
        [conn]: [Connection to DB]
    """

    if conn_str is not None:
        _, conn, _ = get_sqlalchemy_engine_conn_cursor(quote_plus(conn_str))
        return conn

    elif authentication is None:
        return pyodbc.connect(
                quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER=') + quote_plus(server) + '.database.windows.net,1433',
                user=quote_plus(user_name) + '@' + quote_plus(server),
                password=quote_plus(password),
                database=quote_plus(database)
                )
    else:
         return pyodbc.connect(
                quote_plus('Driver={ODBC Driver 17 for SQL Server};') +
                'Server=' + quote_plus(server) + ',1433' + ';'
                'UID=' + quote_plus(user_name) + ';'
                'PWD=' + quote_plus(password) + ';'
                'Database=' + quote_plus(database) + ';'
                'Authentication=' + authentication + ';'
                ) 

def close_cursor(cursor):
    if cursor is not None: cursor.close()

def close_engine(engine):
    if engine is not None: engine.dispose()

def close_connection(conn):
    if conn is not None: conn.close()

def get_sqlalchemy_engine_conn_by_connection_string(connection_string):
    return sqlalchemy_create_engine(quote_plus(connection_string), fast_executemany=True)

def get_sqlalchemy_engine_conn_cursor(connection_string):
    engine = None
    try:
        engine = get_sqlalchemy_engine_conn_by_connection_string(connection_string)
        conn = engine.raw_connection()
        cursor = conn.cursor()
        return cursor, engine, conn
    except BaseException as e:
        close_engine(engine)
        return None, None, None