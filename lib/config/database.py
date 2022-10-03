import pyodbc
pyodbc.pooling = False

from typing import Optional
from sqlalchemy import create_engine as sqlalchemy_create_engine


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
        _, conn, _ = get_sqlalchemy_engine_conn_cursor(conn_str)
        return conn

    elif authentication is None:
        return pyodbc.connect(
                ('DRIVER={ODBC Driver 17 for SQL Server};SERVER=') + server + '.database.windows.net,1433',
                user=user_name + '@' + server,
                password=password,
                database=database
                )
    else:
         return pyodbc.connect(
                'Driver={ODBC Driver 17 for SQL Server};' +
                'Server=' + server + ',1433' + ';'
                'UID=' + user_name + ';'
                'PWD=' + password + ';'
                'Database=' + database + ';'
                'Authentication=' + authentication + ';'
                )

def close_cursor(cursor):
    if cursor is not None: cursor.close()

def close_engine(engine):
    if engine is not None: engine.close()

def close_connection(conn):
    if conn is not None: conn.close()

def get_sqlalchemy_engine_conn_cursor(db_engine, connection_string):
    engine = None
    try:
        if db_engine == 'mssql':
            engine = sqlalchemy_create_engine(url=connection_string, fast_executemany=True)
        elif db_engine == 'postgresql':
            engine = sqlalchemy_create_engine(url=connection_string)
        conn = engine.raw_connection()
        cursor = conn.cursor()
        return cursor, engine, conn
    except BaseException as e:
        close_engine(engine)
        return None, None, None