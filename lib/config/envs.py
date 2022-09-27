from os import environ
from json import load as json_load


parameters_file=open("./.env")
data = json_load(parameters_file)
DATABASE_DRIVER = environ.get("{PostgreSQL ODBC Driver}", data["Database"]["PG_DRIVER"])
DATABASE_HOST = environ.get("PG_HOST", data["Database"]["PG_HOST"])
DATABASE_PORT = environ.get("PG_PORT", data["Database"]["PG_PORT"])
DATABASE_NAME = environ.get("PG_DATABASE", data["Database"]["PG_DATABASE"])
DATABASE_USER = environ.get("PG_USER", data["Database"]["PG_USER"])
DATABASE_PASSWORD = environ.get("PG_PASSWORD", data["Database"]["PG_PASSWORD"])
SECONDS_DB_CONN = environ.get("SECONDS_DB_CONN", data["Database"]["SECONDS_DB_CONN"])

LOGGING_LEVEL='ERROR'
RECONNECTION_ATTEMPTS='5'
MESSAGE_QUEUE_HOST = environ.get("MESSAGE_QUEUE_HOST", data["MessageQueue"]["MESSAGE_QUEUE_HOST"])
BOOTSTRAP_SERVERS = environ.get("BOOTSTRAP_SERVERS", data["MessageQueue"]["BOOTSTRAP_SERVERS"])
TOPIC_NAME = environ.get("TOPIC_NAME", data["MessageQueue"]["TOPIC_NAME"])

MESSAGE_SLEEP_TIME = environ.get("MESSAGE_SLEEP_TIME", data["MessageQueue"]["MESSAGE_SLEEP_TIME"])
MESSAGE_GET_ATTEMPTS = environ.get("MESSAGE_GET_ATTEMPTS", data["MessageQueue"]["MESSAGE_GET_ATTEMPTS"])