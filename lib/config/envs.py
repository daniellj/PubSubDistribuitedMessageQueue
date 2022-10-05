from os import environ
from json import load as json_load


parameters_file=open("./.env")
data = json_load(parameters_file)
DATABASE_ENGINE = environ.get("{PostgreSQL ODBC Driver}", data["Database"]["DB_ENGINE"])
DATABASE_DRIVER = environ.get("{PostgreSQL ODBC Driver}", data["Database"]["DB_DRIVER"])
DATABASE_HOST = environ.get("DB_HOST", data["Database"]["DB_HOST"])
DATABASE_PORT = environ.get("DB_PORT", data["Database"]["DB_PORT"])
DATABASE_NAME = environ.get("DB_DATABASE", data["Database"]["DB_DATABASE"])
DATABASE_USER = environ.get("DB_USER", data["Database"]["DB_USER"])
DATABASE_PASSWORD = environ.get("DB_PASSWORD", data["Database"]["DB_PASSWORD"])
SECONDS_DB_CONN = environ.get("SECONDS_DB_CONN", data["Database"]["SECONDS_DB_CONN"])
DATABASE_RECONNECTION_ATTEMPTS = environ.get("DB_RECONNECTION_ATTEMPTS", data["Database"]["DB_RECONNECTION_ATTEMPTS"])

LOGGING_LEVEL='ERROR'
RECONNECTION_ATTEMPTS='5'
MESSAGE_QUEUE_HOST = environ.get("MESSAGE_QUEUE_HOST", data["MessageQueue"]["MESSAGE_QUEUE_HOST"])
BOOTSTRAP_SERVERS = environ.get("BOOTSTRAP_SERVERS", data["MessageQueue"]["BOOTSTRAP_SERVERS"])
TOPIC_NAME = environ.get("TOPIC_NAME", data["MessageQueue"]["TOPIC_NAME"])

MESSAGE_SLEEP_TIME = environ.get("MESSAGE_SLEEP_TIME", data["MessageQueue"]["MESSAGE_SLEEP_TIME"])
MESSAGE_GET_ATTEMPTS = environ.get("MESSAGE_GET_ATTEMPTS", data["MessageQueue"]["MESSAGE_GET_ATTEMPTS"])

SECURITY_PROTOCOL = environ.get("SECURITY_PROTOCOL", data["MessageQueue"]["SECURITY_PROTOCOL"])
AUTHENTICATION_MECHANISM = environ.get("AUTHENTICATION_MECHANISM", data["MessageQueue"]["AUTHENTICATION_MECHANISM"])
USER_PRINCIPAL = environ.get("USER_PRINCIPAL", data["MessageQueue"]["USER_PRINCIPAL"])
USER_SECRET = environ.get("USER_SECRET", data["MessageQueue"]["USER_SECRET"])

PROJECT_PATH = environ.get("PROJECT_PATH", data["OsEnv"]["ProjectPath"])