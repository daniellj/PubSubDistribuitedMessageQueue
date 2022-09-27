import logging
from lib.config.envs import LOGGING_LEVEL

def get_logger(name) -> logging:
    logger = logging.getLogger(name)
    logger.setLevel(LOGGING_LEVEL)
    ch = logging.StreamHandler()
    ch.setLevel(LOGGING_LEVEL)
    logger.addHandler(ch)
    return logger