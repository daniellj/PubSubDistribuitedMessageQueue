import json
from typing import Type
from exceptions.argument_invalid_exception import ArgumentInvalidException

def try_cast(value, cast_type: Type, variable_name = None):
    try:
        return cast_type(value)
    except ValueError as e:
        raise ArgumentInvalidException(expression=variable_name, message=str(e))