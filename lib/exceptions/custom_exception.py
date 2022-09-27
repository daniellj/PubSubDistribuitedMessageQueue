import json

class CustomException(Exception):
    """
    CustomException raised for errors.

    Attributes:
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """

    def __init__(self, expression, message, status_code):
        self.expression = expression
        self.message = message
        self.status_code = status_code

    def __str__(self):
        message = self.message
        expression = self.expression

        if not isinstance(message, (int, str)):
            message = json.dumps(message)
            
        return f'{expression}: {message}' if expression else message
