from .custom_exception import CustomException

class ArgumentInvalidException(CustomException):
    """
    ArgumentInvalidException raised for errors.

    Attributes:
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """

    def __init__(self, expression, message):
        self.expression = expression
        self.message = message
        self.status_code = 400