from .custom_exception import CustomException

class NotFoundException(CustomException):
    """
    NotFoundException raised for errors.

    Attributes:
        message -- explanation of the error
        expression -- input expression in which the error occurred
    """

    def __init__(self, message, expression = None):
        self.message = message
        self.expression = expression
        self.status_code = 404