from neat.errors import *

class Error(Exception):
    pass

class RecordError(Error):
    pass

class TimeoutError(Error):
    pass

class APIError(Error):
    """Raised when the server rejects the client's API call."""
    
    def __init__(self, message, response):
        self.response = response

        message = message + " (HTTP status: %d)" % self.response.getcode()

        super(APIError, self).__init__(message)
