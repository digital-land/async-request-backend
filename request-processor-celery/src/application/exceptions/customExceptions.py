from datetime import datetime


class URLException(Exception):
    """Exception raised when URL fetch function fails.
    Attributes: response
    """

    def __init__(self, log={}):
        self.log = log
        super().__init__(self.log)
        self.load(log)

    def load(self, log):
        self.detail = {
            "errCode": str(log["status"]),
            "errType": "User Error",
            "errMsg": str(log["message"]),
            "errTime": str(datetime.now()),
        }
