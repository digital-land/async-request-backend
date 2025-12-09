from datetime import datetime


class CustomException(Exception):
    """Exception raised when URL fetch function fails.
    Attributes: response
    """

    # Log['message'] is required, others are optional
    def __init__(self, log={}):
        if "message" not in log or not log["message"]:
            raise ValueError("The 'message' field is required in the log dictionary.")
        self.message = log["message"]

        # Initialize only certain attributes from log dictionary if they are present.
        self.status = log.get("status")
        self.endpoint_url = log.get("endpoint-url")
        self.entry_date = log.get("entry-date")
        self.fetch_status = log.get("fetch-status")
        self.exception_type = log.get("exception")
        # Content type of the response useful for text/html checks (when not using arcgis/wfs plugin)
        self.content_type = log.get("content-type")
        self.message_detail = log.get("user_message_detail")

        self.load()
        super().__init__(self.detail)

    def load(self):
        self.detail = {
            "errCode": str(self.status) if self.status is not None else None,
            "errType": "User Error",
            "errMsg": str(self.message),
            "errMsgDetail": self.message_detail,
            "errTime": str(datetime.now()),
            "endpointUrl": self.endpoint_url,
            "entryDate": self.entry_date,
            "fetchStatus": self.fetch_status,
            "exceptionType": self.exception_type,
            "contentType": self.content_type,
        }
