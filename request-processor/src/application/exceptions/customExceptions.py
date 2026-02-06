from datetime import datetime


def create_generic_error_log(
    url, exception=None, status=None, plugin=None, extra_context=None
):
    """
    Creates a generic error log dictionary for system errors during URL fetch operations.
    """
    error_log = {
        "message": "There is a problem with our service, please try again later.",
        "endpoint-url": url,
        "status": status,
        "exception": type(exception).__name__
        if exception and not isinstance(exception, str)
        else exception,
    }

    if plugin:
        error_log["plugin"] = plugin
    if extra_context:
        error_log["extra_context"] = extra_context

    return error_log


class CustomException(Exception):
    """Exception raised when URL fetch function fails.
    Attributes: response
    """

    # Log['message'] is required, others are optional
    def __init__(self, log={}):
        if not log.get("message"):
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
        self.plugin = log.get("plugin")

        self.load()
        super().__init__(self.detail)

    def load(self):
        # This is not the best way to do this but keeps backward compatibility for now
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
            "plugin": self.plugin,
        }
