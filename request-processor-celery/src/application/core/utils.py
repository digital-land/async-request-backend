from application.logging.logger import get_logger
import os
import hashlib
from application.configurations.config import Directories
import requests
from cchardet import UniversalDetector
from enum import Enum

logger = get_logger(__name__)

tmp_dir = os.path.join(Directories.COLLECTION_DIR + "/resource")


def save_uploaded_file(file):
    os.makedirs(tmp_dir, exist_ok=True)
    temp_file_path = None  # Initialize temp_file_path
    try:
        temp_file_path = os.path.join(tmp_dir, hash_value(file.filename))
        # file.seek(0)
        with open(temp_file_path, "wb") as temp_file:
            temp_file.write(file.file.read())
    except Exception as e:
        logger.error("Unable to save file: %s", str(e))
    return temp_file_path


def hash_value(data):
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def get_request(url, verify_ssl=True):
    # log["ssl-verify"] = verify_ssl
    log = {"status": "", "message": ""}
    try:
        session = requests.Session()
        user_agent = "DLUHC Digital Land"
        response = session.get(
            url,
            headers={"User-Agent": user_agent},
            timeout=120,
            verify=verify_ssl,
        )
    except (
        requests.exceptions.SSLError,
        requests.ConnectionError,
        requests.HTTPError,
        requests.Timeout,
        requests.TooManyRedirects,
        requests.exceptions.MissingSchema,
        requests.exceptions.ChunkedEncodingError,
    ) as exception:
        logger.warning(exception)
        response = None
        log["message"] = (
            "The requested URL could not be downloaded: " + type(exception).__name__
        )

    content = None
    if response is not None:
        log["status"] = str(response.status_code)
        print("status: ", log["status"])
        print("response: ", response.headers.get("Content-Type"))
        if log["status"] == "200":
            if not response.headers.get("Content-Type", "").startswith("text/html"):
                content = response.content
            else:
                log[
                    "message"
                ] = "The requested URL leads to a html webpage which we cannot process"
        else:
            log["message"] = (
                "The requested URL could not be downloaded: " + log["status"] + " error"
            )
    return log, content


def save_content(content):
    resource = hashlib.sha256(content).hexdigest()
    path = os.path.join(tmp_dir, resource)
    save(path, content)
    return resource


def save(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        logger.info(path)
        with open(path, "wb") as f:
            f.write(data)


def detect_encoding(path):
    with open(path, "rb") as f:
        detector = UniversalDetector()
        detector.reset()

        for line in f:
            detector.feed(line)
            if detector.done:
                break
        detector.close()

        return detector.result["encoding"]


class ErrorMap(Enum):
    USER_ERROR = "User error"
    SYSTEM_ERROR = "System error"
