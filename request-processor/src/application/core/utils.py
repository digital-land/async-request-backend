from application.logging.logger import get_logger
import os
import hashlib
import requests
from cchardet import UniversalDetector

logger = get_logger(__name__)


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
    except requests.RequestException as exception:
        logger.warning(exception)
        response = None
        log["message"] = (
            "The requested URL could not be downloaded: " + type(exception).__name__
        )

    content = None
    if response is not None:
        log["status"] = str(response.status_code)
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


def save_content(content, tmp_dir):
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
