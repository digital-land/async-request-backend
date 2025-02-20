from application.logging.logger import get_logger
import os
import hashlib
import requests
from cchardet import UniversalDetector
import csv
import json

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


def check_content(content):
    """
    Determines if the response content from a URL contains multiple layers.

    Parameters:
        content (bytes/str/dict): The content to check.

    Returns:
        bool: True if valid, False if the URL contains multiple layers.
    """
    try:
        if isinstance(content, bytes):
            content = content.decode("utf-8")
        if isinstance(content, str):
            content = json.loads(content)

        if (
            "layers" in content
            and isinstance(content["layers"], list)
            and len(content["layers"]) > 1
        ):
            return False
    except Exception as e:
        logger.warning(f"Error checking/parsing content. Proceeding as normal: {e}")
        return True  # Assume valid if we can't parse it
    return True


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


def extract_dataset_field_rows(folder_path, dataset):
    csv_file_path = os.path.join(folder_path, "dataset-field.csv")
    if os.path.exists(csv_file_path):
        reader = csv.DictReader(open(csv_file_path))
        if reader is not None:
            filtered_rows = [
                row for row in reader if "dataset" in row and row["dataset"] == dataset
            ]
        return filtered_rows
    else:
        logger.error("Error extracting dataset-field.csv in the specified folder.")
        return None
