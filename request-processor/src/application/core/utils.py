from application.logging.logger import get_logger
import os
import hashlib
import requests
from cchardet import UniversalDetector
import csv
import json
from datetime import datetime

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


def hash_sha256(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def hash_md5(value):
    return hashlib.md5(value.encode("utf-8")).hexdigest()


def append_endpoint(
    endpoint_csv_path, endpoint_url, entry_date=None, start_date=None, end_date=None
):
    endpoint_key = hash_sha256(endpoint_url)
    exists = False
    new_row = None

    if os.path.exists(endpoint_csv_path) and os.path.getsize(endpoint_csv_path) > 0:
        with open(endpoint_csv_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        while lines and lines[-1].strip() == "":
            lines.pop()
        with open(endpoint_csv_path, "w", encoding="utf-8", newline="") as f:
            f.writelines(lines)

    if os.path.exists(endpoint_csv_path):
        with open(endpoint_csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("endpoint-url") == endpoint_url:
                    exists = True
                    break
    if not exists:
        with open(endpoint_csv_path, "a", newline="", encoding="utf-8") as f:
            fieldnames = [
                "endpoint",
                "endpoint-url",
                "parameters",
                "plugin",
                "entry-date",
                "start-date",
                "end-date",
            ]
            new_row = {
                "endpoint": endpoint_key,
                "endpoint-url": endpoint_url,
                "parameters": "",
                "plugin": "",
                "entry-date": _formatted_date(entry_date)
                or datetime.now().date().isoformat(),
                "start-date": _formatted_date(start_date),
                "end-date": _formatted_date(end_date),
            }
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writerow(new_row)
    return endpoint_key, new_row


def append_source(
    source_csv_path,
    collection,
    organisation,
    endpoint_key,
    attribution="",
    documentation_url="",
    licence="",
    pipelines="",
    entry_date=None,
    start_date=None,
    end_date=None,
):
    source_key = hash_md5(f"{collection}|{organisation}|{endpoint_key}")
    exists = False
    new_row = None
    if os.path.exists(source_csv_path) and os.path.getsize(source_csv_path) > 0:
        with open(source_csv_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        while lines and lines[-1].strip() == "":
            lines.pop()
        with open(source_csv_path, "w", encoding="utf-8", newline="") as f:
            f.writelines(lines)

    if os.path.exists(source_csv_path):
        with open(source_csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("source") == source_key:
                    exists = True
                    break
    if not exists:
        with open(source_csv_path, "a", newline="", encoding="utf-8") as f:
            fieldnames = [
                "source",
                "attribution",
                "collection",
                "documentation-url",
                "endpoint",
                "licence",
                "organisation",
                "pipelines",
                "entry-date",
                "start-date",
                "end-date",
            ]
            new_row = {
                "source": source_key,
                "attribution": attribution,
                "collection": collection,
                "documentation-url": documentation_url,
                "endpoint": endpoint_key,
                "licence": licence,
                "organisation": organisation,
                "pipelines": pipelines,
                "entry-date": _formatted_date(entry_date)
                or datetime.now().date().isoformat(),
                "start-date": _formatted_date(start_date),
                "end-date": _formatted_date(end_date),
            }
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writerow(new_row)
    return source_key, new_row


def create_user_friendly_error_log(exception_detail):
    """
    Creates a user-friendly error log from CustomException detail, as the message will be displayed to end users.
    Keeps all info from exception_detail and adds user-friendly message.
    """
    status_code = exception_detail.get("errCode")
    exception_type = exception_detail.get("exceptionType")
    content_type = exception_detail.get("contentType")

    user_message = "An error occurred, please try again later."
    user_message_detail = None

    if exception_type in ["SSLError", "SSLCertVerificationError"]:
        user_message = "SSL certificate verification failed"
        user_message_detail = [
            "We couldn't verify the SSL certificate for that link. While it may open in some browsers, our security checks require a complete certificate chain.",
            "Please make sure the site is served over HTTPS and its SSL certificate is correctly installed.",
        ]
    elif content_type and "text/html" in content_type:
        user_message = (
            "The selected file must be a CSV, GeoJSON, GML or GeoPackage file"
        )
        user_message_detail = "The URL returned a webpage (HTML) instead of a data file. Please provide a direct link to the data file (for example: .csv, .geojson, .gml or .gpkg)."
    elif status_code == "403":
        user_message = "The URL must be accessible"
        user_message_detail = [
            "You must host the URL on a server which does not block access due to set permissions.",
            "Contact your IT team for support if you need it, referencing a 'HTTP status code 403' error.",
        ]
    elif status_code == "404":
        user_message = "The URL does not exist. Check the URL you've entered is correct (HTTP 404 error)"

    result = dict(exception_detail)
    result["message"] = user_message
    if user_message_detail is not None:
        result["user_message_detail"] = user_message_detail

    return result


def _formatted_date(date_value):
    if not date_value:
        return ""
    if isinstance(date_value, datetime):
        return date_value.date().isoformat()
    if isinstance(date_value, str):
        if len(date_value) == 10 and date_value[4] == "-" and date_value[7] == "-":
            return date_value
        if "T" in date_value:
            return date_value.split("T")[0]
        try:
            return datetime.fromisoformat(date_value).date().isoformat()
        except Exception:
            pass
    return str(date_value)
