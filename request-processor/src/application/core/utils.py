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
    endpoint_csv_path,
    endpoint_url,
    entry_date=None,
    start_date=None,
    end_date=None,
    plugin=None,
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
                "plugin": plugin or "",
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
    Creates a user-friendly error message from CustomException detail, as the message may be displayed to end users.
    Keeps all info from exception_detail and adds user-friendly message.
    """
    status_code = exception_detail.get("errCode")
    exception_type = exception_detail.get("exceptionType")
    content_type = exception_detail.get("contentType")

    user_message = "An error occurred, please try again later."

    if exception_type in ["SSLError", "SSLCertVerificationError"]:
        user_message = "SSL certificate verification failed"
    elif content_type and "text/html" in content_type:
        user_message = (
            "The selected file must be a CSV, GeoJSON, GML or GeoPackage file"
        )
    elif status_code == "403":
        user_message = "The URL must be accessible"
    elif status_code == "404":
        user_message = "The URL does not exist. Check the URL you've entered is correct (HTTP 404 error)"

    result = dict(exception_detail)
    result["message"] = user_message

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


def validate_endpoint(url, config_dir, plugin, start_date=None):
    """Validate if endpoint URL exists in endpoint.csv and create entry if not."""
    endpoint_csv_path = os.path.join(config_dir, "endpoint.csv")
    if not url:
        logger.info("No endpoint URL provided")
        return {}

    if not os.path.exists(endpoint_csv_path):
        os.makedirs(os.path.dirname(endpoint_csv_path), exist_ok=True)
        with open(endpoint_csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "endpoint",
                    "endpoint-url",
                    "parameters",
                    "plugin",
                    "entry-date",
                    "start-date",
                    "end-date",
                ]
            )

    endpoint_exists = False
    existing_entry = None

    try:
        with open(endpoint_csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("endpoint-url", "").strip() == url.strip():
                    endpoint_exists = True
                    existing_entry = {
                        "endpoint": row.get("endpoint", ""),
                        "endpoint-url": row.get("endpoint-url", ""),
                        "parameters": row.get("parameters", ""),
                        "plugin": row.get("plugin", ""),
                        "entry-date": row.get("entry-date", ""),
                        "start-date": row.get("start-date", ""),
                        "end-date": row.get("end-date", ""),
                    }
                    logger.info("Endpoint URL found in endpoint.csv")
                    break
    except Exception as e:
        logger.error(f"Error reading endpoint.csv: {e}")

    endpoint_summary = {"endpoint_url_in_endpoint_csv": endpoint_exists}

    if endpoint_exists and existing_entry:
        endpoint_summary["existing_endpoint_entry"] = existing_entry

    else:
        if not start_date:
            start_date = datetime.now().strftime("%Y-%m-%d")
        entry_date = datetime.now().isoformat()

        endpoint_key, new_endpoint_row = append_endpoint(
            endpoint_csv_path=endpoint_csv_path,
            endpoint_url=url,
            entry_date=entry_date,
            start_date=start_date,
            end_date="",
            plugin=plugin,
        )

        if new_endpoint_row:
            logger.info(f"Appended new endpoint with hash: {endpoint_key}")
            endpoint_summary["new_endpoint_entry"] = new_endpoint_row

    return endpoint_summary


def validate_source(
    documentation_url,
    config_dir,
    collection,
    organisation,
    dataset,
    endpoint_summary,
    start_date=None,
    licence=None,
):
    """Validate if source exists in source.csv and create entry if not."""
    source_csv_path = os.path.join(config_dir, "source.csv")

    endpoint_key = endpoint_summary.get("existing_endpoint_entry", {}).get(
        "endpoint"
    ) or endpoint_summary.get("new_endpoint_entry", {}).get("endpoint")
    if not endpoint_key:
        logger.warning("No endpoint_key available from endpoint_summary")
        return {}

    if not documentation_url:
        logger.warning("No documentation URL provided")

    if not start_date:
        start_date = datetime.now().strftime("%Y-%m-%d")
    entry_date = datetime.now().isoformat()

    source_key_returned, new_source_row = append_source(
        source_csv_path=source_csv_path,
        collection=collection,
        organisation=organisation,
        endpoint_key=endpoint_key,
        attribution="",
        documentation_url=documentation_url or "",
        licence=licence or "",
        pipelines=dataset,
        entry_date=entry_date,
        start_date=start_date,
        end_date="",
    )

    if new_source_row:
        return {
            "documentation_url_in_source_csv": False,
            "new_source_entry": new_source_row,
        }

    source_summary = {"documentation_url_in_source_csv": True}
    try:
        with open(source_csv_path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("source", "").strip() == source_key_returned:
                    source_summary["existing_source_entry"] = {
                        "source": row.get("source", ""),
                        "attribution": row.get("attribution", ""),
                        "collection": row.get("collection", ""),
                        "documentation-url": row.get("documentation-url", ""),
                        "endpoint": row.get("endpoint", ""),
                        "licence": row.get("licence", ""),
                        "organisation": row.get("organisation", ""),
                        "pipelines": row.get("pipelines", ""),
                        "entry-date": row.get("entry-date", ""),
                        "start-date": row.get("start-date", ""),
                        "end-date": row.get("end-date", ""),
                    }
                    break
    except Exception as e:
        logger.error(f"Error reading existing source: {e}")

    return source_summary
