import datetime
from unittest.mock import patch

import pytest
import shortuuid
from botocore.exceptions import ClientError
from pydantic import ValidationError

from request_model import schemas
from tasks import check_datafile


@patch('s3_transfer_manager.download_with_default_configuration')
@patch('os.remove')
def test_check_datafile(s3_transfer_manager, os_remove):
    request = schemas.Request(
        id=shortuuid.uuid(),
        type=schemas.RequestTypeEnum.check_url,
        created=datetime.datetime.now(),
        modified=datetime.datetime.now(),
        status='NEW',
        params=schemas.CheckFileParams(
            collection="article_4_direction",
            dataset="article_4_direction_area",
            original_filename="bogdan-farca-CEx86maLUSc-unsplash.jpg",
            uploaded_filename="B1E16917-449C-4FC5-96D1-EE4255A79FB1.jpg"
        ).model_dump()
    )
    check_datafile(request)
    assert s3_transfer_manager.called
    assert os_remove.called


def test_check_datafile_with_invalid_request():
    with pytest.raises(ValidationError):
        check_datafile({"type": "invalid"})


def test_check_datafile_with_uploaded_file_ref():
    with pytest.raises(ClientError):
        request = schemas.Request(
            id=shortuuid.uuid(),
            type=schemas.RequestTypeEnum.check_url,
            created=datetime.datetime.now(),
            modified=datetime.datetime.now(),
            status='NEW',
            params=schemas.CheckFileParams(
                collection="article_4_direction",
                dataset="article_4_direction_area",
                original_filename="article_4_direction_area.csv",
                uploaded_filename="unknown.csv"
            ).model_dump()
        )
        check_datafile(request)
