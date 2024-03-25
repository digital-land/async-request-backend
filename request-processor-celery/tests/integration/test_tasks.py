import datetime
import time

import pytest

import database
from request_model import models, schemas
from tasks import check_datafile


def test_check_datafile(celery_app, celery_worker, s3_bucket, db):
    request_model = models.Request(
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
    db_session = database.session_maker()
    with db_session() as session:
        session.add(request_model)
        session.commit()
        session.refresh(request_model)
    request = schemas.Request(
        id=request_model.id,
        type=request_model.type,
        status=request_model.status,
        created=request_model.created,
        modified=request_model.modified,
        params=request_model.params,
    )
    check_datafile_task = celery_app.register_task(check_datafile)
    check_datafile_task.delay(request.model_dump())
    _wait_for_request_status(request.id, 'COMPLETE')


def _wait_for_request_status(request_id, expected_status, timeout_seconds=10, interval_seconds=1):
    seconds_waited = 0
    actual_status = 'UNKNOWN'
    while seconds_waited <= timeout_seconds:
        db_session = database.session_maker()
        with db_session() as session:
            result = (session.query(models.Request)
                      .filter(models.Request.id == request_id)
                      .first())
            actual_status = result.status
            if actual_status == expected_status:
                return
            else:
                time.sleep(interval_seconds)
                seconds_waited += interval_seconds
                print(f"Waiting {interval_seconds} second(s) for expected status of {expected_status} on request {request_id}")

    pytest.fail(f"Expected status of {expected_status} for request {request_id} but actual status was {actual_status}")
