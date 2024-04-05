import datetime
import database
from tasks import save_response_to_db
from request_model import models, schemas


def test_save_response_to_db_success(db):
    # Set up test data
    response_data = {
        "column-field-log": {},
        "error-summary": {},
        "converted-csv": [
            {"column1": "value1", "column2": "value2"},
            {"column1": "value3", "column2": "value4"},
        ],
        "issue-log": [
            {"entry-number": "1", "issue": "Issue 1"},
            {"entry-number": "2", "issue": "Issue 2"},
        ],
    }
    request_model = models.Request(
        type=schemas.RequestTypeEnum.check_file,
        created=datetime.datetime.now(),
        modified=datetime.datetime.now(),
        status="NEW",
        params=schemas.CheckFileParams(
            collection="article-4-direction",
            dataset="article-4-direction-area",
            original_filename="article-direction-area.csv",
            uploaded_filename="492f15d8-45e4-427e-bde0-f60d69889f40",
        ).model_dump(),
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
        response=None,
    )
    # Call the function
    save_response_to_db(request.id, response_data)

    with db_session() as session:
        # Check if response table has data
        response_query = (
            session.query(models.Response)
            .filter_by(request_id=request_model.id)
            .first()
        )
        assert response_query is not None, "Response table should contain data"
        data = response_query.data
        assert "error-summary" in data, "error-summary should be present in data"
        assert "column-field-log" in data, "column-field-log should be present in data"

        # Check if response_details table has details
        response_details_query = (
            session.query(models.ResponseDetails)
            .filter_by(response_id=response_query.id)
            .first()
        )
        assert (
            response_details_query is not None
        ), "ResponseDetails table should contain details"
        detail = response_details_query.detail
        assert "converted_row" in detail, "converted_row should be present in data"
        assert "issue_logs" in detail, "issue_logs should be present in data"
        assert "entry_number" in detail, "entry_number should be present in data"


def test_save_response_to_db_exception(db):
    # Set up test data
    response_data = {"message": "Test message", "status": "404"}
    request_model = models.Request(
        type=schemas.RequestTypeEnum.check_url,
        created=datetime.datetime.now(),
        modified=datetime.datetime.now(),
        status="NEW",
        params=schemas.CheckUrlParams(
            collection="article-4-direction",
            dataset="article-4-direction-area",
            url="invalidurl.geojson",
        ).model_dump(),
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
        response=None,
    )
    save_response_to_db(request.id, response_data)

    with db_session() as session:
        # Check if response table has error
        response_query = (
            session.query(models.Response).filter_by(request_id=request.id).first()
        )
        assert response_query is not None, "Response table should contain data"
        error = response_query.error
        assert "errCode" in error, "errCode should be present in data"
        assert "errMsg" in error, "errMsg should be present in data"
        assert "errTime" in error, "errTime should be present in data"
        assert "errType" in error, "errType should be present in data"
