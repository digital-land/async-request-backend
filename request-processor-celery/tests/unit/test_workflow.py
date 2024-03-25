from src.application.core.workflow import updateColumnFieldLog, error_summary


def test_updateColumnFieldLog():
    column_field_log = [
        {
            "dataset": "conservation-area",
            "column": "documentation-url",
            "field": "documentation-url",
        },
        {
            "dataset": "conservation-area",
            "column": "name",
            "field": "name",
        },
    ]

    required_fields = ["reference", "geometry"]

    assert len(column_field_log) == 2
    updateColumnFieldLog(column_field_log, required_fields)

    assert len(column_field_log) == 4  # Two new entries added
    assert any(
        entry["field"] == "reference" and entry["missing"] for entry in column_field_log
    )
    assert any(
        entry["field"] == "geometry" and entry["missing"] for entry in column_field_log
    )
    assert any(
        entry["field"] == "documentation-url" and not entry["missing"]
        for entry in column_field_log
    )
    assert any(
        entry["field"] == "name" and not entry["missing"] for entry in column_field_log
    )


def test_updateColumnFieldLog_no_missing_fields():
    column_field_log = [
        {
            "dataset": "conservation-area",
            "column": "documentation-url",
            "field": "documentation-url",
        },
        {"dataset": "conservation-area", "column": "geometry", "field": "geometry"},
        {"dataset": "conservation-area", "column": "reference", "field": "reference"},
    ]
    required_fields = ["reference", "geometry"]
    updateColumnFieldLog(column_field_log, required_fields)
    assert len(column_field_log) == 3
    assert any(
        entry["field"] == "geometry" and not entry["missing"]
        for entry in column_field_log
    )
    assert any(
        entry["field"] == "reference" and not entry["missing"]
        for entry in column_field_log
    )


def test_error_summary():
    issue_log = [
        {
            "dataset": "conservation-area",
            "resource": "d5b003b74563bb5bcf06742ee27f9dd573a47a123f8f5d975d9e04187fa58eff",
            "line-number": "2",
            "entry-number": "1",
            "field": "geometry",
            "issue-type": "OSGB out of bounds of England",
            "value": "",
            "severity": "error",
            "description": "Geometry must be in England",
        },
        {
            "dataset": "conservation-area",
            "resource": "d5b003b74563bb5bcf06742ee27f9dd573a47a123f8f5d975d9e04187fa58eff",
            "line-number": "3",
            "entry-number": "2",
            "field": "geometry",
            "issue-type": "OSGB out of bounds of England",
            "value": "",
            "severity": "error",
            "description": "Geometry must be in England",
        },
        {
            "dataset": "conservation-area",
            "resource": "d5b003b74563bb5bcf06742ee27f9dd573a47a123f8f5d975d9e04187fa58eff",
            "line-number": "3",
            "entry-number": "2",
            "field": "start-date",
            "issue-type": "invalid date",
            "value": "40/04/2024",
            "severity": "error",
            "description": "Start date must be a real date",
        },
    ]
    column_field_log = [{"field": "reference", "missing": True}]
    json_data = error_summary(issue_log, column_field_log)
    expected_messages = [
        "2 geometries must be in England",
        "1 start date must be a real date",
        "Reference column missing",
    ]

    assert any(message in json_data for message in expected_messages)
