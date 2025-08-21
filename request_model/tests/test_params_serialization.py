from datetime import date
import schemas


def test_check_url_params_model_dump_includes_optionals():
    params = schemas.CheckUrlParams(
        type=schemas.RequestTypeEnum.check_url,
        dataset="brownfield-land",
        collection="brownfield-land",
        url="http://example.com/data.csv",
        documentation_url="https://government.gov.uk",
        licence="ogl",
        start_date=date(2025, 8, 10),
    )

    dumped = params.model_dump(mode="json")
    assert dumped["documentation_url"].rstrip("/") == "https://government.gov.uk"
    assert dumped["licence"] == "ogl"
    assert dumped["start_date"] == "2025-08-10"
