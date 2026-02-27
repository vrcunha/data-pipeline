"""Unit tests for bronze strategies."""

from unittest.mock import MagicMock, patch

from data_pipeline.strategies import (
    BronzeAsyncAPIExtract,
    BronzeLoad,
    BronzeQuality,
    BronzeTransform,
)


def test_bronze_extract_get_total_pages():
    """Ensure pagination size is computed from metadata."""
    strategy = BronzeAsyncAPIExtract()
    with patch(
        "data_pipeline.strategies.bronze.extract.requests.get"
    ) as req_get:
        req_get.return_value.json.return_value = {"total": 401}
        total_pages = strategy.get_total_pages("http://metadata")

    assert total_pages == 3


def test_bronze_extract_execute():
    """Ensure execute orchestrates async extraction flow."""
    strategy = BronzeAsyncAPIExtract()
    context = {
        "metadata_breweries_url": "http://meta",
        "list_breweries_url": "http://list",
    }
    expected = [{"id": "1"}]

    with patch.object(strategy, "get_total_pages", return_value=1):
        with patch.object(
            strategy, "_gather_pages", new=lambda *args, **kwargs: expected
        ):
            with patch(
                "data_pipeline.strategies.bronze.extract.asyncio.get_event_loop"
            ) as get_loop:
                loop = MagicMock()
                loop.run_until_complete.return_value = expected
                get_loop.return_value = loop

                result = strategy.execute(context)

    assert result == expected
    loop.run_until_complete.assert_called_once_with(expected)


def test_bronze_transform_returns_input():
    """Ensure bronze transform is pass-through."""
    strategy = BronzeTransform()
    data = [{"id": "1"}]

    result = strategy.execute(data, {})

    assert result is data


def test_bronze_load_puts_file_on_s3():
    """Ensure bronze load writes one JSON object to storage."""
    strategy = BronzeLoad()
    s3_mock = MagicMock()
    data = [{"id": "1"}]
    context = {
        "source": "openbrewerydb",
        "execution_date": "2026-02-26",
        "destination_bucket": "bronze",
    }

    with patch.object(strategy, "s3_client", return_value=s3_mock):
        strategy.execute(data, context)

    s3_mock.put_object.assert_called_once()
    kwargs = s3_mock.put_object.call_args.kwargs
    assert kwargs["Bucket"] == "bronze"
    assert kwargs["Key"].startswith("openbrewerydb/2026-02-26/")


def test_bronze_quality_validate_noop():
    """Ensure current bronze quality validator does not raise."""
    strategy = BronzeQuality()
    data = {
        "id": "1",
        "address_1": "",
        "address_2": "",
        "address_3": "",
        "brewery_type": "",
        "city": "",
        "country": "",
        "latitude": "",
        "longitude": "",
        "name": "",
        "phone": "",
        "postal_code": "",
        "state": "",
        "state_province": "",
        "street": "",
        "website_url": "",
    }

    strategy.validate([data], {})
