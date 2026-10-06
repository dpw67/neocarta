"""Warehouse improvements adapted from the Spark ingestion branch."""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from neocarta.connectors.databricks import DatabricksSchemaConnector
from neocarta.connectors.databricks.schema.extract import DatabricksSchemaExtractor
from neocarta.errors import ConfigError


def test_cardinality_filter_skips_high_cardinality_and_complex_columns():
    extractor = DatabricksSchemaExtractor(MagicMock(), "catalog", sample_cardinality_threshold=50)
    extractor._run_query = MagicMock(
        side_effect=[
            pd.DataFrame([{"id": 1000, "status": 3}]),
            pd.DataFrame([{"status": ["complete", "pending"]}]),
        ]
    )
    columns = pd.DataFrame(
        [
            {"table_name": "orders", "column_name": "id", "data_type": "INT"},
            {"table_name": "orders", "column_name": "status", "data_type": "STRING"},
            {"table_name": "orders", "column_name": "details", "data_type": "STRUCT"},
        ]
    )

    result = extractor.extract_column_unique_values_for_table(
        "orders", ["id", "status", "details"], "sales", column_info=columns
    )

    assert result["unique_value"].tolist() == ["complete", "pending"]
    assert set(result["column_name"]) == {"status"}
    queries = [call.args[0] for call in extractor._run_query.call_args_list]
    assert "approx_count_distinct(`id`)" in queries[0]
    assert "`details`" not in queries[0]
    assert "collect_set(`status`)" in queries[1]
    assert "collect_set(`id`)" not in queries[1]


def test_cardinality_filter_writes_no_samples_when_all_columns_are_excluded():
    extractor = DatabricksSchemaExtractor(MagicMock(), "catalog", sample_cardinality_threshold=50)
    extractor._run_query = MagicMock(return_value=pd.DataFrame([{"id": 50}]))
    columns = pd.DataFrame([{"table_name": "orders", "column_name": "id", "data_type": "INT"}])

    result = extractor.extract_column_unique_values_for_table(
        "orders", ["id"], "sales", column_info=columns
    )

    assert result.empty
    extractor._run_query.assert_called_once()


@pytest.mark.parametrize("threshold", [-1, True, "50"])
def test_cardinality_threshold_is_validated(threshold):
    with pytest.raises(ConfigError, match="sample_cardinality_threshold"):
        DatabricksSchemaExtractor(MagicMock(), "catalog", sample_cardinality_threshold=threshold)


@pytest.mark.parametrize("overwrite", [False, True])
def test_metadata_refresh_reuses_loader_and_preserves_default(overwrite):
    connector = DatabricksSchemaConnector(
        MagicMock(), "catalog", MagicMock(), overwrite_existing=overwrite
    )
    connector.loader = MagicMock()
    connector.transformer = MagicMock()
    connector._transformed = True

    connector.load()

    for kind in ("database", "schema", "table", "column", "value"):
        call = getattr(connector.loader, f"load_{kind}_nodes").call_args
        assert call.kwargs == ({"overwrite_existing": True} if overwrite else {})
