"""Foreign-key inference over both core and enriched schema graphs."""

from unittest.mock import MagicMock

import pytest

from neocarta.enrichment.foreign_keys import infer_foreign_keys
from neocarta.enrichment.foreign_keys.infer import INFERRED_SOURCE, _infer_edges
from neocarta.enrichment.foreign_keys.rules import ColumnMeta
from neocarta.errors import ConfigError


def _columns():
    return [
        ColumnMeta(
            "customers.id", "catalog", "schema", "customers", "id", "INT", "customer key", True
        ),
        ColumnMeta(
            "orders.customer_id",
            "catalog",
            "schema",
            "orders",
            "customer_id",
            "INT",
            "customer key",
            False,
        ),
    ]


def test_inference_proposes_join_and_preserves_declared_pair():
    expected = [("orders.customer_id", "customers.id", 0.88)]
    assert _infer_edges(_columns(), set(), 0.8) == expected
    assert _infer_edges(_columns(), {expected[0][:2]}, 0.8) == []


def test_graph_workflow_reads_hierarchy_and_counts_actual_writes():
    driver = MagicMock()
    session = driver.session.return_value.__enter__.return_value
    column_rows = [
        {
            "id": c.col_id,
            "catalog": c.catalog,
            "schema": c.schema,
            "table": c.table,
            "name": c.column,
            "type": c.data_type,
            "description": c.description,
            "is_primary_key": c.is_primary_key,
        }
        for c in _columns()
    ]
    read_columns = MagicMock()
    read_columns.data.return_value = column_rows
    read_declared = MagicMock()
    read_declared.data.return_value = []
    write = MagicMock()
    write.single.return_value = {"written": 1}
    session.run.side_effect = [read_columns, read_declared, write]

    result = infer_foreign_keys(driver, database_name="catalog_graph", catalogs=["catalog"])

    assert (result.columns_scanned, result.edges_written) == (2, 1)
    calls = session.run.call_args_list
    assert "HAS_SCHEMA" in calls[0].args[0]
    assert calls[0].kwargs["catalogs"] == ["catalog"]
    assert calls[1].kwargs["inferred_source"] == INFERRED_SOURCE
    assert calls[2].kwargs["rows"] == [
        {"source_id": "orders.customer_id", "target_id": "customers.id", "confidence": 0.88}
    ]
    assert "WHERE r.source = $source" in calls[2].args[0]
    assert all(
        call.kwargs == {"database": "catalog_graph"} for call in driver.session.call_args_list
    )


@pytest.mark.parametrize("threshold", [0, -1, 1.1, float("nan"), float("inf")])
def test_invalid_threshold_fails_before_reading_graph(threshold):
    driver = MagicMock()
    with pytest.raises(ConfigError, match="threshold"):
        infer_foreign_keys(driver, threshold=threshold)
    driver.session.assert_not_called()
