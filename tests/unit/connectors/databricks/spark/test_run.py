"""Spark orchestration boundaries tested without external services."""

from unittest.mock import MagicMock

from neocarta.connectors.databricks.spark import run
from neocarta.connectors.databricks.spark.ingest.load.neo4j_io import Neo4jConfig
from neocarta.connectors.databricks.spark.settings import SparkIngestSettings


def test_wheel_entrypoint_discards_summary(monkeypatch):
    ingest = MagicMock(return_value=object())
    monkeypatch.setattr(run, "run_ingest", ingest)
    assert run.main() is None
    ingest.assert_called_once_with()


def test_external_ingest_records_graph_metadata_after_relationships(monkeypatch):
    settings = SparkIngestSettings(catalog="sales", include_values=False)
    summary = run._build_summary("run-1", settings, [])
    extraction = MagicMock()
    neo4j = Neo4jConfig("neo4j://localhost", "neo4j", "secret")
    operations = MagicMock()
    driver = MagicMock()
    import neo4j as neo4j_module

    monkeypatch.setattr(neo4j_module.GraphDatabase, "driver", MagicMock(return_value=driver))
    for name in (
        "preflight",
        "bootstrap_constraints",
        "_write_nodes",
        "_load",
        "upsert_neocarta_graph_node",
        "query_counts",
    ):
        method = MagicMock()
        operations.attach_mock(method, name)
        monkeypatch.setattr(run, name, method)
    monkeypatch.setattr(run, "extract", MagicMock(return_value=extraction))
    monkeypatch.setattr(run, "transform_sample_values", MagicMock(return_value=None))
    monkeypatch.setattr(run, "run_fk_discovery", MagicMock())

    run._run(MagicMock(), settings, [], summary, neo4j)

    names = [call[0] for call in operations.mock_calls]
    assert names.index("_load") < names.index("upsert_neocarta_graph_node")
    operations.upsert_neocarta_graph_node.assert_called_once_with(driver.__enter__.return_value)
    extraction.unpersist_cached.assert_called_once()


def test_connection_repr_hides_password():
    assert "secret-password" not in repr(
        Neo4jConfig("neo4j://localhost", "neo4j", "secret-password")
    )
