"""Spark-logic tests for the node builders' transient ``embedding_text`` column.

These exercise the real DataFrame builders against a local SparkSession, so they
run under the ``databricks-spark`` extra (``uv run pytest tests/unit/connectors/databricks/spark``). The
top-level ``pyspark`` import is guarded with ``importorskip`` so collection in
the default ``test-unit`` group skips, rather than errors, this module.
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from neocarta.connectors.databricks.spark.ingest.contract_expr import node_id, qualified_name
from neocarta.connectors.databricks.spark.ingest.schema_graph import (
    build_column_nodes,
    build_database_nodes,
    build_schema_nodes,
    build_table_nodes,
)


def _one(df, column):
    """Return the single-row value of ``column`` from a one-row DataFrame."""
    rows = df.collect()
    assert len(rows) == 1
    return rows[0][column]


def _tables_schema():
    """information_schema.tables shape, with explicit nullable timestamp types so
    the all-None ``created`` / ``last_altered`` columns do not break inference."""
    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

    return StructType(
        [
            StructField("table_catalog", StringType()),
            StructField("table_schema", StringType()),
            StructField("table_name", StringType()),
            StructField("comment", StringType()),
            StructField("table_type", StringType()),
            StructField("created", TimestampType()),
            StructField("last_altered", TimestampType()),
        ]
    )


def test_database_nodes_embedding_text_uses_name(local_spark):
    """A catalog without a description still embeds its name."""
    df = build_database_nodes(local_spark, ["sales"], platform="AWS")
    assert "embedding_text" in df.columns
    assert _one(df, "embedding_text") == "sales"
    assert _one(df, "platform") == "DATABRICKS"
    assert _one(df, "service") == "UNITY_CATALOG"
    assert _one(df, "cloud_platform") == "AWS"


def test_column_without_description_embeds_name_and_type(local_spark):
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField(name, StringType())
            for name in (
                "table_catalog",
                "table_schema",
                "table_name",
                "column_name",
                "data_type",
                "is_nullable",
                "comment",
            )
        ]
        + [StructField("ordinal_position", LongType())]
    )
    columns = local_spark.createDataFrame(
        [("sales", "public", "orders", " customer_id ", " INTEGER ", "YES", None, 1)],
        schema,
    )
    assert _one(build_column_nodes(columns), "embedding_text") == "customer_id | INTEGER"


def test_schema_nodes_embedding_text_is_comment(local_spark):
    """Schema embedding text is the name and description, matching the shared path."""
    schemata = local_spark.createDataFrame(
        [("sales", "public", "customer facing")],
        ["catalog_name", "schema_name", "comment"],
    )
    df = build_schema_nodes(schemata)
    assert "embedding_text" in df.columns
    assert _one(df, "embedding_text") == "public | customer facing"


def test_schema_nodes_embedding_text_blank_comment_uses_name(local_spark):
    """A blank comment leaves the name available for retrieval."""
    schemata = local_spark.createDataFrame(
        [("sales", "public", "   ")],
        ["catalog_name", "schema_name", "comment"],
    )
    assert _one(build_schema_nodes(schemata), "embedding_text") == "public"


def test_table_nodes_embedding_text_is_comment(local_spark):
    """Table embedding text is the name and description."""
    tables = local_spark.createDataFrame(
        [("sales", "public", "orders", "order facts", "MANAGED", None, None)],
        _tables_schema(),
    )
    df = build_table_nodes(tables)
    assert "embedding_text" in df.columns
    assert _one(df, "embedding_text") == "orders | order facts"


def test_column_nodes_embedding_text_is_comment(local_spark):
    """Column embedding text is the name, type, and description."""
    columns = local_spark.createDataFrame(
        [("sales", "public", "orders", "total", "DECIMAL", "YES", "line total", 3)],
        [
            "table_catalog",
            "table_schema",
            "table_name",
            "column_name",
            "data_type",
            "is_nullable",
            "comment",
            "ordinal_position",
        ],
    )
    df = build_column_nodes(columns)
    assert "embedding_text" in df.columns
    assert _one(df, "embedding_text") == "total | DECIMAL | line total"


def test_schema_node_id_and_qualified_name_agree_with_python(local_spark):
    """The Spark builder's id/qualified_name match the Python builders byte-for-
    byte, so driver-built edge endpoints (FK, candidates) connect to their nodes.
    The id is the normalized compose_id (hyphen folded to ``_``); qualified_name
    keeps the hyphen."""
    schemata = local_spark.createDataFrame(
        [("sales", "graph-enriched-schema", "curated")],
        ["catalog_name", "schema_name", "comment"],
    )
    df = build_schema_nodes(schemata)
    assert _one(df, "id") == node_id("sales", "graph-enriched-schema")
    assert _one(df, "qualified_name") == qualified_name("sales", "graph-enriched-schema")
    assert _one(df, "qualified_name") == "sales.graph-enriched-schema"
    assert _one(df, "id") == "sales.graph_enriched_schema"  # normalized, not the readable path


def test_builders_do_not_leak_information_schema_helpers(local_spark):
    """Only declared properties plus the transient embedding_text survive; raw
    information_schema helper columns never leave the builder."""
    tables = local_spark.createDataFrame(
        [("sales", "public", "orders", "c", "MANAGED", None, None)],
        _tables_schema(),
    )
    cols = set(build_table_nodes(tables).columns)
    assert "embedding_text" in cols
    assert "table_catalog" not in cols
    assert "comment" not in cols  # surfaces as `description`, not the raw name
