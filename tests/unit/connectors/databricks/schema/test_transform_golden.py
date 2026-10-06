"""Preserve the warehouse connector's graph output while adding the Spark path."""

from pathlib import Path

from neocarta.connectors.databricks.schema.transform import DatabricksSchemaTransformer
from tests.support.characterization import assert_matches_golden, serialize_transform


def test_warehouse_transform_stays_compatible(databricks_extractor_with_cache):
    extractor = databricks_extractor_with_cache
    transformer = DatabricksSchemaTransformer()
    for method, frame in (
        ("transform_to_database_nodes", "database_info"),
        ("transform_to_schema_nodes", "schema_info"),
        ("transform_to_table_nodes", "table_info"),
        ("transform_to_column_nodes", "column_info"),
        ("transform_to_value_nodes", "column_unique_values"),
        ("transform_to_has_schema_relationships", "schema_info"),
        ("transform_to_has_table_relationships", "table_info"),
        ("transform_to_has_column_relationships", "column_info"),
        ("transform_to_references_relationships", "column_references_info"),
        ("transform_to_has_value_relationships", "column_unique_values"),
    ):
        getattr(transformer, method)(getattr(extractor, frame))
    assert_matches_golden(
        Path(__file__).parent / "golden" / "databricks_schema_transform.json",
        serialize_transform(transformer),
    )
