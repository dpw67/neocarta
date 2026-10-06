"""Additive graph models for the Databricks Spark connector."""

from datetime import datetime

from pydantic import ConfigDict, Field

from ...instance import Value
from .models import Column, Database, References, Schema, Table


class DatabricksDatabase(Database):
    """Database node emitted by the Databricks connector."""

    qualified_name: str = Field(
        ..., description="Lossless lowercased dotted path; the readable form of the normalized id"
    )
    contract_version: str = Field(..., description="Graph contract version marker")
    cloud_platform: str | None = Field(default=None, description="Optional AWS/AZURE/GCP cloud tag")


class DatabricksSchema(Schema):
    """Schema node emitted by the Databricks connector."""

    qualified_name: str = Field(
        ..., description="Lossless lowercased dotted path; the readable form of the normalized id"
    )
    contract_version: str = Field(..., description="Graph contract version marker")


class DatabricksTable(Table):
    """Table node emitted by the Databricks connector (core + additive)."""

    model_config = ConfigDict(populate_by_name=True)

    qualified_name: str = Field(
        ..., description="Lossless lowercased dotted path; the readable form of the normalized id"
    )
    catalog: str = Field(..., description="Unity Catalog catalog containing the table")
    schema_: str = Field(..., alias="schema", description="Schema containing the table")
    layer: str | None = Field(default=None, description="Medallion layer (bronze/silver/gold)")
    table_type: str | None = Field(default=None, description="MANAGED / EXTERNAL / VIEW")
    created: datetime | None = Field(default=None, description="Catalog creation timestamp")
    last_altered: datetime | None = Field(
        default=None, description="Catalog last-altered timestamp"
    )
    contract_version: str = Field(..., description="Graph contract version marker")


class DatabricksColumn(Column):
    """Column node emitted by the Databricks connector (core + additive)."""

    model_config = ConfigDict(populate_by_name=True)

    qualified_name: str = Field(
        ..., description="Lossless lowercased dotted path; the readable form of the normalized id"
    )
    catalog: str = Field(..., description="Unity Catalog catalog containing the column")
    schema_: str = Field(..., alias="schema", description="Schema containing the column")
    table: str = Field(..., description="Table containing the column")
    ordinal_position: int | None = Field(default=None, description="1-based column position")
    contract_version: str = Field(..., description="Graph contract version marker")


class DatabricksValue(Value):
    """Value node emitted by the Databricks connector (expanded Value + additive)."""

    model_config = ConfigDict(populate_by_name=True)

    count: int | None = Field(default=None, description="Occurrences of the sampled value")
    catalog: str | None = Field(default=None, description="Catalog of the owning column")
    schema_: str | None = Field(
        default=None, alias="schema", description="Schema of the owning column"
    )
    last_run: datetime | None = Field(
        default=None, description="Run-start stamp for scoped cleanup"
    )
    contract_version: str = Field(..., description="Graph contract version marker")


class DatabricksReferences(References):
    """REFERENCES edge emitted by the Databricks connector (core + provenance).

    ``source_column_id`` / ``target_column_id`` are inherited from the core
    model but are transient join keys, NOT stored edge properties; the
    connector's contract derivation excludes them.
    """

    confidence: float | None = Field(default=None, description="FK confidence score")
    source: str | None = Field(
        default=None, description="FK provenance: 'declared' | 'inferred_metadata'"
    )
