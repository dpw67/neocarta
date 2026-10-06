"""Databricks (managed Unity Catalog) connectors.

Provides two sub-connectors over managed Databricks Unity Catalog:

- :class:`DatabricksSchemaConnector` — structural schema metadata
  (:Database/:Schema/:Table/:Column/:Value and their HAS_*/REFERENCES edges) read
  from a catalog's ``<catalog>.information_schema.*`` views over a Databricks SQL
  warehouse, using the in-process ``databricks-sql-connector`` (DB-API). No Spark
  or JDBC runtime is required — the lightweight, BigQuery-style path.
- :class:`DatabricksTagsConnector` — governed-tag *definitions* mapped into the
  vendor-neutral governance-tag data model (:GovernanceTagKey / :GovernanceTagValue)
  via the Databricks SDK (no SQL warehouse).

The optional Spark schema connector uses the Neo4j Spark Connector and supports
inline embeddings. Install ``neocarta[databricks-spark]`` to run it. Importing
this package does not import Spark.
"""

from ...warnings import DatabricksTagsWarning
from .schema import DatabricksSchemaConnector
from .spark import DatabricksSparkSchemaConnector
from .tags import DatabricksTagsConnector

__all__ = [
    "DatabricksSchemaConnector",
    "DatabricksSparkSchemaConnector",
    "DatabricksTagsConnector",
    "DatabricksTagsWarning",
]
