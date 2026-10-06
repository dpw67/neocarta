# Foreign-key inference

Infer potential joins from column names, compatible types, declared primary keys,
and descriptions. This enrichment runs after schema ingestion and works with both
the Databricks warehouse and Spark connectors, plus other connectors that produce
the Database → Schema → Table → Column hierarchy.

```python
from neocarta.enrichment.foreign_keys import infer_foreign_keys

result = infer_foreign_keys(
    neo4j_driver,
    database_name="neo4j",
    catalogs=["sales"],
    schemas=["public"],
    threshold=0.8,
)
print(result.edges_written)
```

The graph queries require Neo4j with Cypher 25 support. Catalog and schema filters
match node names. Candidates stay within each schema. The enrichment reads the
graph hierarchy, so columns need no connector-specific catalog/schema properties.

Accepted edges carry `source="inferred_metadata"` and a heuristic `confidence`
score. These scores express rule strength rather than calibrated probabilities.
Existing declared relationships are preserved, including their properties, and
inferred edges never change a column's declared `is_foreign_key` flag. Repeated
runs can update scores on inferred edges; they do not remove old inferred edges.

Inference uses metadata only. It does not verify referential integrity against
table data. Consumers can filter inferred edges by provenance and score.
