# Metadata Harmonization

From the harvest inputs:
- get updated versions (if hash-id-source is not processed yet)
- deduplicate using identifier-alias
- harmonize metadata to common model

Python mudule using sqlalchemy for database (postgresql) administration

Using pygeometa to parse iso19139:2007, schema.org, dcat
