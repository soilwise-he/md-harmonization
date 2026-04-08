# SoilWise-he Metadata Harmonization

Metadata from various source platforms arrives in various data models (iso19115, DCAT, Datacite) and serialisations (xml, json, ttl).
This module harmonizes the metadata to a common relational database model. From where it is further processed.

This process runs at intervals on newly arrived records.


## Features

- get updated versions (if hash-id-source is not processed yet)
- deduplicate using identifier-alias
- harmonize metadata to common model


## Installation

### Local

The same database as the harvester component is used, as described in [db-migrate]().

Clone the repository
```
git clone https://github.com/soilwise-he/md-harmonization
cd md-harmonization
```

Rename and Update .env-template (as .env) file with postgres db connection details. In a virtual environment:
```
pip install -r requirements.txt
```

## Usage

### Locally

```
python src/process.py
```

### Using docker

Run the process via a container. Set database connection details in the src/.env file.

```
docker run -it --env-file src/.env ghcr.io/soilwise-he/md-harmonization:latest python src/process.py
```


## Additional information

Python mudule using sqlalchemy for database (postgresql) administration

Based on [pygeometa](https://github.com/geopython/pygeometa) to parse iso19139, schema.org, datacite, dcat


---
## Soilwise-he project
This work has been initiated as part of the [Soilwise-he](https://soilwise-he.eu) project. The project receives
funding from the European Union’s HORIZON Innovation Actions 2022 under grant agreement No.
101112838. Views and opinions expressed are however those of the author(s) only and do not necessarily
reflect those of the European Union or Research Executive Agency. Neither the European Union nor the
granting authority can be held responsible for them.
