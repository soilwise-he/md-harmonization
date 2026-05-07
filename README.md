[![DOI](https://zenodo.org/badge/1139675566.svg)](https://doi.org/10.5281/zenodo.19692781)

# SoilWise-he Metadata Harmonization

The Metadata Harmonization Component is responsible for transforming heterogeneous metadata records originating from multiple external systems into a unified relational data model.

The component ingests metadata records in different serialization formats such as XML, JSON, and RDF, normalizes them through a common intermediary representation, and persists the resulting entities into a structured relational schema optimized for querying, deduplication, and downstream data integration.

This process runs at intervals on newly arrived records.

Read full documentation at [docs](./docs/index.md)


## Installation

### Local

A PostGreSQL backend is used, records are imported from `harvest.items` and migrated to a designated table structure in schema `metadata`.

Clone the repository
```
git clone https://github.com/soilwise-he/md-harmonization
cd md-harmonization
```

Rename and Update .env-template (as .env) file with postgres db connection details. In a python virtual environment install requirements:

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

---
## Soilwise-he project
This work has been initiated as part of the [Soilwise-he](https://soilwise-he.eu) project. The project receives
funding from the European Union’s HORIZON Innovation Actions 2022 under grant agreement No.
101112838. Views and opinions expressed are however those of the author(s) only and do not necessarily
reflect those of the European Union or Research Executive Agency. Neither the European Union nor the
granting authority can be held responsible for them.
