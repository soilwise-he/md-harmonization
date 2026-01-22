# Metadata Harmonization

From the harvest inputs:
- get updated versions (if hash-id-source is not processed yet)
- deduplicate using identifier-alias
- harmonize metadata to common model

Python mudule using sqlalchemy for database (postgresql) administration

Using pygeometa to parse iso19139:2007, schema.org, dcat

## Setup

Clone repository
```
git clone https://github.com/soilwise-he/md-harmonization
cd md-harmonization
```

### Using python on localhost

Rename and Update .env-template (as .env) file with postgres db connection details. In a virtual environment:

```
pip install -r requirements.txt
python src/process.py
```

### Using docker

Build the latest image
```
docker build -t soilwise/md-harmonization .
```

Open bash in container
```
docker run -it soilwise/md-harmonization bash
```
Or run process directly
docker run -it soilwise/md-harmonization python src/process.py

