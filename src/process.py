"""
Script: pygeometa_harmonize_to_postgres.py

Purpose:
- Read rows from a Postgres (or SQLite) source table containing a string field with XML or JSON metadata (`txt_result`).
- Use pygeometa to detect schema and harmonize to a common model (MCF-like dict) via `pygeometa.core.import_metadata`.
- Store harmonized metadata and related entities in relational tables under a configurable schema prefix (default: 'metadata.').

Changes in this update:
- The SELECT in `process_all_source_rows()` only retrieves source rows whose MD5 (if present) is not already in the target `records.md5_hash`.
  Rows with NULL md5 will also be retrieved (they will be deduplicated later after computing MD5).
- `insert_record` now inserts all fields of the `records` table.
- Contacts from the MCF are created in the contacts table (if missing) before creating the relationship in contact_metadata.
- All distributions from the MCF are inserted into the `distributions` table and reference the record.

Requirements:
    pip install databases asyncpg python-dotenv pygeometa

Usage:
    export DATABASE_URL=postgresql://user:pass@host:5432/dbname
    export TARGET_SCHEMA=metadata.  # include trailing dot, or set to '' for sqlite
    python pygeometa_harmonize_to_postgres.py

"""

import os
import sys
import json
import hashlib
import logging
import traceback
from typing import Any, Dict, Optional, Tuple
from dateutil.parser import parse
import urllib.parse
import asyncio
import databases
from dotenv import load_dotenv
from pygeometa.core import import_metadata

# pygeometa import deferred inside functions to let module import fail gracefully

# Load environment
load_dotenv()


# Logger configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

DATABASE_URL = os.getenv('DATABASE_URL')
SOURCE_TABLE = os.getenv('SOURCE_TABLE', 'items')
TARGET_SCHEMA = os.getenv('TARGET_SCHEMA', '')  # include trailing dot for schema-qualified names
RECORDS_PER_PAGE = 100

if not DATABASE_URL:
    logger.error('DATABASE_URL not set.')
    sys.exit(1)

# Helper: qualified name builder
SCHEMA_PREFIX = TARGET_SCHEMA or ''

def qn(table: str) -> str:
    return f"{SCHEMA_PREFIX}{table}"

# Database instance
database = databases.Database(DATABASE_URL)

# Utilities

def compute_md5_string(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    return hashlib.md5(text.encode('utf-8')).hexdigest()


# DDL (same tables as before) - kept minimal; assumes JSONB available on PG; on sqlite it will be TEXT
async def create_tables():
    schema_prefix = SCHEMA_PREFIX
    # create schema if needed (only when TARGET_SCHEMA contains a name)
    if TARGET_SCHEMA and TARGET_SCHEMA.endswith('.') and len(TARGET_SCHEMA) > 1:
        schema_name = TARGET_SCHEMA[:-1]
        await database.execute(f'CREATE SCHEMA IF NOT EXISTS {schema_name}')

    is_sqlite = DATABASE_URL.startswith('sqlite')
    if is_sqlite:
        id_def = 'INTEGER PRIMARY KEY AUTOINCREMENT'
    else:
        id_def = 'SERIAL PRIMARY KEY'


    # Build DDL as one string but execute statements individually because asyncpg
    # (and the `databases` library) do not allow multiple SQL commands in a single prepared statement.
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {qn('records')} (
        identifier TEXT PRIMARY KEY,
        md5_hash TEXT UNIQUE,
        language TEXT,
        edition TEXT,
        format TEXT,
        type TEXT,
        RevisionDate TEXT,
        CreationDate TEXT,
        PublicationDate TEXT,
        resolution TEXT,
        AccessConstraints TEXT,
        license TEXT,
        rights TEXT,
        lineage TEXT,
        spatial_coverage TEXT,
        temporal_coverage TEXT,
        title TEXT,
        abstract TEXT,
        raw_mcf TEXT
    );

    CREATE TABLE IF NOT EXISTS {qn('person')} (
        id {id_def},
        name TEXT,
        email TEXT,
        orchid TEXT,
        UNIQUE(id)
    );

    CREATE TABLE IF NOT EXISTS {qn('organization')} (
        id {id_def},
        name TEXT,
        alias TEXT,
        phone TEXT,
        ror TEXT,
        address TEXT,
        postalcode TEXT,
        city TEXT,
        administrativearea TEXT,
        country TEXT,
        url TEXT,
        UNIQUE(id)
    );

    CREATE TABLE IF NOT EXISTS {qn('contact_in_record')} (
        id {id_def},
        fk_organization INTEGER REFERENCES {qn('organization')}(id) ON DELETE CASCADE,
        fk_person INTEGER REFERENCES {qn('person')}(id) ON DELETE CASCADE,
        record_id TEXT REFERENCES {qn('records')}(identifier) ON DELETE CASCADE,
        role TEXT,
        position TEXT,
        UNIQUE(id)
    );    

    CREATE TABLE IF NOT EXISTS {qn('subjects')} (
        id {id_def},
        uri TEXT,
        label TEXT,
        thesaurus_name TEXT,
        thesaurus_url TEXT
    );

    CREATE TABLE IF NOT EXISTS {qn('record_subject')} (
        id {id_def},
        record_id TEXT REFERENCES {qn('records')}(identifier) ON DELETE CASCADE,
        subject_id INTEGER REFERENCES {qn('subjects')}(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS {qn('attributes')} (
        id {id_def},
        record_id TEXT REFERENCES {qn('records')}(identifier) ON DELETE CASCADE,
        name TEXT,
        title TEXT,
        url TEXT,
        units TEXT,
        type TEXT
    );

    CREATE TABLE IF NOT EXISTS {qn('relations')} (
        id {id_def},
        record_id TEXT REFERENCES {qn('records')}(identifier) ON DELETE CASCADE,
        identifier TEXT,
        scheme TEXT,
        type TEXT
    );

    CREATE TABLE IF NOT EXISTS {qn('sources')} (
        id {id_def},
        name TEXT UNIQUE,
        description TEXT
    );

    CREATE TABLE IF NOT EXISTS {qn('record_sources')} (
        id {id_def},
        record_id TEXT REFERENCES {qn('records')}(identifier) ON DELETE CASCADE,
        fk_source INTEGER REFERENCES {qn('sources')}(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS {qn('record_in_project')} (
        id {id_def},
        record_id TEXT UNIQUE REFERENCES {qn('records')}(identifier) ON DELETE CASCADE,
        project TEXT
    );    

    CREATE TABLE IF NOT EXISTS {qn('alternate_identifiers')} (
        id {id_def},
        record_id TEXT REFERENCES {qn('records')}(identifier) ON DELETE CASCADE,
        alt_identifier TEXT
    );

    CREATE TABLE IF NOT EXISTS {qn('distributions')} (
        id {id_def},
        record_id TEXT REFERENCES {qn('records')}(identifier) ON DELETE CASCADE,
        name TEXT,
        format TEXT,
        url TEXT,
        description TEXT 
    );
    """

    # Split into individual statements and execute one by one
    statements = [s.strip() for s in ddl.split(';') if s.strip()]
    for stmt in statements:
        # add semicolon back for clarity (not strictly required)
        try:
            await database.execute(stmt + ';')
        except Exception:
            # Some DB backends (sqlite) may not like certain statements (e.g. JSONB types) or semicolons —
            # try without appending semicolon if it failed.
            try:
                await database.execute(stmt)
            except Exception as e:
                logger.exception('Failed executing DDL statement: %s', e)
    logger.info('Ensured tables (prefix=%s)', SCHEMA_PREFIX)


async def insert_record_and_related(mcf: Dict[str, Any], fk_sourceentifier: str, source_md5: Optional[str], source: str, project: str) -> str:
    # map MCF -> record fields
    identifier = mcf.get('metadata',{}).get('identifier') or fk_sourceentifier
    language = mcf.get('metadata',{}).get('language','')
    edition = mcf.get('identification',{}).get('edition','')
    fmt = mcf.get('identification',{}).get('format','')
    typ = mcf.get('identification',{}).get('type','')
    for tp,dt in (mcf.get('identification',{}).get('dates',{}) or {}).items():
        if tp == 'creation':
            cre = parse_date(dt)
        elif tp == 'modification':
            rev = parse_date(dt)
        elif tp == 'publication':
            pub = parse_date(dt)
    rev = mcf.get('identification',{}).get('RevisionDate','') 
    cre = mcf.get('identification',{}).get('CreationDate','')
    pub = mcf.get('identification',{}).get('PublicationDate','') 
    resolution = mcf.get('identification',{}).get('resolution','')
    access_constraints = intl_str(mcf.get('identification',{}).get('AccessConstraints',''))
    license = intl_str(mcf.get('identification',{}).get('license',''))
    rights = intl_str(mcf.get('identification',{}).get('rights',''))
    lineage = intl_str(mcf.get('identification',{}).get('lineage',''))
    spatial = mcf.get('identification',{}).get('spatial_coverage','') 
    temporal = mcf.get('identification',{}).get('temporal_coverage','') 
    title = intl_str(mcf.get('identification',{}).get('title',''))
    abstract = intl_str(mcf.get('identification',{}).get('abstract',''))

    # deduplicate: prefer identifier, else md5
    if identifier:
        exists = await database.fetch_one(f"SELECT identifier FROM {qn('records')} WHERE identifier = :identifier", values={'identifier': identifier})
        if not exists: # deduplication (see if identifier matches with alt-identifier of other record) # todo: see which is the main identifier, prefer doi
            exists2 = await database.fetch_one(f"SELECT record_id FROM {qn('alternate_identifiers')} WHERE alt_identifier = :identifier", values={'identifier': identifier})
            if exists2:
                exists = exists2
                # add record-source relation, fails if already exists
                await upsert_source(exists['identifier'], source)
        if exists:    
            logger.info('Record with identifier %s exists (%s), returning existing', identifier, exists['identifier'])
            return exists['identifier']

    res = await database.fetch_one(
        f"INSERT INTO {qn('records')} (identifier, md5_hash, language, edition, format, type, RevisionDate, CreationDate, PublicationDate, resolution, AccessConstraints, license, rights, lineage, spatial_coverage, temporal_coverage, title, abstract, raw_mcf) VALUES (:identifier, :md5, :language, :edition, :format, :type, :RevisionDate, :CreationDate, :PublicationDate, :resolution, :AccessConstraints, :license, :rights, :lineage, :spatial_coverage, :temporal_coverage, :title, :abstract, :raw_mcf) RETURNING identifier",
        values={
            'identifier': identifier,
            'md5': source_md5,
            'language': language,
            'edition': edition,
            'format': fmt,
            'type': typ,
            'RevisionDate': rev,
            'CreationDate': cre,
            'PublicationDate': pub,
            'resolution': resolution,
            'AccessConstraints': access_constraints,
            'license': license,
            'rights': rights,
            'lineage': lineage,
            'spatial_coverage': spatial,
            'temporal_coverage': temporal,
            'title': title,
            'abstract': abstract,
            'raw_mcf': json.dumps(mcf, default=str)
        }
    )
    record_id = res['identifier']

    # add new source reference
    await upsert_source(identifier, source)

    # set project
    await upsert_project(record_id, project)

    # alternate identifiers
    alts = []
    if mcf.get('identification').get('dataseturi') not in (None,''):
        alts.append({'alt': mcf.get('identification').get('dataseturi'), 'schema': ''})

    for alt in mcf.get('metadata',{}).get('alternate_identifiers', []) or []:
        alts.append({'alt': alt.get('identifier'), 'schema': alt.get('scheme')})

    for alt in alts:
        row = await database.fetch_one(f"SELECT id FROM {qn('alternate_identifiers')} WHERE alt_identifier = :uri and record_id = :record_id", 
                                       values={'uri': alt.get('identifier'), 'rid': record_id})
        if not row:
            await database.execute(f"INSERT INTO {qn('alternate_identifiers')} (record_id, alt_identifier, scheme) VALUES (:rid, :alt, :scheme)", 
                               values={'rid': record_id, 'alt': alt.get('identifier'), 'schema': alt.get('scheme')})

    # contacts: ensure contact exists then link
    for b,c in (mcf.get('contact', {}) or {}).items():
        orchid = None
        ror = None
        if c.get('url') and c.get('url').startswith('http'):
            if 'orchid.' in c.get('url'):
                orchid = c.get('url')
            elif 'ror.' in c.get('url'):
                ror = c.get('url')
        # email/orchid from the person
        pers_id = await upsert_pers(c,orchid)
        org_id = await upsert_org(c,ror) 

        # contact brings together both person and org
        role = c.get('role') or b

        if pers_id or org_id:
            try:
                await database.execute(f"""
                    INSERT INTO {qn('contact_in_record')} (
                        record_id, fk_organization, fk_person, role, position
                    ) VALUES (
                        :rid, :oid, :pid, :role, :position)""", 
                    values={'rid': record_id, 'oid': org_id, 'pid': pers_id, 'role': role, 'position': c.get('position')})
            except Exception as e:
                logger.info('Failed to create contact link for record %s: %s', record_id, e)
 
    # distributions
    for c,d in (mcf.get('distribution', {}) or {}).items():
        if d.get('url') and d.get('url').startswith('http'):
            fmt = d.get('type') or c
            url = urllib.parse.quote_plus(d.get('url')) 
            name = d.get('name') 
            description = intl_str(d.get('description')) 
            await database.execute(f"INSERT INTO {qn('distributions')} (record_id, format, url, name, description) VALUES (:rid, :fmt, :url, :name, :description)", 
                                   values={'rid': record_id, 'fmt': fmt, 'url': url, 'name': name, 'description': description})

    # attributes
    for a in mcf.get('content_info', {}).get('attributes', []) or []:
        if a.get('name'):
            name = a.get('name') 
            title = a.get('title')
            url = a.get('url') 
            units = a.get('units')
            type = a.get('type') 
            await database.execute(f"INSERT INTO {qn('attributes')} (record_id, name, title, url, units, type) VALUES (:rid, :name, :title, :url, :units, :type)", 
                                   values={'rid': record_id, 'name': name, 'title': title, 'url': url, 'units': units, 'type': type})

    # subjects
    sbjs = []
    for k,v in (mcf.get('identification',{}).get('keywords', {}) or {}).items():
        kws = intl_list(v.get('keywords',{}) or {})
        thes_name = intl_str(v.get('vocabulary',{}).get('name'))
        thes_url = v.get('vocabulary',{}).get('url')
        for kw in kws:
            if kw not in (None,''):
                if kw.startswith('http'):
                    row = await database.fetch_one(f"SELECT id FROM {qn('subjects')} WHERE uri = :uri OR ( AND )", 
                                                values={'uri': kw})
                    if row:
                        subject_id = int(row['id'])
                    else:
                        row2 = await database.fetch_one(f"INSERT INTO {qn('subjects')} (uri) VALUES (:uri ) RETURNING id", 
                                                        values={'uri': kw})
                        subject_id = int(row2['id'])
                else:
                    row = await database.fetch_one(f"SELECT id FROM {qn('subjects')} WHERE label = :label and thesaurus_name = :thes_name", 
                                                values={'label': kw, 'thes_name': thes_name })
                    if row:
                        subject_id = int(row['id'])
                    else:
                        row2 = await database.fetch_one(f"INSERT INTO {qn('subjects')} (label, thesaurus_name, thesaurus_url) VALUES (:label, :thes_name, :thes_url) RETURNING id", 
                                                        values={'label': kw, 'thes_name': thes_name, 'thes_url': thes_url})
                        subject_id = int(row2['id'])
                await database.execute(f"INSERT INTO {qn('record_subject')} (record_id, subject_id) VALUES (:rid, :sid) ON CONFLICT DO NOTHING", 
                                    values={'rid': record_id, 'sid': subject_id})

    # relations
    sources = []
    for rel in mcf.get('metadata',{}).get('relations', []) or []:
        if isinstance(rel, dict) and rel.get('identifier') not in (None,''):
            if rel.get('type','')=='source':
                sources.append(src.get('identifier'))
            else:
                await database.execute(f"INSERT INTO {qn('relations')} (record_id, identifier, scheme, type) VALUES (:rid, :identifier, :scheme, :type) ON CONFLICT DO NOTHING", 
                                       values={'rid': record_id, 'identifier': src.get('identifier'),'scheme': src.get('scheme'), 'type': src.get('type') })    
    # sources
    for src in sources:
        await upsert_source(record_id, src)

    return record_id

async def upsert_source(record_id,src):
    if src not in (None,''):
        rws = await database.fetch_one(f"SELECT id FROM {qn('sources')} WHERE lower(name) = :name", values={'name': src.lower()})
        if rws:
            fk_source = int(rws['id'])
        else:
            r2 = await database.fetch_one(f"INSERT INTO {qn('sources')} (name) VALUES (:name) RETURNING id", values={'name': src.lower()})
            fk_source = int(r2['id'])
        await database.execute(f"INSERT INTO {qn('record_sources')} (record_id, fk_source) VALUES (:rid, :sid) ON CONFLICT DO NOTHING",
                            values={'rid': record_id, 'sid': fk_source})

async def upsert_project(record_id, prj):
    if prj not in (None,''):
        await database.execute(f"INSERT INTO {qn('record_in_project')} (record_id, project) VALUES (:rid, :prj) ON CONFLICT DO NOTHING",
                            values={'rid': record_id, 'prj': prj})

async def upsert_pers(c,orchid=None):
    if orchid:
        row = await database.fetch_one(f"SELECT id FROM {qn('person')} WHERE orchid = :orchid", values={'orchid': orchid})
        if row:
            return int(row['id'])
    if c.get('individualname'):
        row = await database.fetch_one(f"SELECT id FROM {qn('person')} WHERE lower(name) = :name or email = :email", 
                                       values={'name': c.get('individualname').lower(), 'email': c.get('email')})
        if row:
            return int(row['id'])
    # insert new person
    res = await database.fetch_one(
        f"INSERT INTO {qn('person')} (name, email, orchid) VALUES (:name, :email, :orchid) RETURNING id",
        values={
            'name': c.get('individualname'),
            'email': c.get('email'),
            'orchid': orchid
        }
    )    
    return int(res['id'])
        
async def upsert_org(c,ror=None):
    # see if organization already exists
    if ror:
        row = await database.fetch_one(f"SELECT id FROM {qn('organization')} WHERE ror = :ror", values={'ror': ror})
        if row:
            return int(row['id'])
    if c.get('organization'):
        row = await database.fetch_one(f"SELECT id FROM {qn('organization')} WHERE lower(name) = :name or lower(alias) = :name or url = :url", values={'name': c.get('organization').lower(), 'url': c.get('url')})
        if row:
            return int(row['id'])
    # insert new org
    res = await database.fetch_one(
        f"INSERT INTO {qn('organization')} (name, phone, ror, address, postalcode, city, administrativearea, country, url) VALUES (:name, :phone, :ror, :address,  :postalcode, :city, :administrativearea, :country, :url) RETURNING id",
        values={
            'name': c.get('organization'),
            'phone': c.get('phone'),
            'ror': ror,
            'address': c.get('address'),
            'postalcode': c.get('postalcode'),
            'city': c.get('city'),
            'administrativearea': c.get('administrativearea'),
            'country': c.get('country'),
            'url': c.get('url')
        }
    )    
    return int(res['id'])

def intl_str(val, lang='en'):
    if isinstance(val, str):
        return val
    elif isinstance(val, list):
        if len(val) > 0:
            return intl_str(val[0])
        else:
            return None
    elif isinstance(val, dict):
        # get most relevant translated key, if not empty
        for u,v in val.items():
            if u in ('en','eng','en-uk','en-us') and v not in (None,''):
                return v
        for u,v in val.items():
            if u == lang and v not in (None,''):
                return v
        for u,v in val.items():
            if v not in (None,''):
                return v
    return None

def intl_list(val, lang='en'):
    if isinstance(val, str):
        return [val]
    elif isinstance(val, list):
        return val
    elif isinstance(val, dict):
        # get most relevant translated key, if not empty
        for u,v in val.items():
            if u in ('en','eng','en-uk','en-us') and v not in (None,''):
                return v
        for u,v in val.items():
            if u == lang and v not in (None,''):
                return v
        for u,v in val.items():
            if v not in (None,''):
                return v
    return []


def parse_date(ds):
    try:
        return parse(ds, fuzzy=True)
    except:
        return None


async def process_all_source_rows():
    # Select only source rows whose MD5 (if present) is not already in records

    query = f"""
        SELECT identifier, resultobject, hash, source, project FROM {SOURCE_TABLE} s 
        WHERE resulttype='iso19139:2007' and (NOT EXISTS (
            SELECT 1 FROM {qn('records')} r WHERE r.md5_hash = s.hash)) LIMIT {RECORDS_PER_PAGE}"""

    rows = await database.fetch_all(query)
    logger.info('Selected %d source rows to process', len(rows))

    for r in rows:
        try:
            fk_source = str(r['identifier'])
            txt = r['resultobject']
            source_md5 = r['hash']
            source = str(r['source'])
            project = str(r['project'])

            # parse using pygeometa
            try:
                mcf = import_metadata('autodetect', txt)
                if not isinstance(mcf, dict) or 'identification' not in mcf.keys():
                    raise KeyError(f'Empty document')
            except Exception as e:
                logger.info(f'pygeometa failed for  {fk_source} in {source}: {e}: {traceback.format_exc()}')
                continue

            # insert record and related entities
            record_id = await insert_record_and_related(mcf, fk_source, source_md5, source, project)
            logger.info('Inserted/ensured record id %s for source %s', record_id, source)

        except Exception as e:
            logger.info('Failed processing source row %s for source %s: %s: %s', fk_source, source, e, traceback.format_exc())


async def main():
    await database.connect()
    await create_tables()
    await process_all_source_rows()
    await database.disconnect()


if __name__ == '__main__':
    asyncio.run(main())
