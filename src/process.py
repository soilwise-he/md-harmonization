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
from typing import Any, Dict, Optional, Tuple
import asyncio
import databases
from dotenv import load_dotenv

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
SOURCE_TABLE = os.getenv('SOURCE_TABLE', 'source_metadata')
TXT_FIELD = os.getenv('TXT_FIELD', 'txt_result')
ID_FIELD = os.getenv('ID_FIELD', 'id')
MD5_FIELD = os.getenv('MD5_FIELD', 'content_md5')
TARGET_SCHEMA = os.getenv('TARGET_SCHEMA', 'metadata.')  # include trailing dot for schema-qualified names

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

    # Use JSONB for raw_mcf/details when supported; allow failures on sqlite which will ignore JSONB
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {qn('records')} (
        id SERIAL PRIMARY KEY,
        identifier TEXT UNIQUE,
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

    CREATE TABLE IF NOT EXISTS {qn('contacts')} (
        id SERIAL PRIMARY KEY,
        name TEXT,
        email TEXT,
        phone TEXT,
        position TEXT,
        organization TEXT,
        address TEXT,
        town TEXT,
        country TEXT,
        url TEXT,
        ror TEXT UNIQUE,
        orchid TEXT UNIQUE
    );

    CREATE TABLE IF NOT EXISTS {qn('contact_metadata')} (
        id SERIAL PRIMARY KEY,
        contact_id INTEGER REFERENCES {qn('contacts')}(id) ON DELETE CASCADE,
        record_id INTEGER REFERENCES {qn('records')}(id) ON DELETE CASCADE,
        role TEXT,
        UNIQUE(contact_id, record_id, role)
    );

    CREATE TABLE IF NOT EXISTS {qn('subjects')} (
        id SERIAL PRIMARY KEY,
        uri TEXT UNIQUE,
        label TEXT
    );

    CREATE TABLE IF NOT EXISTS {qn('record_subject')} (
        id SERIAL PRIMARY KEY,
        record_id INTEGER REFERENCES {qn('records')}(id) ON DELETE CASCADE,
        subject_id INTEGER REFERENCES {qn('subjects')}(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS {qn('attributes')} (
        id SERIAL PRIMARY KEY,
        record_id INTEGER REFERENCES {qn('records')}(id) ON DELETE CASCADE,
        name TEXT,
        uri TEXT,
        unit TEXT
    );

    CREATE TABLE IF NOT EXISTS {qn('sources')} (
        id SERIAL PRIMARY KEY,
        name TEXT UNIQUE,
        description TEXT
    );

    CREATE TABLE IF NOT EXISTS {qn('record_sources')} (
        id SERIAL PRIMARY KEY,
        record_id INTEGER REFERENCES {qn('records')}(id) ON DELETE CASCADE,
        source_id INTEGER REFERENCES {qn('sources')}(id) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS {qn('alternate_identifiers')} (
        id SERIAL PRIMARY KEY,
        record_id INTEGER REFERENCES {qn('records')}(id) ON DELETE CASCADE,
        alt_identifier TEXT
    );

    CREATE TABLE IF NOT EXISTS {qn('distributions')} (
        id SERIAL PRIMARY KEY,
        record_id INTEGER REFERENCES {qn('records')}(id) ON DELETE CASCADE,
        format TEXT,
        url TEXT,
        details TEXT
    );
    """
    await database.execute(ddl)
    logger.info('Ensured tables (prefix=%s)', SCHEMA_PREFIX)


# Contact helper: insert if missing (prefer ror/orchid then name)
async def get_or_create_contact(contact_obj: Any) -> int:
    name = None
    email = None
    phone = None
    position = None
    organization = None
    ror = None
    orchid = None
    address = None
    town = None
    country = None
    url = None

    if isinstance(contact_obj, str):
        name = contact_obj
    elif isinstance(contact_obj, dict):
        name = contact_obj.get('organisationName') or contact_obj.get('name') or contact_obj.get('org')
        email = contact_obj.get('email') or contact_obj.get('electronicMailAddress')
        phone = contact_obj.get('phone') or contact_obj.get('telephone')
        position = contact_obj.get('positionName') or contact_obj.get('position')
        organization = contact_obj.get('organisationName') or contact_obj.get('organization')
        ror = contact_obj.get('ror')
        orchid = contact_obj.get('orchid') or contact_obj.get('orcid')
        address = contact_obj.get('deliveryPoint') or contact_obj.get('address')
        town = contact_obj.get('city') or contact_obj.get('town')
        country = contact_obj.get('country')
        url = contact_obj.get('onlineResource') or contact_obj.get('url')

    # prefer unique id lookups
    if ror:
        row = await database.fetch_one(f"SELECT id FROM {qn('contacts')} WHERE ror = :ror", values={'ror': ror})
        if row:
            return int(row['id'])
    if orchid:
        row = await database.fetch_one(f"SELECT id FROM {qn('contacts')} WHERE orchid = :orchid", values={'orchid': orchid})
        if row:
            return int(row['id'])
    if name:
        row = await database.fetch_one(f"SELECT id, email, phone, position, organization, address, town, country, url, ror, orchid FROM {qn('contacts')} WHERE name = :name", values={'name': name})
        if row:
            updates = {}
            if ror and not row.get('ror'):
                updates['ror'] = ror
            if orchid and not row.get('orchid'):
                updates['orchid'] = orchid
            if email and not row['email']:
                updates['email'] = email
            if phone and not row['phone']:
                updates['phone'] = phone
            if position and not row['position']:
                updates['position'] = position
            if organization and not row['organization']:
                updates['organization'] = organization
            if address and not row['address']:
                updates['address'] = address
            if town and not row['town']:
                updates['town'] = town
            if country and not row['country']:
                updates['country'] = country
            if url and not row['url']:
                updates['url'] = url
            if updates:
                set_clause = ', '.join([f"{k} = :{k}" for k in updates.keys()])
                values = updates.copy()
                values['id'] = row['id']
                await database.execute(f"UPDATE {qn('contacts')} SET " + set_clause + " WHERE id = :id", values=values)
            return int(row['id'])

    # insert new contact
    res = await database.fetch_one(
        f"INSERT INTO {qn('contacts')} (name, email, phone, position, organization, ror, orchid, address, town, country, url) VALUES (:name, :email, :phone, :position, :organization, :ror, :orchid, :address, :town, :country, :url) RETURNING id",
        values={
            'name': name,
            'email': email,
            'phone': phone,
            'position': position,
            'organization': organization,
            'ror': ror,
            'orchid': orchid,
            'address': address,
            'town': town,
            'country': country,
            'url': url,
        }
    )
    return int(res['id'])


async def insert_record_and_related(mcf: Dict[str, Any], source_identifier: str, source_md5: Optional[str]) -> int:
    # map MCF -> record fields
    identifier = mcf.get('identifier') or mcf.get('id') or source_identifier
    language = mcf.get('language')
    edition = mcf.get('edition')
    fmt = mcf.get('format')
    typ = mcf.get('type')
    rev = mcf.get('RevisionDate') or mcf.get('revisionDate')
    cre = mcf.get('CreationDate') or mcf.get('creationDate')
    pub = mcf.get('PublicationDate') or mcf.get('publicationDate')
    resolution = mcf.get('resolution')
    access_constraints = mcf.get('AccessConstraints') or mcf.get('accessConstraints')
    license = mcf.get('license')
    rights = mcf.get('rights')
    lineage = mcf.get('lineage')
    spatial = mcf.get('spatial_coverage') or mcf.get('spatial')
    temporal = mcf.get('temporal_coverage') or mcf.get('temporal')
    title = mcf.get('title')
    abstract = mcf.get('abstract')

    # deduplicate: prefer identifier, else md5
    if identifier:
        exists = await database.fetch_one(f"SELECT id FROM {qn('records')} WHERE identifier = :identifier", values={'identifier': identifier})
        if exists:
            logger.info('Record with identifier %s exists (id=%s), returning existing', identifier, exists['id'])
            return int(exists['id'])
    if source_md5:
        exists = await database.fetch_one(f"SELECT id FROM {qn('records')} WHERE md5_hash = :md5", values={'md5': source_md5})
        if exists:
            logger.info('Record with md5 %s exists (id=%s), returning existing', source_md5, exists['id'])
            return int(exists['id'])

    res = await database.fetch_one(
        f"INSERT INTO {qn('records')} (identifier, md5_hash, language, edition, format, type, RevisionDate, CreationDate, PublicationDate, resolution, AccessConstraints, license, rights, lineage, spatial_coverage, temporal_coverage, title, abstract, raw_mcf) VALUES (:identifier, :md5, :language, :edition, :format, :type, :RevisionDate, :CreationDate, :PublicationDate, :resolution, :AccessConstraints, :license, :rights, :lineage, :spatial_coverage, :temporal_coverage, :title, :abstract, :raw_mcf) RETURNING id",
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
    record_id = int(res['id'])

    # alternate identifiers
    for alt in mcf.get('alternate_identifiers', []) or []:
        await database.execute(f"INSERT INTO {qn('alternate_identifiers')} (record_id, alt_identifier) VALUES (:rid, :alt)", values={'rid': record_id, 'alt': alt})

    # contacts: ensure contact exists then link
    for c in mcf.get('contacts', []) or []:
        role = None
        contact_obj = c
        if isinstance(c, dict):
            role = c.get('role') or c.get('contactRole')
            if 'contact' in c and isinstance(c['contact'], (dict, str)):
                contact_obj = c['contact']
        try:
            contact_id = await get_or_create_contact(contact_obj)
            await database.execute(f"INSERT INTO {qn('contact_metadata')} (contact_id, record_id, role) VALUES (:cid, :rid, :role) ON CONFLICT (contact_id, record_id, role) DO NOTHING", values={'cid': contact_id, 'rid': record_id, 'role': role})
        except Exception:
            logger.exception('Failed to create contact link for record %s', record_id)

    # distributions
    for d in mcf.get('distributions', []) or []:
        fmt_d = d.get('format') if isinstance(d, dict) else None
        url = d.get('url') if isinstance(d, dict) else None
        details = json.dumps(d, default=str) if isinstance(d, (dict, list)) else json.dumps({'value': d})
        await database.execute(f"INSERT INTO {qn('distributions')} (record_id, format, url, details) VALUES (:rid, :fmt, :url, :details)", values={'rid': record_id, 'fmt': fmt_d, 'url': url, 'details': details})

    # attributes
    for a in mcf.get('attributes', []) or []:
        name = a.get('name') if isinstance(a, dict) else None
        uri = a.get('uri') if isinstance(a, dict) else None
        unit = a.get('unit') if isinstance(a, dict) else None
        await database.execute(f"INSERT INTO {qn('attributes')} (record_id, name, uri, unit) VALUES (:rid, :name, :uri, :unit)", values={'rid': record_id, 'name': name, 'uri': uri, 'unit': unit})

    # subjects
    for s in mcf.get('subjects', []) or []:
        if isinstance(s, str):
            subj = {'label': s}
        elif isinstance(s, dict):
            subj = s
        else:
            continue
        # upsert subject by uri or label
        uri = subj.get('uri')
        label = subj.get('label') or subj.get('term')
        if uri:
            row = await database.fetch_one(f"SELECT id FROM {qn('subjects')} WHERE uri = :uri", values={'uri': uri})
            if row:
                subject_id = int(row['id'])
            else:
                row2 = await database.fetch_one(f"INSERT INTO {qn('subjects')} (uri, label) VALUES (:uri, :label) RETURNING id", values={'uri': uri, 'label': label})
                subject_id = int(row2['id'])
        else:
            row = await database.fetch_one(f"SELECT id FROM {qn('subjects')} WHERE label = :label", values={'label': label})
            if row:
                subject_id = int(row['id'])
            else:
                row2 = await database.fetch_one(f"INSERT INTO {qn('subjects')} (label) VALUES (:label) RETURNING id", values={'label': label})
                subject_id = int(row2['id'])
        await database.execute(f"INSERT INTO {qn('record_subject')} (record_id, subject_id) VALUES (:rid, :sid) ON CONFLICT DO NOTHING", values={'rid': record_id, 'sid': subject_id})

    # sources
    for src in mcf.get('sources', []) or []:
        if isinstance(src, str):
            name = src
            desc = None
        elif isinstance(src, dict):
            name = src.get('name') or src.get('source')
            desc = src.get('description')
        else:
            continue
        row = await database.fetch_one(f"SELECT id FROM {qn('sources')} WHERE name = :name", values={'name': name})
        if row:
            source_id = int(row['id'])
        else:
            r2 = await database.fetch_one(f"INSERT INTO {qn('sources')} (name, description) VALUES (:name, :desc) RETURNING id", values={'name': name, 'desc': desc})
            source_id = int(r2['id'])
        await database.execute(f"INSERT INTO {qn('record_sources')} (record_id, source_id) VALUES (:rid, :sid) ON CONFLICT DO NOTHING", values={'rid': record_id, 'sid': source_id})

    return record_id


async def process_all_source_rows():
    # Select only source rows whose MD5 (if present) is not already in records
    # Rows with NULL MD5 are retrieved as well (we'll compute MD5 later and deduplicate)
    if MD5_FIELD:
        query = f"SELECT {ID_FIELD} AS id, {TXT_FIELD} AS txt, {MD5_FIELD} AS md5 FROM {SOURCE_TABLE} s WHERE (s.{MD5_FIELD} IS NULL) OR (NOT EXISTS (SELECT 1 FROM {qn('records')} r WHERE r.md5_hash = s.{MD5_FIELD}))"
    else:
        query = f"SELECT {ID_FIELD} AS id, {TXT_FIELD} AS txt, NULL AS md5 FROM {SOURCE_TABLE}"

    rows = await database.fetch_all(query)
    logger.info('Selected %d source rows to process', len(rows))

    for r in rows:
        try:
            source_id = str(r['id'])
            txt = r['txt']
            source_md5 = r['md5'] or compute_md5_string(txt)

            # parse using pygeometa
            try:
                from pygeometa.core import import_metadata
                mcf = import_metadata('autodetect', txt)
            except Exception as e:
                logger.exception('pygeometa failed for source %s: %s', source_id, e)
                continue

            # insert record and related entities
            record_id = await insert_record_and_related(mcf, source_id, source_md5)
            logger.info('Inserted/ensured record id=%s for source %s', record_id, source_id)

        except Exception:
            logger.exception('Failed processing source row %s', r)


async def main():
    await database.connect()
    await create_tables()
    await process_all_source_rows()
    await database.disconnect()


if __name__ == '__main__':
    asyncio.run(main())
