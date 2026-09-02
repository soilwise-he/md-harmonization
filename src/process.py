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

from genericpath import exists
import os, sys, yaml, json, copy, re
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
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

logging.getLogger("pygeometa").setLevel(logging.DEBUG)

host=os.environ.get("POSTGRES_HOST",'')
port=os.environ.get("POSTGRES_PORT",'')
dbname=os.environ.get("POSTGRES_DB",'')
user=os.environ.get("POSTGRES_USER",'')
password=os.environ.get("POSTGRES_PASSWORD",'')
DATABASE_URL = os.getenv('DATABASE_URL') or f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
SCHEMA_PREFIX = os.getenv('SCHEMA', 'metadata')  # include trailing dot for schema-qualified names

if not DATABASE_URL:
    logger.error('DATABASE_URL not set.')
    sys.exit(1)

def qn(table: str) -> str:
    return f"{SCHEMA_PREFIX}{'.' if SCHEMA_PREFIX else ''}{table}"

# Database instance
database = databases.Database(DATABASE_URL)

# Utilities
def compute_md5_string(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    return hashlib.md5(text.encode('utf-8')).hexdigest()

DOI_PATTERN = re.compile(r"^10\.\d{4,9}/[-._;()/:A-Z0-9]+$", re.I)

def is_doi(value: str) -> bool:
    return bool(DOI_PATTERN.fullmatch(value.strip()))

# from https://gist.github.com/angstwad/bf22d1822c38a92ec0a9
def dict_merge(dct, merge_dct):
    """
    Recursive dict merge. Inspired by :meth:``dict.update()``, instead of
    updating only top-level keys, __dict_merge recurses down into dicts
    nested to an arbitrary depth, updating keys. The ``merge_dct`` is
    merged into ``dct``.
    :param dct: dict onto which the merge is executed
    :param merge_dct: dct merged into dct
    :returns: None
    """
    if merge_dct and not isinstance(merge_dct, str):
        for k, v in merge_dct.items():
            try:
                if (k in dct and isinstance(dct[k], dict)):
                    dict_merge(dct[k], merge_dct[k])
                else:
                    if k in dct and dct[k] and not v:
                        pass
                    else:
                        dct[k] = merge_dct[k]
            except Exception as e:
                print(e,"; k:",k,"; v:",v)


async def process_workflow(record_id, mcf, source, hash, mode):
    # check if identifier in source already processed? -> update else insert 
    exists = await database.fetch_all(f"""
            SELECT identifier FROM {qn('records_processed')} 
            WHERE identifier = :identifier and source = :source""", 
            values={'identifier': record_id,
                    'source': source })
    mode = 'insert'
    if not exists:
        mode = 'update' 
    # now enter process step
    if mode == 'insert':
        await database.fetch_one(
            f"""INSERT INTO {qn('records_processed')} (
                identifier, hash, source, mode, project, mcf
            ) VALUES (
                :id, :hash, :source, :mode, :project, :mcf)""",
            values={
                'id': record_id,
                'hash': hash,
                'source': source,
                'mode': mode,
                'mcf': json.dumps(mcf, default=str)
            })
    else:
        await database.fetch_one(
            f"""UPDATE {qn('records_processed')} SET 
                hash = :hash, mode = :mode, mcf = :mcf
                WHERE identifier = :id AND source = :source""",
            values={
                'id': record_id,
                'hash': hash,
                'source': source,
                'mode': mode,
                'mcf': json.dumps(mcf, default=str)
            })
        

def mcf_sanity_check(identifier, mcf):
    # some checks on any incoming mcf
    if 'doi.org/' in identifier:
        identifier = identifier.split('doi.org/').pop().split(' ')[0]

    ## if identifier is not a doi, and alt-identifiers has a doi, replace identifier
    if not is_doi(identifier):
        for i in (mcf['metadata'].get('additional_identifiers' or [])):
            i2 = i.get('identifier','').split('doi.org/').pop().split(' ')[0]
            if is_doi(i2):
                mcf['metadata']['identifier'] = i2
                identifier = i2
                i['identifier'] = identifier
                break

    ## if identifier is a doi, make sure it is in distributions
    if is_doi(identifier):
        hasDoi = False
        for k,v in mcf.get('distribution',{}).items():
            if v.get('url','').split('doi.org/').pop() == identifier:
                hasDoi = True
                break
        if not hasDoi:

            mcf.setdefault("distribution", {})[identifier] = {
                'url' : 'https://doi.org/'+identifier,
                'type': 'WWW:LINK',
                'name': mcf.get('identification',{}).get('title','doi'), 
                'function': 'download'
            }
    
async def mcf_deduplicate(identifier, mcf):
    # deduplication of records

    all_identifiers = [identifier]
    # add alt ids
    for id in mcf.get('metadata',{}).get('additional_identifiers',[]):
        if id.get('identifier'):
            all_identifiers.append(id.get('identifier')) 
    ids_as_str = ",".join([f"'{i}'" for i in all_identifiers])
    exists = await database.fetch_all(f"""
                SELECT identifier, mcf, source 
                FROM {qn('records_processed')}
                WHERE identifier in (
                    select record_id from metadata.alternate_identifiers 
                    where record_id in ({ids_as_str})
                    or alt_identifier in ({ids_as_str}) 
                )""", 
                values={})

    final_mcf = {}
    # prefer openaire as a source
    for r in exists:
        if r['source'] == 'openaire':
            final_mcf = json.loads(r['mcf'])
            # use this as main identifier
            identifier = r['identifier']
            # a second arrival here is not expected, but also not problematic

    for r in exists:
        if r['source'] != 'openaire':
            t_mcf = json.loads(r['mcf'])
            if 'identification' in t_mcf:
                dict_merge(final_mcf,  t_mcf)

    return final_mcf

async def insert_record_and_related(mcf_in: Dict[str, Any], record_id: str, md5_hash: Optional[str], source: str, project: str, harvest_date = None, modus='insert') -> str:

    # record (version) already exists? -> insert or update or skip
    # if md5 is the same, skip; if md5 is different, update record with new metadata and md5
    # todo: problematic if metadata content for this id is updated by secondary processes (e.g. augmentations) 
    # after initial insert, as this will cause the md5 to differ and the record to be updated again on next harvest, 
    # even if the original source metadata has not changed. Possible solution: separate md5 for source metadata vs 
    # md5 for stored metadata (after augmentation).


    # sanity checks on mcf
    await process_workflow(record_id=record_id, mcf=mcf_in, source=source, hash=md5_hash, mode=modus)
    # deduplication
    mcf = await mcf_deduplicate(identifier=record_id, mcf=mcf_in)

    # process mcf
    mmd_= mcf.get('metadata',{})
    mid_= mcf.get('identification',{})
    mci_= mcf.get('content_info',{})
    md_date = parse_date((mmd_.get('dates') or {}).get('revision')) or parse_date((mmd_.get('dates') or {}).get('creation')) or parse_date((mmd_.get('dates') or {}).get('publication'))
    md_lang = intl_str(mmd_.get('language',''))
    language = intl_str(mid_.get('language',mmd_.get('language','')))
    edition = mid_.get('edition','')
    fmt = mid_.get('format','')
    typ = mmd_.get('hierarchylevel','')
    dts = {"creation": None,
           "publication": None,
           "embargoend": None,
           "modification": None}
    for tp,dt in mid_.get('dates',{}).items():
        dts[tp] = parse_date(dt) 
 
    denominator=''
    resolution=''
    if 'denominators' in mci_ and isinstance(mci_['denominators'],list):
        denominator = ','.join(mci_.get('denominators'))
    if 'resolution' in mci_ and isinstance(mci_['resolution'],list):
        resolution = ','.join([f.get('distance') for f in mci_.get('resolution') if 'distance' in f and f['distance'] not in [None,'']])

    accessconstraints = intl_str(mid_.get('accessconstraints',''))
    license = ''
    lic_ = mid_.get('license')
    if lic_:
        license = lic_.get('url')
        if license in [None,'']:
            license = intl_str(lic_.get('name',))

    rights = intl_str(mid_.get('rights',''))
    lineage = intl_str(mcf.get('dataquality',{}).get('lineage',''))
    thumbnail = mcf.get('identification',{}).get('browsegraphic','')
    first_spatial_extent = next(iter(mid_.get('extents',{}).get('spatial',[])), {})
    spatial = str(first_spatial_extent.get('bbox',''))
    spatial_desc = first_spatial_extent.get('description','') 
    first_temporal_extent = next(iter(mid_.get('extents',{}).get('temporal',[])), {})
    temporal_start = parse_date(first_temporal_extent.get('begin',None))
    temporal_end = parse_date(first_temporal_extent.get('end',None))
    title = intl_str(mid_.get('title',''))
    abstract = intl_str(mid_.get('abstract',''))


    # add record to source
    await upsert_source(record_id, source)
    dbvals = {
                'identifier': record_id,
                'md_lang': md_lang,
                'md_date': md_date,
                'harvest_date': harvest_date,
                'title': title,
                'abstract': abstract,
                'language': language,
                'edition': edition,
                'format': fmt,
                'type': typ,
                'thumbnail': thumbnail,
                'revisiondate': dts['modification'],
                'creationdate': dts['creation'],
                'publicationdate': dts['publication'],
                'embargodate': dts['embargoend'],
                'resolution': str(resolution),
                'denominator': str(denominator),
                'accessconstraints': accessconstraints,
                'license': license,
                'rights': rights,
                'lineage': lineage,
                'spatial': spatial,
                'spatial_desc': spatial_desc,
                'temporal_start': temporal_start,
                'temporal_end': temporal_end,
                'md5_hash': md5_hash,
                'raw_mcf': json.dumps(mcf, default=str)
            }

    # if record (from this source) already exists, update it
    qry = f"""INSERT INTO {qn('records')} (
                {', '.join(dbvals.keys())}
            ) VALUES (
                {', '.join([f":{f}" for f in dbvals.keys()])}
            ) on conflict (identifier) do update set
                {', '.join([f"{f}=:{f}" for f in dbvals.keys()])}"""
    res = await database.execute(qry,values=dbvals)

    # set project # todo: what happens if the project has changed on a record?

    # project taken from record, project reference is stored in mcf.relations@project
    if 'relations' in mcf.get('metadata',{}) and isinstance(mcf['metadata']['relations'],list):
        for rel in mcf['metadata']['relations']:
            if rel.get('type','').lower() == 'project' and rel.get('identifier') not in (None,''):
                await upsert_project(record_id, rel.get('identifier'))
    elif project not in (None,'','None'):
        await upsert_project(record_id, project)

    # process alt identifiers
    a_ids = mcf.get('metadata',{}).get('additional_identifiers',[])
    for k in ['identifier','dataseturi']:
        idt = mcf.get('metadata',{}).get(k)
        if isinstance(idt, list):
            idt = copy.deepcopy(idt)[0]
        if idt not in [None,''] and idt != record_id:
            a_ids.append({
                'scheme': ('uri' if idt.startswith('http') else 'uuid'),
                'identifier': idt })
    for tid in a_ids:
        if tid.get('identifier') not in [None,''] and tid.get('identifier') != record_id and not tid.get('identifier').startswith('50|'):
            exists2 = await database.fetch_one(f"""
                SELECT record_id FROM {qn('alternate_identifiers')} WHERE (
                alt_identifier = :identifier and record_id = :fk_identifier ) OR (
                alt_identifier = :fk_identifier and record_id = :identifier)""", 
                values={'identifier': record_id, 'fk_identifier': tid['identifier'] })
            if not exists2:
                vals = {
                        'identifier': record_id, 
                        'fk_identifier': tid['identifier'], 
                        'scheme': tid.get('scheme', '') }
                qry = f"insert into {qn('alternate_identifiers')} (record_id, alt_identifier, scheme) values (:identifier, :fk_identifier, :scheme )"
                await database.execute(qry, values=vals)
    
        # append to distributions if the identifier is a URL and not already present
        if (tid.get('scheme') or '') == 'doi':
            found = False
            for d in mcf.get('distribution', {}).values():
                if tid.get('identifier') in d.get('url'):
                    break
            if not found:
                mcf.setdefault('distribution', {})[f"dist_{tid.get('identifier')}"] = {
                    'url': tid.get('identifier'),
                    'type': 'link',
                    'name': intl_str(mcf.get('identification').get('title', '')) }
                
    # collect pers-orgs for pers-org matching
    orgs_=[]
    pers_=[]
    skipMatchedOrgs=[]
    skipMatchedPers=[]
    for c in (mcf.get('contact', {}) or {}).values():
        if 'organization' in c and c['organization'] not in [None,'']:
            orgs_.append(c['organization'].lower())
        if 'individualname' in c and c['individualname'] not in [None,'']:
            pers_.append(c['individualname'].lower())

    # contacts: ensure contact exists then link
    for b,c in (mcf.get('contact', {}) or {}).items():
        # has org or pers been matched before?
        if 'organization' in c and c['organization'] in skipMatchedOrgs:
            continue
        if 'individualname' in c and c['individualname'] in skipMatchedPers:
            continue
        # select identifiers
        orcid = None
        ror = None
        if c.get('url') and c.get('url').startswith('http'):
            if 'orcid.org' in c.get('url'):
                orcid = c.get('url')
            elif 'ror.org' in c.get('url'):
                ror = c.get('url')
        # email/orcid from the person
        pers_id = await upsert_pers(c,orcid)
        org_id = await upsert_org(c,ror) 

        # contact matching: in some cases organizations are listed separately from persons
        # find those cases and see if can make a match
        if not pers_id:
            # see if a person is in the list which belongs to this organization
            q_=f"select p.id, p.name, p.alias from {qn('employment')} e, {qn('persons')} p where p.id=e.person_id and e.organization_id = :org_id"
            res = await database.execute(q_,values={"org_id": org_id})
            for r in res:
                if str(r['name']).lower() in pers_:
                    pers_id = str(r['id'])
                    skipMatchedOrgs.append(str(r['name']).lower())
        elif not org_id:
            # see if a organization is in the list which belongs to this person
            q_=f"select o.id, o.name, o.alias from {qn('employment')} e, {qn('organizations')} o where o.id=e.organization_id and e.person_id = :pers_id"
            res = await database.execute(q_,values={"pers_id": pers_id})
            for r in res:
                if str(r['name']).lower() in orgs_:
                    org_id = str(r['id'])
                    skipMatchedOrgs.append(str(r['name']).lower())
                    
            # for r in recs:
            #  org in orgs  

        # contact brings together both person and org
        role = c.get('role','').lower()
        # in mcf, the key is sometimes a random string, sometimes it is the role name
        roles = ",".join(["resourceProvider,custodian,owner,user,distributor,originator,pointOfContact,principalInvestigator" +
                "processor,publisher,author,creator,contributor,originator,dataCollector,projectMember,projectManager,projectLeader",
                "workPackageLeader,researcher,editor,producer,supervisor,dataCurator,other"]).lower().split(',')
        if role == '' and b.lower() in roles:
            role = b.lower()

        if pers_id or org_id:
            try:
                await database.execute(f"""
                    INSERT INTO {qn('contact_in_record')} (
                        record_id, fk_organization, fk_person, role, position
                    ) VALUES (
                        :rid, :oid, :pid, :role, :position) on conflict (fk_organization, fk_person, record_id, role) do nothing""", 
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
            await database.execute(f"""INSERT INTO {qn('distributions')} (
                                    record_id, format, url, name, description
                                   ) VALUES (
                                    :rid, :fmt, :url, :name, :description
                                   ) on conflict (
                                   record_id, url, name) do nothing""", 
                                   values={'rid': record_id, 'fmt': fmt, 'url': url, 'name': name, 'description': description})

    # attributes 
    # todo: similar for dimensions?
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
        row = None
        kws = intl_list(v.get('keywords',{}) or {})
        thes_name = intl_str(v.get('vocabulary',{}).get('name','')).strip() or ''
        thes_url = (v.get('vocabulary',{}).get('url') or '').strip() or ''
        for kw in kws: # todo: this assumes kw is a str (startswith('http')?), not a uri+label -> understand how this info could be ingested/communicated
            if isinstance(kw,dict):
                if 'label' in kw:
                    kw = kw['label']
                else:
                    print("Cannot process keyword, no label found in dict:", kw)
                    continue
                
            if kw not in (None,''):
                subject_id = None
                if kw.lower().startswith('http'): # query by subject uri
                    row = await database.fetch_one(f"""SELECT id 
                                                   FROM {qn('subjects')} 
                                                   WHERE uri = :uri""", 
                                                values={'uri': kw})
                    if row:
                        subject_id = int(row['id'])
                    else:
                        row2 = await database.fetch_one(f"""INSERT INTO {qn('subjects')} 
                                                        (uri) VALUES (:uri) 
                                                        ON conflict(uri) 
                                                        DO UPDATE SET uri = EXCLUDED.uri 
                                                        RETURNING id""", 
                                                        values={'uri': kw})
                        subject_id = int(row2['id'])
                else: # query by parts
                    kw = kw.lower() 
                    if thes_url and thes_url.strip() != '':
                        row = await database.fetch_one(f"""SELECT id FROM {qn('subjects')} 
                                                       WHERE lower(label) = :label 
                                                       AND coalesce(thesaurus_url, '') = :thes_url""", 
                                                values={'label': kw, 'thes_url': thes_url })
                        
                        if row:
                            subject_id = int(row['id'])
                        else:
                            try:
                                row2 = await database.fetch_one(f"""
                                        INSERT INTO {qn('subjects')} (
                                            label, thesaurus_url
                                        ) VALUES (
                                            :label, :thes_url
                                        ) ON conflict(
                                            label, thesaurus_url
                                        ) DO UPDATE SET label = EXCLUDED.label
                                        RETURNING id;""", 
                                        values={'label': kw, 'thes_url': thes_url})
                                subject_id = int(row2['id'])
                            except:
                                None
                    elif thes_name and thes_name.strip() != '': # query gives nill if thes_name = nill
                        row = await database.fetch_one(f"""SELECT id FROM {qn('subjects')} 
                                                       WHERE lower(label) = :label 
                                                       AND lower(thesaurus_name) = :thes_name""", 
                                                values={'label': kw, 'thes_name': thes_name.lower() })
                        if row:
                            subject_id = int(row['id'])
                        else:
                            try: 
                                row2 = await database.fetch_one(f"""
                                        INSERT INTO {qn('subjects')} (
                                            label, thesaurus_name
                                        ) VALUES (
                                            :label, :thes_name 
                                        ) ON conflict(
                                            label, thesaurus_name
                                        ) DO UPDATE SET label = EXCLUDED.label
                                        RETURNING id;""", 
                                        values={'label': kw, 'thes_name': thes_name.lower()})
                                subject_id = int(row2['id'])
                            except:
                                None
                    
                    else: #no thesaurus
                        row = await database.fetch_one(f"""SELECT id FROM {qn('subjects')}
                                                       WHERE label = :label
                                                       AND coalesce(thesaurus_name, '') = ''
                                                       AND coalesce(thesaurus_url, '') = ''""", 
                                                values={'label': kw })
                        if row:
                            subject_id = int(row['id'])
                        else:
                            try:
                                row2 = await database.fetch_one(f"""
                                    INSERT INTO {qn('subjects')} (
                                        label, thesaurus_name, thesaurus_url
                                    ) VALUES (
                                        :label, :thes_name, :thes_url
                                    ) ON conflict(
                                        label, thesaurus_name, thesaurus_url
                                    ) DO UPDATE SET label = EXCLUDED.label
                                        RETURNING id;""", 
                                    values={'label': kw, 'thes_name': thes_name.lower(), 'thes_url': thes_url})
                                subject_id = int(row2['id'])
                            except:
                                None

                if subject_id not in [None,'']:
                    try:       
                        await database.execute(f"""INSERT INTO {qn('record_subject')} (
                                            record_id, subject_id
                                            ) VALUES (:rid, :sid) 
                                            ON CONFLICT DO NOTHING""", 
                                            values={'rid': record_id, 
                                                    'sid': subject_id})
                    except:
                        None

    # relations
    sources = []
    for rel in mcf.get('metadata',{}).get('relations', []) or []:
        if isinstance(rel, dict) and rel.get('identifier') not in (None,''):
            if rel.get('type','')=='source':
                sources.append(rel.get('identifier'))
            else:
                await database.execute(f"""INSERT INTO {qn('relations')} (
                                       record_id, identifier, scheme, type
                                       ) VALUES (
                                       :rid, :identifier, :scheme, :type) 
                                       ON CONFLICT DO NOTHING""", 
                                       values={'rid': record_id, 
                                               'identifier': rel.get('identifier'),
                                               'scheme': rel.get('scheme'), 
                                               'type': rel.get('type') })    
    # sources
    for src in sources:
        await upsert_source(record_id, src)

    return record_id

async def upsert_source(record_id,src):
    if src not in (None,''):
        rws = await database.fetch_one(f"SELECT name FROM {qn('sources')} WHERE lower(name) = :name", values={'name': src.lower()})
        if rws:
            fk_source = str(rws['name'])
        else:
            r2 = await database.fetch_one(f"INSERT INTO {qn('sources')} (name) VALUES (:name) RETURNING name", values={'name': src.lower()})
            fk_source = str(r2['name'])
        await database.execute(f"INSERT INTO {qn('record_sources')} (record_id, fk_source) VALUES (:rid, :sid) ON CONFLICT DO NOTHING",
                            values={'rid': record_id, 'sid': fk_source})

async def upsert_project(record_id, prj):
    if prj not in (None,'','None'):
        await database.execute(f"INSERT INTO {qn('record_in_project')} (record_id, project) VALUES (:rid, :prj) ON CONFLICT DO NOTHING",
                            values={'rid': record_id, 'prj': prj})

async def upsert_pers(c,orcid=None):
    if orcid:
        row = await database.fetch_one(f"SELECT id FROM {qn('person')} WHERE orcid = :orcid", values={'orcid': orcid})
        if row:
            return int(row['id'])
    if c.get('individualname') not in (None,''):
        row = await database.fetch_one(f"""
                SELECT id FROM {qn('person')} 
                WHERE lower(name) = :name 
                or lower(alias) like :name2""",  # todo: prevent match on partial names in alias
                values={'name': c.get('individualname').lower(), 
                        'name2': f"%{c.get('individualname').lower()}%"})
        if row:
            return int(row['id'])
    if c.get('email') not in (None,''):
        row = await database.fetch_one(f"""
                SELECT id FROM {qn('person')} 
                WHERE email = :email""", 
                values={'email': c.get('email')})
                        
        if row:
            return int(row['id'])
    # insert new person
    res = await database.fetch_one(
        f"INSERT INTO {qn('person')} (name, email, orcid) VALUES (:name, :email, :orcid) RETURNING id",
        values={
            'name': c.get('individualname',c.get('name')),
            'email': c.get('email'),
            'orcid': orcid
        }
    )    
    return int(res['id'])
        
async def upsert_org(c,ror=None):
    # see if organization already exists
    if ror:
        row = await database.fetch_one(f"SELECT id FROM {qn('organization')} WHERE ror = :ror", values={'ror': ror})
        if row:
            return int(row['id'])
    if c.get('organization') not in (None,''):
        row = await database.fetch_one(f"""
                SELECT id FROM {qn('organization')} 
                WHERE lower(name) = :name 
                or lower(alias) like :name2""", 
                values={'name': c.get('organization').lower(), 
                        'name2': f"%{c.get('organization').lower()}%"})
        if row:
            return int(row['id'])
    if c.get('url') not in (None,''):
        row = await database.fetch_one(f"""
                SELECT id FROM {qn('organization')} 
                WHERE url = :url""", 
                values={'url': c.get('url')})
        if row:
            return int(row['id'])
    # insert new org
    res = await database.fetch_one(
        f"""INSERT INTO {qn('organization')} (
                name, phone, ror, address, postalcode, city, administrativearea, country, url
            ) VALUES (
                :name, :phone, :ror, :address, :postalcode, :city, :administrativearea, :country, :url
            ) RETURNING id""",
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
        dt = parse(ds.split("+")[0], fuzzy=True)
        return dt.replace(tzinfo=None)
    except:
        return None


async def reprocess_rows():
    query = f"""
            select identifier, raw_mcf, md5_hash, harvest_date 
            from {qn('records')}
            where raw_mcf is not Null
            """
    rows = await database.fetch_all(query)
    logger.info('Selected %d source rows to update', len(rows))

    for r in rows:
        identifier = str(r['identifier'])
        mcf = json.loads(r['raw_mcf'])
        md5_hash= r['md5_hash']
        harvest_date= r['harvest_date']
        await insert_record_and_related(mcf=mcf, record_id=identifier, md5_hash=md5_hash, project=None, harvest_date=harvest_date, modus="update")

async def process_source_rows(PROCESS_SOURCE=None, RECORDS_PER_PAGE=100):

    filtersql=""
    if PROCESS_SOURCE:
        filtersql = f" AND upper(s.source) = '{PROCESS_SOURCE.upper().strip()}' "

    # Select only source rows whose MD5 (if present) 
    # is not already in records
    query = f"""
        SELECT identifier, identifiertype, resultobject,
          resulttype, hash, source, project, turtle, error, (   
            select turtle_prefix from harvest.sources 
            where name = s.source
          ) as ttl_pref, doimetadata, insert_date 
        FROM harvest.vw_unique_harvest_items s 
        WHERE (NOT EXISTS (
            SELECT 1 FROM metadata.records_failed r WHERE r.hash = s.hash)) 
        AND (NOT EXISTS (
            SELECT 1 FROM metadata.records_processed r WHERE r.hash = s.hash)) 
        {filtersql}   
        LIMIT {RECORDS_PER_PAGE}"""

    # resulttype='iso19139:2007' and
    rows = await database.fetch_all(query)
    logger.info('Selected %d source rows to process', len(rows))

    for r in rows:
        identifier = str(r['identifier'])
        txt = r['resultobject']
        resulttype = r['resulttype']
        md5_hash = r['hash']
        source = str(r['source'])
        project = r['project']
        turtle = str(r['turtle'])
        ttl_pref = str(r['ttl_pref']) or ''
        doimetadata = r['doimetadata']
        identifiertype = r['identifiertype']
        error = r['error']
        insert_date = r['insert_date'] or ''
        ahash = r['hash']

        mcf = None

        try:
        # some sources log to various source-fields
        # based on source select the relevant source field
            #logger.info(f'parse {source}:{identifier}')
            if identifiertype == 'doi' or doimetadata not in (None,''):
                if doimetadata in (None,''):
                    raise ValueError(f'Empty doi metadata for {identifier} from {source}: {error}')
                txt = doimetadata
                mcf = import_metadata('openaire', txt)
                if not isinstance(mcf, dict) or 'identification' not in mcf.keys():
                    mcf = import_metadata('autodetect', txt) # try again as autodetect -> Datacite?
                    if not isinstance(mcf, dict) or 'identification' not in mcf.keys():
                        raise ValueError(f'Failed parsing {identifier} from {source} as autodetect')
            elif resulttype == 'schema.org' and txt not in [None,'']:
                mcf = import_metadata('schema-org', txt)
                if not isinstance(mcf, dict) or 'identification' not in mcf.keys():
                    raise ValueError(f'Failed parsing {identifier} from {source} as schema.org')
                elif mcf['identification'].get('title','') != '':
                    # requires at least title (case impact4soil)
                    raise ValueError(f'Incomplete record {identifier} from {source} as schema.org')
            elif resulttype == 'iso19139:2007' or '<gmd:MD_Metadata' in txt:
                mcf = import_metadata('iso19139', txt)
                if not isinstance(mcf, dict) or 'identification' not in mcf.keys():
                    raise ValueError(f'Failed parsing {identifier} from {source} as iso19139:2007')
            elif source == 'DATA.EUROPA.EU' or source == 'DATA.EUROPA.EU.BY.SOIL':
                mcf = import_metadata('schema-org', txt)
                if not isinstance(mcf, dict) or 'identification' not in mcf.keys():
                    raise ValueError(f'Failed parsing {identifier} from {source} as explicit schema-org metadata')
            #elif source in ('CORDIS','IMPACT4SOIL','PREPSOIL'): # use turtle 
            #    txt = r['']
            elif turtle and len(turtle) > 10:
                txt = ttl_pref + turtle 
                mcf = import_metadata('dcat', txt)
                if not isinstance(mcf, dict) or 'identification' not in mcf.keys():
                    raise ValueError(f'Failed parsing {identifier} from {source} as implicit dcat metadata')
            else:           
                mcf = import_metadata('autodetect', txt)
                if not isinstance(mcf, dict) or 'identification' not in mcf.keys():
                    raise ValueError(f'Failed parsing {identifier} from {source} as autodetect metadata')
                        
            # insert record and related entities
            record_id = await insert_record_and_related(mcf, identifier, md5_hash, source, project, r['insert_date'])

            logger.info('record id %s for source %s', identifier, source)

        except Exception as e:
            logger.error(f"Import in {source}: {e}: {traceback.format_exc()}")
            await database.fetch_one(
                f"INSERT INTO {qn('records_failed')} (identifier, hash, error, date) VALUES (:id, :hash, :error, CURRENT_TIMESTAMP) on conflict (hash) do nothing",
                values={
                    'id': identifier,
                    'hash': ahash,
                    'error': f"Import in {source}: {e}: {traceback.format_exc()}"
                })

async def main():
    PROCESS_MODE = os.getenv('PROCESS_MODE') or "INSERT"
    print("Processing",PROCESS_MODE)
    await database.connect()
    PROCESS_SOURCES = os.getenv('PROCESS_SOURCES', '').split(',')
    RECORDS_PER_PAGE = os.getenv('RECORDS_PER_PAGE', 50)
    if PROCESS_MODE == 'UPDATE':
        await reprocess_rows()
    else:
        if PROCESS_SOURCES == ['sampling']:
            recs = await database.fetch_all(f"select name from harvest.sources")
            for r in recs:
                logger.info(f"Processing {r['name']}")
                await process_source_rows(r['name'], 50)
        elif len(PROCESS_SOURCES) > 0 :
            for r in PROCESS_SOURCES:
                await process_source_rows(r, RECORDS_PER_PAGE) 
        else:    
            await process_source_rows(None, RECORDS_PER_PAGE)
    await database.disconnect()


if __name__ == '__main__':
    asyncio.run(main())
