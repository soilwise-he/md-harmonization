# script updates the materialized view used by pycsw instance

from typing import Any, Dict, Optional, Tuple
import asyncio, os
import databases
from dotenv import load_dotenv

# Load environment
load_dotenv()

host=os.environ.get("POSTGRES_HOST",'')
port=os.environ.get("POSTGRES_PORT",'')
dbname=os.environ.get("POSTGRES_DB",'')
user=os.environ.get("POSTGRES_USER",'')
password=os.environ.get("POSTGRES_PASSWORD",'')
DATABASE_URL = os.getenv('DATABASE_URL') or f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

database = databases.Database(DATABASE_URL)

async def main():
    await database.connect()

    qry="drop table metadata.pycsw_records;"
    res = await database.execute(qry)

    qry="""
    SELECT 
        identifier,
        title, abstract, edition, language, 
        spatial as bbox, 
        'csw:Record' as typename, 
        'http://www.opengis.net/cat/csw/2.0.2'as schema,
        'local' as mdsource, 
        coalesce(TO_CHAR(harvestdate, 'YYYY-MM-DD')) as insert_date, 
        '' as xml, 
        '' as metadata, 
        'application/xml' as metadata_type, 
        type, 
        '' as title_alternate,
        replace(trim(both '[]' from subjects::text), '"', '') as keywords,
        '' as keywordstype, 
        '' as themes, 
        '' as parentidentifier, 
        '' as relation, 
        coalesce(TO_CHAR(temporal_start, 'YYYY-MM-DD'), '') as time_begin, 
        coalesce(TO_CHAR(temporal_end, 'YYYY-MM-DD'), '') as time_end, 
        '' as topicategory, 
        language as resourcelanguage, 
        authors->0->>'person' as creator,
        '' as publisher, 
        '' as contributor, 
        '' as organization, 
        rights as securityconstraints, 
        '' as accessconstraints, 
        license as otherconstraints, 
        coalesce(TO_CHAR(metadatadate, 'YYYY-MM-DD'), '') as date, 
        coalesce(TO_CHAR(revisiondate, 'YYYY-MM-DD'), '') as date_revision, 
        coalesce(TO_CHAR(creationdate, 'YYYY-MM-DD'), '') as date_creation, 
        coalesce(TO_CHAR(publicationdate, 'YYYY-MM-DD'), '') as date_publication, 
        coalesce(TO_CHAR(revisiondate, 'YYYY-MM-DD'), '') as date_modified, 
        format, sources ->> 0 as source, '' as crs, '' as geodescode, resolution as denominator, '' as distancevalue, '' as distanceuom, 
        ST_AsText(ST_MakeEnvelope(
                    (string_to_array(trim(spatial, '[]'), ','))[1]::double precision,  -- xmin
                    (string_to_array(trim(spatial, '[]'), ','))[2]::double precision,  -- ymin
                    (string_to_array(trim(spatial, '[]'), ','))[3]::double precision,  -- xmax
                    (string_to_array(trim(spatial, '[]'), ','))[4]::double precision,  -- ymax
                    4326)) AS wkt_geometry, 
        '' as servicetype, '' as servicetypeversion, '' as operation, '' as couplingtype, '' as operateson, '' as operatesonidentifier, '' as operatesoname, 
        '' as degree, '' as classification, '' as conditionapplyingtoaccessanduse, lineage, 
        '' as responsiblepartyrole, '' as specificationtitle, '' as specificationdate, '' as specificationdatetype, 
        '' as platform, '' as instrument, '' as sensortype, '' as cloudcover, '' as bands, '' as links, '' as tsvector, 
        ST_AsBinary(ST_MakeEnvelope(
                    (string_to_array(trim(spatial, '[]'), ','))[1]::double precision,  -- xmin
                    (string_to_array(trim(spatial, '[]'), ','))[2]::double precision,  -- ymin
                    (string_to_array(trim(spatial, '[]'), ','))[3]::double precision,  -- xmax
                    (string_to_array(trim(spatial, '[]'), ','))[4]::double precision,  -- ymax
                    4326)) AS wkb_geometry, 
        subjects||title||abstract||lineage||contacts as anytext, 
        contacts as contacts,
        '' as illuminationelevationangle
        
    INTO metadata.pycsw_records 
    FROM metadata.mv_records;
    """
    res = await database.execute(qry)

    qry="""
    ALTER TABLE metadata.pycsw_records
    ADD PRIMARY KEY (identifier);
    """
    res = await database.execute(qry)

    await database.disconnect()


if __name__ == '__main__':
    asyncio.run(main())
