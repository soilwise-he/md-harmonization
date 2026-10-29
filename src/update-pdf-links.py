# script updates the pdf links
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

    qry="""
    SELECT r.record_id,l.urlname 
    FROM linky.links l, linky.records r 
    where link_type='application/pdf'
    and not deprecated 
    and r.id = l.fk_record
    """
    res = await database.fetch_all(qry)

    for r in res:
        record_id = r['record_id'].split('items/').pop()
        urlname   = r['urlname']
        try:
            await database.fetch_one("""
            INSERT INTO metadata.pdf_items(identifier, pdfurl)
            VALUES (:id, :url) 
            ON CONFLICT (identifier, pdfurl) DO NOTHING""",
            values={
                'id': record_id,
                'url': urlname})
        except Exception as e:
            print(e)
    await database.disconnect()


if __name__ == '__main__':
    asyncio.run(main())