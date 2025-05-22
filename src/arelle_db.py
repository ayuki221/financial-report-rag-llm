import os
import glob
import pandas as pd
import json
from lxml import etree
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

csv_path = os.getenv('TICKER_CSV_PATH', '../csv/test.csv')
df = pd.read_csv(csv_path, dtype=str)
tickers = df['Ticker'].dropna().unique()

# 取得 script 所在資料夾，再定位到 xbrl_downloads
base_dir = os.path.dirname(os.path.abspath(__file__))
xml_dir = os.getenv('XBRL_DIR',
    os.path.normpath(os.path.join(base_dir, '..', 'xbrl_downloads'))
)

# DB 連線參數
DB_PARAMS = {
    'host':     os.getenv('DB_HOST'),
    'port':     os.getenv('DB_PORT'),
    'dbname':   os.getenv('DB_NAME'),
    'user':     os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
}
conn = psycopg2.connect(**DB_PARAMS)
conn.autocommit = True

def ensure_table(cursor, table_name):
    create = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {table} (
            id SERIAL PRIMARY KEY,
            report VARCHAR,
            facts JSONB
        )
    """).format(table=sql.Identifier(table_name))
    cursor.execute(create)

with conn.cursor() as cur:
    for ticker in tickers:
        tbl = ticker.lower()
        ensure_table(cur, tbl)

        patterns = [
            os.path.join(xml_dir, f"{ticker}_*Q?.xml"),
            os.path.join(xml_dir, f"{ticker}_*Q?&Annual.xml")
        ]
        xbrl_files = []
        for pat in patterns:
            matched = glob.glob(pat)
            # 顯示 matched 的數量和檔案清單
            names = [os.path.basename(p) for p in matched]
            print(f"[DEBUG] pattern={os.path.basename(pat)!r} -> found {len(names)} files:")
            xbrl_files.extend(matched)

        if not xbrl_files:
            print(f"[WARNING] ticker={ticker!r}: no XBRL files found under {xml_dir}")
            continue

        for fp in xbrl_files:
            report = os.path.basename(fp).rsplit('.',1)[0]
            tree = etree.parse(fp)
            root = tree.getroot()
            facts = {}
            for fact in root.findall('.//'):
                ctx = fact.get('contextRef')
                if not ctx: continue
                tag = etree.QName(fact.tag).localname
                facts[tag] = {
                    'value': fact.text,
                    'unitRef': fact.get('unitRef'),
                    'contextRef': ctx,
                    'decimals': fact.get('decimals')
                }
            insert = sql.SQL(
                "INSERT INTO {table} (report, facts) VALUES (%s, %s::jsonb)"
            ).format(table=sql.Identifier(tbl))
            cur.execute(insert, (report, json.dumps(facts)))

print("all ticker XBRL->JSONB finish")
