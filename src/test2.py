import os
import time
import json
import requests
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from lxml import etree
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import re

# 載入 .env
load_dotenv()

# 設定
CSV_PATH = os.getenv('TICKER_CSV_PATH', '../csv/global_ticker.csv')
XBRL_DIR = os.getenv('XBRL_DIR', os.path.normpath(os.path.join(os.path.dirname(__file__), 'xbrl_downloads')))
os.makedirs(XBRL_DIR, exist_ok=True)

DB_PARAMS = {
    'host':     os.getenv('DB_HOST'),
    'port':     os.getenv('DB_PORT'),
    'dbname':   os.getenv('DB_NAME'),
    'user':     os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
}

# 連線 PostgreSQL
conn = psycopg2.connect(**DB_PARAMS)
conn.autocommit = True

HEADERS = {"User-Agent": "Your Name your@email.com"}
CIK_URL = "https://www.sec.gov/files/company_tickers.json"
BASE_SUB_URL = "https://data.sec.gov/submissions/CIK{}.json"
BASE_ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data"

def ensure_table(cursor, table_name):
    # 使用 pg 的 to_regclass 檢查 table 是否存在
    cursor.execute(
        "SELECT to_regclass(%s);",
        (table_name,)
    )
    exists = cursor.fetchone()[0] is not None
    if exists:
        # 已處理過
        return False
    # 不存在就建立
    cursor.execute(sql.SQL("""
        CREATE TABLE {} (
            id      SERIAL PRIMARY KEY,
            report  VARCHAR(32) UNIQUE,
            facts   JSONB
        );
    """).format(sql.Identifier(table_name)))
    return True

def load_cik_map():
    resp = requests.get(CIK_URL, headers=HEADERS)
    resp.raise_for_status()
    data = resp.json()
    # 對 CIK 左側補零至 10 位
    return {item['ticker'].lower(): str(item['cik_str']).zfill(10) for item in data.values()}

def get_cik(ticker, cik_map):
    return cik_map.get(ticker.lower())

def get_filings(cik, min_count=40):
    url_main = BASE_SUB_URL.format(cik)
    resp = requests.get(url_main, headers=HEADERS)
    time.sleep(0.2)
    if resp.status_code != 200:
        return []
    data_main = resp.json()
    def extract_from(block):
        fld = block.get('filings', {}).get('recent', block)
        out = []
        for i, form in enumerate(fld.get('form', [])):
            if form in ('10-Q', '10-K'):
                acc = fld['accessionNumber'][i]
                no_dash = acc.replace('-', '')
                out.append({
                    'accessionNumber': acc,
                    'reportDate':      fld['reportDate'][i],
                    'filingDate':      fld['filingDate'][i],
                    'filingURL':       f"{BASE_ARCHIVE_URL}/{int(cik)}/{no_dash}/index.json",
                    'form':            form
                })
        return out
    filings = extract_from(data_main)
    for page in data_main.get('filings', {}).get('files', []):
        if len(filings) >= min_count:
            break
        name = page.get('name')
        if not name:
            continue
        page_url = f"https://data.sec.gov/submissions/{name}"
        pr = requests.get(page_url, headers=HEADERS)
        time.sleep(0.2)
        if pr.status_code != 200:
            continue
        filings += extract_from(pr.json())
    # 去重並依 filingDate 排序
    seen = set()
    unique = []
    for f in sorted(filings, key=lambda x: x['filingDate'], reverse=True):
        if f['accessionNumber'] not in seen:
            unique.append(f)
            seen.add(f['accessionNumber'])
    return unique

def get_quarter(filing_date_str):
    dt = datetime.strptime(filing_date_str, "%Y-%m-%d")
    m, y = dt.month, dt.year
    if m in (4,5,6):
        return f"{y}Q1"
    if m in (7,8,9):
        return f"{y}Q2"
    if m in (10,11,12):
        return f"{y}Q3"
    return f"{y-1}Q4"

def download_and_insert(ticker, filing, cur, cik):
    quarter = get_quarter(filing['filingDate'])
    report_name = f"{ticker}_{quarter}"
    if filing['form'] == '10-K':
        report_name += "&Annual"

    # 檢查是否已存在
    cur.execute(
        sql.SQL("SELECT 1 FROM {table} WHERE report = %s").format(
            table=sql.Identifier(ticker.lower())
        ),
        (report_name,)
    )
    if cur.fetchone():
        print(f"[INFO] {ticker} {report_name} exists, skip")
        return False

    # 取得 index.json
    idx = requests.get(filing['filingURL'], headers=HEADERS)
    time.sleep(0.2)
    if idx.status_code != 200:
        print(f"[WARNING] can't get index.json for {ticker} {report_name}")
        return False

    items = idx.json().get("directory", {}).get("item", [])
    # 只留三種候選檔案：YYYYMMDD.xml 或 *_htm.xml
    xml_items = [
        d for d in items
        if re.search(r'-\d{8}\.xml$|_htm\.xml$', d['name'], re.IGNORECASE)
    ]

    print(f"[INFO] {ticker} {report_name} find {len(xml_items)} files：")
    for doc in xml_items:
        print(f"  - {doc['name']}")
    if not xml_items:
        print(f"[WARNING] {ticker} {report_name} can't find any files")
        return False

    # 優先順序：先純日期檔，再所有 *_htm.xml
    raw_xml = [d for d in xml_items if re.search(r'-\d{8}\.xml$', d['name'])]
    htm_xml = [d for d in xml_items if re.search(r'_htm\.xml$', d['name'], re.IGNORECASE)]
    ordered = raw_xml + htm_xml
    if not ordered:
        ordered = xml_items

    # 下載並解析，遇到非空 facts 就存入
    for doc in ordered:
        print(f"[TRY] use：{doc['name']}")
        url = f"{BASE_ARCHIVE_URL}/{int(cik)}/{filing['accessionNumber'].replace('-','')}/{doc['name']}"
        r = requests.get(url, headers=HEADERS)
        time.sleep(0.2)
        if r.status_code == 200:
            try:
                tree = etree.fromstring(r.content)
                facts = {}
                for fact in tree.findall('.//'):
                    ctx = fact.get('contextRef')
                    if not ctx:
                        continue
                    tag = etree.QName(fact.tag).localname
                    facts[tag] = {
                        'value': fact.text,
                        'unitRef': fact.get('unitRef'),
                        'contextRef': ctx,
                        'decimals': fact.get('decimals')
                    }
                # 空的跳過
                if not facts:
                    print(f"[WARNING] {ticker} {report_name} parsed {doc['name']} get null facts, skip")
                    continue

                cur.execute(
                    sql.SQL("INSERT INTO {table} (report, facts) VALUES (%s, %s::jsonb)").format(
                        table=sql.Identifier(ticker.lower())
                    ),
                    (report_name, json.dumps(facts))
                )
                print(f"[SUCCESS] {ticker} {report_name} downloaded & parsed,source：{doc['name']}）")
                return True
            except Exception as e:
                print(f"[ERROR] parsed {ticker} {report_name} {doc['name']} fail: {e}")
    return False


def main():
    df = pd.read_csv(CSV_PATH, dtype=str)
    tickers = df['Ticker'].dropna().unique()
    print(f"[INFO] all {len(tickers)} stocks")
    cik_map = load_cik_map()
    no_reports = []
    few_reports = []
    with conn.cursor() as cur:
        # 逐支股票處理
        for ticker in tqdm(tickers):
            #print("")
            table_name = ticker.lower()
            print(f"\n[INFO] ===== {ticker} ===== ")
            # 每次處理前，建立該 ticker 的 table
            first_time = ensure_table(cur, table_name)
            if not first_time:
                print(f"[SKIP] {ticker} is exist, skip")
                continue

            cik = get_cik(ticker, cik_map)
            if not cik:
                print(f"[WARNING] {ticker} can't get CIK")
                no_reports.append(ticker)
                continue

            filings = get_filings(cik)
            if not filings:
                print(f"[WARNING] {ticker} no any filings")
                no_reports.append(ticker)
                continue
            if len(filings) < 10:
                few_reports.append(ticker)

            for filing in filings:
                download_and_insert(ticker, filing, cur, cik)
            time.sleep(1)

    # 輸出沒有報告或不足的
    pd.DataFrame(no_reports, columns=['Ticker']).to_csv('no_reports.csv', index=False)
    pd.DataFrame(few_reports, columns=['Ticker']).to_csv('few_reports.csv', index=False)
    print("\n[INFO] success")

if __name__ == "__main__":
    main()
