import os
import time
import glob
import json
import requests
import pandas as pd
from lxml import etree
from datetime import datetime
from tqdm import tqdm
from psycopg2 import connect, sql
from dotenv import load_dotenv

# 載入環境變數
load_dotenv()

# DB 連線參數
DB_PARAMS = {
    'host':     os.getenv('DB_HOST'),
    'port':     os.getenv('DB_PORT'),
    'dbname':   os.getenv('DB_NAME'),
    'user':     os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
}

# XBRL 檔案存放路徑
base_dir = os.path.dirname(os.path.abspath(__file__))
XML_DIR = os.getenv(
    'XBRL_DIR',
    os.path.normpath(os.path.join(base_dir, '..', 'xbrl_downloads'))
)
os.makedirs(XML_DIR, exist_ok=True)

# CSV 檔案路徑
CSV_PATH = os.getenv('TICKER_CSV_PATH', '../csv/test.csv')

# HTTP headers for SEC requests
HEADERS = {
    "User-Agent": os.getenv('SEC_USER_AGENT', 'Your Name your@email.com')
}

# EDGAR API URLs
BASE_SUB_URL    = "https://data.sec.gov/submissions/CIK{}.json"
BASE_ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data"

# 功能函式

def get_cik(ticker):
    """從 SEC 公司代碼清單取得 CIK"""
    url = "https://www.sec.gov/files/company_tickers.json"
    res = requests.get(url, headers=HEADERS)
    time.sleep(0.2)
    if res.status_code != 200:
        return None
    data = res.json()
    for item in data.values():
        if item['ticker'].lower() == ticker.lower():
            return str(item['cik_str']).zfill(10)
    return None


def get_filings(cik, min_count=40):
    """取得最近的 10-Q 與 10-K 申報清單"""
    resp = requests.get(BASE_SUB_URL.format(cik), headers=HEADERS)
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
        filings.extend(extract_from(pr.json()))

    # 去重並依申報日排序
    seen = set(); unique = []
    for f in sorted(filings, key=lambda x: x['filingDate'], reverse=True):
        if f['accessionNumber'] not in seen:
            unique.append(f)
            seen.add(f['accessionNumber'])
    return unique


def get_quarter(filing_date_str):
    """依據申報日期推算季度標籤"""
    filing_date = datetime.strptime(filing_date_str, "%Y-%m-%d")
    m, y = filing_date.month, filing_date.year
    if m in [4,5,6]:
        return f"{y}Q1"
    elif m in [7,8,9]:
        return f"{y}Q2"
    elif m in [10,11,12]:
        return f"{y}Q3"
    else:
        return f"{y-1}Q4"


def download_xbrl(filing, cik, ticker, save_dir):
    """下載單筆申報的 XBRL 原始檔案"""
    res = requests.get(filing['filingURL'], headers=HEADERS)
    time.sleep(0.2)
    if res.status_code != 200:
        print(f"[error] can't get index.json for {ticker} {filing['accessionNumber']}")
        return False
    doc_data = res.json()
    items = doc_data.get("directory", {}).get("item", [])
    xbrl_docs = [d for d in items if d["name"].lower().endswith(".xml")]
    if not xbrl_docs:
        return False
    quarter = get_quarter(filing['filingDate'])
    prefix = f"{ticker}_{quarter}" + ("&Annual" if filing['form']=='10-K' else "")
    for doc in xbrl_docs:
        xbrl_url = (
            f"{BASE_ARCHIVE_URL}/{int(cik)}/"
            f"{filing['accessionNumber'].replace('-', '')}/{doc['name']}"
        )
        r = requests.get(xbrl_url, headers=HEADERS)
        time.sleep(0.2)
        if r.status_code == 200:
            path = os.path.join(save_dir, f"{prefix}.xml")
            with open(path, "wb") as f:
                f.write(r.content)
            print(f"[download] {prefix}.xml")
            return True
    return False


def ensure_table(cursor, table_name):
    """如果不存在則建立資料表，內含 id, report 與 JSONB 欄位"""
    create = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {table} (
            id SERIAL PRIMARY KEY,
            report VARCHAR,
            facts JSONB
        )""").format(table=sql.Identifier(table_name))
    cursor.execute(create)


if __name__ == "__main__":
    # 建立 DB 連線
    conn = connect(**DB_PARAMS)
    conn.autocommit = True
    cur = conn.cursor()

    # 讀取 Ticker 清單
    df = pd.read_csv(CSV_PATH, dtype=str)
    tickers = df['Ticker'].dropna().unique()

    for ticker in tqdm(tickers, desc="Processing tickers"):
        print(f"\n=== {ticker} ===")
        # 1. 取得 CIK
        cik = get_cik(ticker)
        if not cik:
            print(f"[WARNING] no CIK for {ticker}")
            continue

        # 2. 取得申報清單
        filings = get_filings(cik)
        if not filings:
            print(f"[WARNING] no filings for {ticker}")
            continue

        # 3. 下載所有申報 XBRL
        for f in filings:
            download_xbrl(f, cik, ticker, save_dir=XML_DIR)
        time.sleep(1)

        # 4. 建立對應資料表
        tbl = ticker.lower()
        ensure_table(cur, tbl)

        # 5. 解析下載的 XBRL 並插入資料庫
        patterns = [
            os.path.join(XML_DIR, f"{ticker}_*Q?.xml"),
            os.path.join(XML_DIR, f"{ticker}_*Q?&Annual.xml")
        ]
        xbrl_files = []
        for pat in patterns:
            matched = glob.glob(pat)
            names = [os.path.basename(p) for p in matched]
            print(f"[DEBUG] pattern={os.path.basename(pat)!r} -> found {len(names)} files: {names}")
            xbrl_files.extend(matched)

        if not xbrl_files:
            print(f"[WARNING] no XBRL files to parse for {ticker}")
            continue

        for fp in xbrl_files:
            report = os.path.basename(fp).rsplit('.', 1)[0]
            tree = etree.parse(fp)
            root = tree.getroot()
            facts = {}
            for fact in root.findall('.//'):
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
            insert = sql.SQL(
                "INSERT INTO {table} (report, facts) VALUES (%s, %s::jsonb)"
            ).format(table=sql.Identifier(tbl))
            cur.execute(insert, (report, json.dumps(facts)))

    print("All tickers processed and loaded into DB.")
