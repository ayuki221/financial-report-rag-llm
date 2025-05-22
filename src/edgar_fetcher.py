import os
import time
import requests
import pandas as pd
from tqdm import tqdm
from datetime import datetime

HEADERS = {
    "User-Agent": "Your Name your@email.com"
}

BASE_SUB_URL = "https://data.sec.gov/submissions/CIK{}.json"
BASE_ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data"

def get_cik(ticker):
    cik_url = "https://www.sec.gov/files/company_tickers.json"
    res = requests.get(cik_url, headers=HEADERS)
    time.sleep(0.2)
    data = res.json()
    for item in data.values():
        if item['ticker'].lower() == ticker.lower():
            return str(item['cik_str']).zfill(10)
    return None

def get_filings(cik, min_count=40):
    # 1. 先拿主檔
    url_main = BASE_SUB_URL.format(cik)
    resp = requests.get(url_main, headers=HEADERS)
    time.sleep(0.2)
    if resp.status_code != 200:
        return []

    data_main = resp.json()

    # 2. 萃取 helper（支援 main.recent 或 page root）
    def extract_from(block):
        fld = block.get('filings', {}).get('recent', block)
        out = []
        for i, form in enumerate(fld.get('form', [])):
            if form in ('10-Q','10-K'):
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

    # 3. 先抓最近的幾筆
    filings = extract_from(data_main)

    # 4. 當不足時，往所有「分頁檔」繼續抓
    for page in data_main.get('filings', {}).get('files', []):
        if len(filings) >= min_count:
            break
        name = page.get('name')  # e.g. "CIK0000019617-submissions-011.json"
        if not name:
            continue
        page_url = f"https://data.sec.gov/submissions/{name}"
        pr = requests.get(page_url, headers=HEADERS)
        time.sleep(0.2)
        if pr.status_code != 200:
            continue
        filings += extract_from(pr.json())

    # 5. 去重並依申報日排序
    seen = set(); unique = []
    for f in sorted(filings, key=lambda x: x['filingDate'], reverse=True):
        if f['accessionNumber'] not in seen:
            unique.append(f); seen.add(f['accessionNumber'])

    return unique

def get_quarter(filing_date_str):
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

def download_xbrl(filing, cik, ticker, save_dir="../xbrl_downloads"):
    res = requests.get(filing['filingURL'], headers=HEADERS)
    time.sleep(0.2)
    if res.status_code != 200:
        print(f"[error] can't get index.json for {ticker} {filing['accessionNumber']}")
        return False

    doc_data = res.json()
    items = doc_data.get("directory", {}).get("item", [])

    xbrl_docs = [
        d for d in items
        if d["name"].lower().endswith(".xml")
    ]

    if not xbrl_docs:
        return False

    os.makedirs(save_dir, exist_ok=True)
    quarter = get_quarter(filing['filingDate'])
    prefix = f"{ticker}_{quarter}"
    if filing['form'] == '10-K':
        prefix += "&Annual"

    for doc in xbrl_docs:
        xbrl_url = (
            f"https://www.sec.gov/Archives/edgar/data/"
            f"{int(cik)}/{filing['accessionNumber'].replace('-', '')}/{doc['name']}"
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

def process_csv(csv_path):
    df = pd.read_csv(csv_path)
    tickers = df["Ticker"].dropna().unique()
    no_reports   = []
    few_reports  = []

    for ticker in tqdm(tickers):
        print("\n")
        cik = get_cik(ticker)
        if not cik:
            no_reports.append(ticker)
            continue

        filings = get_filings(cik)
        if not filings:
            no_reports.append(ticker)
            continue

        if len(filings) < 10:
            few_reports.append(ticker)

        ok = False
        for f in filings:
            if download_xbrl(f, cik, ticker):
                ok = True
        if not ok:
            no_reports.append(ticker)

        time.sleep(5)

    return no_reports, few_reports

if __name__ == "__main__":
    csv_in = "../csv/global_ticker.csv"
    no, few = process_csv(csv_in)
    out_dir   = os.path.dirname(csv_in)

    pd.DataFrame(no, columns=["Ticker"]).to_csv(os.path.join(out_dir, "no_reports.csv"), index=False)

    pd.DataFrame(few, columns=["Ticker"]).to_csv(os.path.join(out_dir, "few_reports.csv"), index=False)
