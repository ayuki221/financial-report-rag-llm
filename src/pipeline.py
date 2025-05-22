#!/usr/bin/env python3
import os
import sys
import json
import argparse
import requests
import psycopg2
import uuid
from psycopg2 import sql
from dotenv import load_dotenv
from qdrant_client import QdrantClient
from qdrant_client.http.models import VectorParams, Distance, PointStruct

load_dotenv()

DB_PARAMS = {
    'host':   os.getenv('DB_HOST'),
    'port':   os.getenv('DB_PORT'),
    'dbname': os.getenv('DB_NAME'),
    'user':   os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
}

OLLAMA_URL = 'http://localhost:11434'
QDRANT_URL = 'http://localhost:6333'

qdrant = QdrantClient(url=QDRANT_URL, prefer_grpc=False)

# 列出所有 ticker table
def list_ticker_tables() -> list[str]:
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type = 'BASE TABLE';
    """)
    tables = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return tables

# ETL：Extract JSONB → 可讀文本
def extract_reports(ticker: str):
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    try:
        # 用 psycopg2.sql.Identifier 安全引用 table 名，會自動加上雙引號
        query = sql.SQL("SELECT report, facts FROM {}").format(
            sql.Identifier(ticker)
        )
        cur.execute(query)
        for report, facts_jsonb in cur.fetchall():
            facts = facts_jsonb
            lines = [f"Report: {report}"]
            for tag, props in facts.items():
                val  = props.get('value', '')
                unit = props.get('unitRef') or ''
                lines.append(f"{tag}: {val} {unit}".strip())
            yield report, "\n".join(lines)
    finally:
        cur.close()
        conn.close()

# Chunking + Embedding → Upsert Qdrant
def chunk_text(text: str) -> list[str]:
    return [text]

def embed(texts: list[str]) -> list[list[float]]:
    embeddings = []
    for text in texts:
        resp = requests.post(
            f"{OLLAMA_URL}/api/embeddings",
            json={
                #"model": "llama3:latest",
                "model": "nomic-embed-text",
                "prompt": text
            }
        )
        resp.raise_for_status()
        # Ollama 回傳的欄位是 "embedding"
        embeddings.append(resp.json()["embedding"])
    return embeddings

def ensure_collection(name: str, vector_size: int, reset: bool=False):
    # 若要重置，先刪除已存在的 collection
    if reset and qdrant.collection_exists(collection_name=name):
        qdrant.delete_collection(collection_name=name)
    # 若 collection 不存在，才建立新 collection
    if not qdrant.collection_exists(collection_name=name):
        qdrant.create_collection(
            collection_name=name,
            vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE),
        )

def upsert_chunks(ticker: str, reset: bool=False):
    for report, text in extract_reports(ticker):
        collection_name = report.lower()
        created = False

        for idx, chunk in enumerate(chunk_text(text)):
            # 第一次 embed 時建立 collection
            if not created:
                emb0 = embed([chunk])[0]
                ensure_collection(collection_name, len(emb0), reset=reset)
                created = True

            emb = embed([chunk])[0]

            # 用 UUID v5 產生合法且可重現的 point ID
            raw_id = f"{ticker}_{report}_{idx}"
            point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, raw_id))

            # 用 retrieve() 取代舊的 get() 方法
            existing_points = qdrant.retrieve(
                collection_name=collection_name,
                ids=[point_id],
                with_payload=False,
                with_vectors=False
            )
            if existing_points:
                print(f"• {report} (UUID: {point_id}) exist,skip")
                continue

            # 組裝並上傳新點
            payload = {
                "ticker": ticker,
                "report": report,
                "chunk_index": idx,
                "text": chunk
            }
            point = PointStruct(id=point_id, vector=emb, payload=payload)
            qdrant.upsert(collection_name=collection_name, points=[point])
            print(f"• {report} (UUID: {point_id}) upload")

def main():
    parser = argparse.ArgumentParser(description="ETL + Embedding Pipeline")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # extract
    p1 = sub.add_parser("extract", help="從 DB 拆 JSONB，顯示可讀文本")
    p1.add_argument("--all", action="store_true",
        help="對所有 ticker table 都執行 extract")
    p1.add_argument("ticker", nargs="?",
        help="指定單一 ticker，例如 AAPL")

    # upsert
    p2 = sub.add_parser("upsert", help="chunk→embed→寫入 Qdrant")
    p2.add_argument("--all", action="store_true",
        help="對所有 ticker table 都執行 upsert")
    p2.add_argument("--reset", action="store_true",
        help="先清除舊的向量資料（刪除 collection）再上傳")
    p2.add_argument("ticker", nargs="?",
        help="指定單一 ticker，例如 AAPL")

    args = parser.parse_args()

    if args.cmd == "extract" or args.cmd == "upsert":
        if args.all:
            tickers = list_ticker_tables()
        elif args.ticker:
            tickers = [args.ticker.lower()]
        else:
            print("請指定 --all 或 ticker，例如：")
            print("  python pipeline.py upsert --all --reset")
            sys.exit(1)

        for tk in tickers:
            if args.cmd == "extract":
                print(f"\n=== Extract {tk} ===")
                for report, text in extract_reports(tk):
                    print(f"\n--- {report} ---\n{text}\n")
            else:
                upsert_chunks(tk, reset=args.reset)

if __name__ == "__main__":
    main()