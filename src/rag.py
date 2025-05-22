#chcp 65001
#Microsoft Windows [版本 10.0.26100.4061]
import os
import requests
import json
from qdrant_client import QdrantClient
from dotenv import load_dotenv
import sys
import io

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
else:
    sys.stdout = io.TextIOWrapper(
        sys.stdout.buffer, encoding="utf-8", errors="replace"
    )
if os.name == 'nt':
    os.system('chcp 65001 >nul')

# 載入環境變數
load_dotenv()
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
EMBED_API_URL = os.getenv("OLLAMA_EMBED_API_URL", "http://localhost:11434/api/embeddings")
EMBED_MODEL = "nomic-embed-text"
#LLM_MODEL = "google/gemini-2.5-pro-exp-03-25"
LLM_MODEL = "deepseek/deepseek-chat-v3-0324:free"

qdrant = QdrantClient(url=QDRANT_URL, prefer_grpc=False)

# 取得所有 collection 名稱
def get_all_collections():
    return [c.name for c in qdrant.get_collections().collections]

# 取得某公司的所有 collection（用ticker開頭比對）
def get_collections_by_company(ticker: str):
    ticker = ticker.lower()
    return [c for c in get_all_collections() if c.startswith(ticker)]

# 文字轉 embedding
def embed_query(query: str) -> list:
    resp = requests.post(
        EMBED_API_URL,
        json={"model": EMBED_MODEL, "prompt": query}
    )
    resp.raise_for_status()
    return resp.json()["embedding"]

# 查單一 collection
# def search_qdrant(collection: str, query_emb: list, top_k: int = 3):
#     hits = qdrant.query_points(
#         collection_name=collection,
#         vector=query_emb,
#         limit=top_k,
#         with_payload=True
#     ).result
#     return [hit.payload["text"] for hit in hits]

def search_qdrant(collection: str, query_emb: list, top_k: int = 3):
    hits = qdrant.search(
        collection_name=collection,
        query_vector=query_emb,
        limit=top_k,
        with_payload=True
    )
    return [hit.payload["text"] for hit in hits]

# 組 prompt 丟 LLM
def ask_llm(context: str, question: str) -> str:
    prompt = f"""你是一個財報分析助理。以下是財報片段，請根據內容詳細回答使用者問題。\n\n[財報內容]\n{context}\n\n[使用者問題]\n{question}\n\n[回答]"""
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "HTTP-Referer": "https://example.com/",
        "X-Title": "RAG-財報助手"
    }
    data = {
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": "你是財報分析助理，請根據財報片段作答。"},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.3,
        "max_tokens": 10000,
        "transforms": ["middle-out"]
    }
    resp = requests.post("https://openrouter.ai/api/v1/chat/completions", headers=headers, json=data)
    if resp.status_code != 200:
        print("▶︎ OpenRouter API Error:", resp.status_code)
        print(resp.text)   # 查看伺服器回傳的錯誤訊息 JSON
        return ""
    # 只取 content，並確保回傳純文字
    return resp.json()["choices"][0]["message"]["content"]

# 主查詢函式：可選全部、某公司、單一 collection
def rag_ask_multi(query_mode, question, per_collection_k=2, max_chunks=8):
    query_emb = embed_query(question)
    if query_mode == "all":
        collections = get_all_collections()
    elif query_mode.startswith("company:"):
        ticker = query_mode.split(":", 1)[1].lower()
        collections = get_collections_by_company(ticker)
    else:
        # query_mode 直接是 collection name
        collections = [query_mode]

    all_chunks = []
    for col in collections:
        chunks = search_qdrant(col, query_emb, top_k=per_collection_k)
        all_chunks.extend(chunks)
    # 取最前面 max_chunks 個（你可以根據需要調整）
    context = "\n---\n".join(all_chunks[:max_chunks])
    return ask_llm(context, question)

if __name__ == "__main__":
    print("==== RAG 財報查詢 ====")
    print("選擇查詢模式：")
    print("1. 輸入 collection name(例:aapl-2024-q1)查單一財報")
    print("2. 輸入 company:<公司代碼> 查詢該公司所有財報(例:company:aapl)")
    print("3. 輸入 all 查詢全部公司所有財報")
    print("--------------------------")
    query_mode = input("請輸入查詢條件：").strip()

    print("請開始輸入你的問題（輸入 exit 離開）(根據財報判斷apple公司未來的發展如何，詳細敘述理由和看法)：")
    while True:
        question = input("\n[你的問題] ")
        if question.lower() in ["exit", "quit", "q"]:
            print("已離開。")
            break
        try:
            answer = rag_ask_multi(query_mode, question, per_collection_k=2, max_chunks=8)
            #print("\n【AI回答】\n", answer.encode('utf-8', errors='replace').decode('utf-8'))
            #print(answer)
            sys.stdout.buffer.write(answer.encode("utf-8", "replace") + b"\n")
        except Exception as e:
            #print("發生錯誤：", e)
            sys.stdout.buffer.write(str(e).encode("utf-8", "replace") + b"\n")
#根據財報，判斷apple公司的未來發展如何，詳細敘述理由