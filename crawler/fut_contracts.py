from __future__ import annotations
import re, requests, argparse, logging, pprint, sys
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from pymongo import UpdateOne
from utils.db import get_col

LOG      = logging.getLogger(__name__)
URL      = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"
HEADERS  = {"User-Agent": "Mozilla/5.0"}

# 資料庫集合
COL = get_col("fut_contracts")
COL.create_index([("product",1),("date",1)], unique=True)

TARGETS = {"小型臺指期貨": "mtx", "微型臺指期貨": "imtx"}
IDF_SET = {"自營商", "投信", "外資"}

# ───────── helpers ─────────
def _clean_int(txt: str) -> int:
    """處理數字格式，去除非數字字符"""
    return int(re.sub(r"[^\d\-]", "", txt or "0") or 0)

def _row_net(cells) -> int:
    """取『未平倉多空淨額‑口數』：行長可能 15,14,13 → index 13 / 12 / 11"""
    if len(cells) < 12:
        raise ValueError("too few columns")
    return _clean_int(cells[-2].get_text())

def _row_idf(cells) -> str | None:
    """寬容比對身份別"""
    for c in cells:
        t = c.get_text(strip=True)
        for key in IDF_SET:
            if key in t:              # 使用 in 而非 ==
                return key
    return None

# ───────── parser ─────────
def parse(html: str) -> list[dict]:
    """解析 HTML，抓取資料並返回格式化的字典列表"""
    m = re.search(r"日期(\d{4}/\d{2}/\d{2})", html)
    if not m:
        raise RuntimeError("找不到日期")
    date_obj = datetime.strptime(m.group(1), "%Y/%m/%d").replace(tzinfo=timezone.utc)

    soup  = BeautifulSoup(html, "lxml")
    rows  = soup.find_all("tr", class_="12bk")
    if not rows:
        raise RuntimeError("tbody 無 tr.12bk 列")

    result: dict[str, dict] = {}
    current_product = None

    for tr in rows:
        cells = tr.find_all("td")
        if len(cells) < 3:
            continue

        prod_txt = cells[1].get_text(strip=True)
        if prod_txt:
            current_product = prod_txt

        if current_product not in TARGETS:
            continue

        idf = _row_idf(cells)
        if idf is None:
            continue

        try:
            net = _row_net(cells)
        except ValueError:
            continue

        entry = result.setdefault(
            current_product,
            {"prop_net": 0, "itf_net": 0, "foreign_net": 0}
        )
        if idf == "自營商":
            entry["prop_net"] = net
        elif idf == "投信":
            entry["itf_net"]  = net
        else:
            entry["foreign_net"] = net

    docs = []
    for pname, v in result.items():
        docs.append({
            "date": date_obj,
            "product": TARGETS[pname],
            **v,
            "retail_net": -(v["prop_net"] + v["itf_net"] + v["foreign_net"]),
        })
    return docs

# ───────── fetch / util ─────────
def _is_weekend() -> bool:
    """判斷是否是週末"""
    return datetime.now().weekday() >= 5      # Sat / Sun

def fetch(force=False):
    """抓取資料，若為週末可加 --force 來強制抓取"""
    if _is_weekend() and not force:
        raise RuntimeError("週末不抓 (加 --force)")

    res = requests.get(URL, headers=HEADERS, timeout=20)
    res.raise_for_status()
    docs = parse(res.text)
    if not docs:
        raise RuntimeError("未取得任何資料")

    # 更新資料庫
    COL.bulk_write([
        UpdateOne({"product": d["product"], "date": d["date"]},
                  {"$set": d}, upsert=True)
        for d in docs
    ], ordered=False)
    LOG.info("upsert %d docs OK", len(docs))
    return docs

def latest(product: str|None=None):
    """從資料庫取得最新資料"""
    query = {"product": product} if product else {}
    return COL.find_one(query, {"_id": 0}, sort=[("date", -1)])

# ───────── CLI ─────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["run"])
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    if args.cmd == "run":
        try:
            pprint.pp(fetch(args.force))
        except Exception as e:
            LOG.error("crawler error: %s", e)
            sys.exit(1)
