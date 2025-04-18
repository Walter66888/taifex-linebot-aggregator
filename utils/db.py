"""
utils/db.py  (fixed)
--------------------
集中管理 Mongo 連線 + 每個集合的索引

集合               索引
-----------------------------------------------
pc_ratio           date 唯一
fut_contracts      (date, product) 複合唯一
"""

import os, functools, logging
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError

_MONGO_URI = os.getenv("MONGODB_URI")
_DB_NAME   = os.getenv("MONGODB_DB", "taifex")

@functools.lru_cache
def _client():
    if not _MONGO_URI:
        raise RuntimeError("環境變數 MONGODB_URI 未設定")
    return MongoClient(_MONGO_URI)

def _safe_create(col, keys, **opts):
    """若索引已存在或衝突，直接略過"""
    try:
        col.create_index(keys, **opts)
    except DuplicateKeyError as e:
        logging.warning(f"[index] skip duplicate index: {e.details.get('errmsg')}")
    except Exception as e:
        logging.warning(f"[index] {e}")

def get_col(name: str):
    col = _client()[_DB_NAME][name]

    if name == "pc_ratio":
        _safe_create(col, [("date", ASCENDING)], unique=True, name="date_1")
    elif name == "fut_contracts":
        # 正確索引：date + product 複合唯一
        _safe_create(col, [("date", ASCENDING), ("product", ASCENDING)],
                     unique=True, name="date_1_product_1")
    # 其他集合可在此擴充 …

    return col
