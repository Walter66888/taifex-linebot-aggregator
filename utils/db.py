"""
utils/db.py
-----------
統一管理 MongoDB 連線與集合索引。
"""
import os
from pymongo import MongoClient, ASCENDING

def _client() -> MongoClient:
    uri = os.getenv("MONGODB_URI")
    if not uri:
        raise RuntimeError("環境變數 MONGODB_URI 未設定")
    return MongoClient(uri, tz_aware=True)

def get_col(name: str):
    db_name = os.getenv("MONGODB_DB", "taifex")
    col = _client()[db_name][name]
    # 建立唯一索引（只做一次）
    if "date_1" not in col.index_information():
        col.create_index([("date", ASCENDING)], unique=True)
    return col
