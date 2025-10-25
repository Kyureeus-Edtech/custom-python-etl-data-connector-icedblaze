#!/usr/bin/env python3
"""
Software Architecture Assignment — Custom Python ETL Data Connector
Topic: CZDS (ICANN) – Mock Implementation
Author: icedblaze
"""

import os
import argparse
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List
import requests
from dotenv import load_dotenv
from pymongo import MongoClient, InsertOne, UpdateOne

ISO8601 = "%Y-%m-%dT%H:%M:%SZ"


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime(ISO8601)


class CZDSConnector:
    """Mock CZDS ETL connector with 3 endpoints (zones, downloads, users)."""

    def __init__(self):
        load_dotenv()
        self.base_url = os.getenv("API_BASE_URL", "https://mock.icann.czds.local").rstrip("/")
        self.mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
        self.mongo_db = os.getenv("MONGO_DB", "ssn_connectors")
        self.connector_name = os.getenv("CONNECTOR_NAME", "czds")

        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    # ---------- EXTRACT ----------
    def fetch_endpoint(self, endpoint: str) -> List[Dict[str, Any]]:
        """Fetch data from one mock endpoint."""
        url = f"{self.base_url}/{endpoint}"
        print(f"[INFO] Fetching from {url} ...")
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list):
                raise ValueError("Expected list response")
            return data
        except Exception as e:
            print(f"[WARN] Failed to fetch {endpoint}: {e}")
            return []

    def fetch_all(self) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch data from all three endpoints."""
        return {
            "zones": self.fetch_endpoint("zones"),
            "downloads": self.fetch_endpoint("downloads"),
            "users": self.fetch_endpoint("users"),
        }

    # ---------- TRANSFORM ----------
    def transform_record(self, record: Dict[str, Any], source: str) -> Dict[str, Any]:
        doc = dict(record)
        doc["_source"] = source
        doc["_ingested_at"] = iso_now()
        doc["_connector"] = self.connector_name
        if "id" in doc:
            doc["_id"] = f"{source}_{doc['id']}"
        return doc

    def transform(self, payloads: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        transformed = {}
        for src, records in payloads.items():
            transformed[src] = [self.transform_record(r, src) for r in records]
        return transformed

    # ---------- LOAD ----------
    def load_to_mongo(self, dataset: Dict[str, List[Dict[str, Any]]], upsert: bool = True) -> int:
        total = 0
        for src, docs in dataset.items():
            coll_name = f"{self.connector_name}_{src}_raw"
            coll = self.db[coll_name]
            ops = []
            for d in docs:
                if upsert and "_id" in d:
                    ops.append(UpdateOne({"_id": d["_id"]}, {"$set": d}, upsert=True))
                else:
                    ops.append(InsertOne(d))
            if ops:
                res = coll.bulk_write(ops, ordered=False)
                count = res.upserted_count + res.modified_count + res.inserted_count
                total += count
                print(f"[INFO] {src}: inserted/updated {count} docs into {coll_name}")
        return total


def main():
    parser = argparse.ArgumentParser(description="CZDS Mock ETL Connector")
    parser.add_argument("--dry-run", action="store_true", help="Run without writing to MongoDB")
    args = parser.parse_args()

    connector = CZDSConnector()
    data = connector.fetch_all()
    transformed = connector.transform(data)

    total_records = sum(len(v) for v in transformed.values())
    print(f"[INFO] Transformed {total_records} total records from all endpoints.")

    if args.dry_run:
        for k, v in transformed.items():
            print(f"[DRY RUN] {k}: showing first 2 records →")
            print(v[:2])
        return

    inserted = connector.load_to_mongo(transformed)
    print(f"[INFO] ETL completed: {inserted} total records written.")


if __name__ == "__main__":
    main()
