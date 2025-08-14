#!/usr/bin/env python3
"""
Software Architecture Assignment — Custom Python ETL Data Connector
Example: ThreatFox JSON Feed
"""
import os
import argparse
from datetime import datetime, timezone
from typing import Any, Dict, Generator, Iterable, List

import requests
from dotenv import load_dotenv
from pymongo import MongoClient, InsertOne, UpdateOne
from pymongo.errors import PyMongoError

ISO8601 = "%Y-%m-%dT%H:%M:%SZ"


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime(ISO8601)


class Connector:
    def __init__(self):
        load_dotenv()
        self.base_url = os.getenv("API_BASE_URL", "").rstrip("/")
        self.connector_name = os.getenv("CONNECTOR_NAME", "connector")
        self.mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
        self.mongo_db = os.getenv("MONGO_DB", "ssn_connectors")
        self.collection_name = f"{self.connector_name}_raw"

        if not self.base_url:
            raise ValueError("API_BASE_URL is required in .env")

    # ---------- EXTRACT ----------
    def fetch(self) -> Generator[Dict[str, Any], None, None]:
        url = f"{self.base_url}/export/json/threatfox_abuse_ch.json"
        try:
            resp = requests.get(url, timeout=30)
        except requests.RequestException as e:
            raise RuntimeError(f"Network error: {e}") from e

        if resp.status_code != 200:
            raise RuntimeError(f"Bad status: {resp.status_code} | {resp.text[:500]}")

        data = resp.json()
        if not isinstance(data, list):
            raise RuntimeError("Expected a JSON array from ThreatFox")

        for item in data:
            yield item

    # ---------- TRANSFORM ----------
    def transform_record(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        doc = dict(raw)
        # Use IOC value as unique ID
        if "ioc_value" in doc:
            doc["_id"] = doc["ioc_value"]
        doc["_ingested_at"] = iso_now()
        doc["_connector"] = self.connector_name
        return doc

    def transform(self, records: Iterable[Dict[str, Any]]):
        for r in records:
            yield self.transform_record(r)

    # ---------- LOAD ----------
    def get_collection(self):
        client = MongoClient(self.mongo_uri)
        db = client[self.mongo_db]
        return db[self.collection_name]

    def load(self, docs: Iterable[Dict[str, Any]], upsert: bool = True) -> int:
        coll = self.get_collection()
        ops = []
        for d in docs:
            if upsert and "_id" in d:
                ops.append(UpdateOne({"_id": d["_id"]}, {"$set": d}, upsert=True))
            else:
                ops.append(InsertOne(d))
        if ops:
            res = coll.bulk_write(ops, ordered=False)
            return res.upserted_count + res.modified_count + res.inserted_count
        return 0


def main():
    parser = argparse.ArgumentParser(description="ThreatFox ETL Connector")
    parser.add_argument("--dry-run", action="store_true", help="Run without inserting into MongoDB")
    args = parser.parse_args()

    c = Connector()
    print("[INFO] Fetching ThreatFox feed...")
    raw_iter = c.fetch()
    transformed = list(c.transform(raw_iter))

    if args.dry_run:
        print(f"[INFO] Dry run mode - fetched {len(transformed)} records.")
        print(transformed[:3])  # preview first 3
        return

    count = c.load(transformed)
    print(f"[INFO] Inserted/updated {count} documents into MongoDB.")


if __name__ == "__main__":
    main()
