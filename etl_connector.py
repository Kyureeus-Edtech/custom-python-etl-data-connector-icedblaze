#!/usr/bin/env python3
"""
Software Architecture Assignment — Custom Python ETL Data Connector (Template)

Edit the sections marked TODO to match your chosen API.

Run:
    python etl_connector.py --since "2025-01-01T00:00:00Z" --limit 2000 --dry-run
"""
import os
import time
import argparse
from datetime import datetime, timezone
from typing import Any, Dict, Generator, Iterable, List, Optional, Tuple

import requests
from requests import Response
from dotenv import load_dotenv
from pymongo import MongoClient, InsertOne, UpdateOne
from pymongo.errors import PyMongoError
from tqdm import tqdm

ISO8601 = "%Y-%m-%dT%H:%M:%SZ"  # Basic Zulu format


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime(ISO8601)


class RateLimiter:
    """Simple rate limiter using sleep between requests or header hints."""
    def __init__(self, min_interval_sec: float = 0.0):
        self.min_interval = min_interval_sec
        self._last = 0.0

    def wait(self, resp: Optional[Response] = None) -> None:
        # Respect explicit header-based limits if present
        if resp is not None:
            rl_reset = resp.headers.get("X-RateLimit-Reset")
            rl_remaining = resp.headers.get("X-RateLimit-Remaining")
            if rl_remaining == "0" and rl_reset:
                try:
                    reset_epoch = int(rl_reset)
                    sleep_for = max(0, reset_epoch - int(time.time()))
                    if sleep_for > 0:
                        time.sleep(sleep_for)
                        self._last = time.time()
                        return
                except ValueError:
                    pass

        # Fallback: simple interval
        elapsed = time.time() - self._last
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last = time.time()


class Connector:
    def __init__(self):
        load_dotenv()

        # --- ENV (edit your keys/values in .env) ---
        self.base_url = os.getenv("API_BASE_URL", "").rstrip("/")
        self.api_key = os.getenv("API_KEY", "")
        self.api_token = os.getenv("API_TOKEN", "")
        self.connector_name = os.getenv("CONNECTOR_NAME", "connector")
        self.page_size = int(os.getenv("PAGE_SIZE", "100"))

        # Mongo
        self.mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
        self.mongo_db = os.getenv("MONGO_DB", "ssn_connectors")
        self.collection_name = f"{self.connector_name}_raw"

        # Simple rate limiter: adjust if your API needs it
        self.ratelimiter = RateLimiter(min_interval_sec=0.2)

        if not self.base_url:
            raise ValueError("API_BASE_URL is required in .env")

    # ---------- EXTRACT ----------
    def build_request(self, params: Dict[str, Any]) -> Tuple[str, Dict[str, str], Dict[str, Any]]:
        """
        TODO: Adjust endpoint path, headers, and query params to your API.
        Returns: (url, headers, query_params)
        """
        endpoint = "/items"  # e.g., "/users", "/posts", etc.
        url = f"{self.base_url}{endpoint}"
        headers = {
            "Accept": "application/json",
            # Common patterns:
            # "Authorization": f"Bearer {self.api_token}",
            # "X-API-Key": self.api_key,
        }
        query = {
            "limit": self.page_size,
            **params,
        }
        return url, headers, query

    def parse_page(self, resp: Response) -> Tuple[List[Dict[str, Any]], bool, Dict[str, Any]]:
        """
        TODO: Map this to match your API's JSON structure.
        Must return: (records, has_more, next_params)
        """
        data = resp.json()
        # Example structures (adjust as needed):
        items = data.get("items") or data.get("data") or []
        has_more = bool(data.get("has_more"))

        # Cursor-based example:
        next_cursor = data.get("next_cursor")
        next_params = {"cursor": next_cursor} if next_cursor else {}

        # Offset/page-based example (uncomment and adapt):
        # page = data.get("page", 1)
        # total_pages = data.get("total_pages", 1)
        # has_more = page < total_pages
        # next_params = {"page": page + 1}

        return items, has_more, next_params

    def get_next_page_params(self, current_params: Dict[str, Any], next_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        TODO: Adapt for your API. This merges current params with server-provided cursors/offsets.
        """
        merged = {**current_params, **next_params}
        return {k: v for k, v in merged.items() if v is not None}

    def fetch(self, initial_params: Dict[str, Any]) -> Generator[Dict[str, Any], None, None]:
        params = dict(initial_params)
        while True:
            url, headers, query = self.build_request(params)
            try:
                resp = requests.get(url, headers=headers, params=query, timeout=30)
            except requests.RequestException as e:
                raise RuntimeError(f"Network error: {e}") from e

            if resp.status_code == 429:
                # Rate limit — sleep and retry
                self.ratelimiter.wait(resp)
                continue

            if resp.status_code >= 500:
                # Transient server error — brief backoff + retry
                time.sleep(2.0)
                continue

            if resp.status_code != 200:
                raise RuntimeError(f"Bad status: {resp.status_code} | Body: {resp.text[:2000]}")

            try:
                items, has_more, next_params = self.parse_page(resp)
            except ValueError:
                raise RuntimeError("Response was not JSON. Check endpoint or auth.")

            if not isinstance(items, list):
                raise RuntimeError("Expected a list of items from API. Check parse_page().")

            for it in items:
                yield it

            if not has_more or not next_params:
                break

            params = self.get_next_page_params(params, next_params)
            self.ratelimiter.wait(resp)

    # ---------- TRANSFORM ----------
    def transform_record(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """
        TODO: Normalize/clean a single raw item into a MongoDB-ready document.
        Must return a dict.
        """
        doc = dict(raw)  # shallow copy
        # Example: rename id -> _id if unique
        if "id" in doc and "_id" not in doc:
            doc["_id"] = str(doc["id"])
        # Add ingestion metadata
        doc["_ingested_at"] = iso_now()
        doc["_connector"] = self.connector_name
        return doc

    def transform(self, records: Iterable[Dict[str, Any]]):
        for r in records:
            try:
                yield self.transform_record(r)
            except Exception as e:
                # Skip or log malformed items
                print(f"[WARN] transform error on record: {str(e)}")

    # ---------- LOAD ----------
    def get_collection(self):
        client = MongoClient(self.mongo_uri, uuidRepresentation="standard")
        db = client[self.mongo_db]
        return db[self.collection_name]

    def load(self, docs: Iterable[Dict[str, Any]], upsert: bool = False, batch_size: int = 500) -> int:
        coll = self.get_collection()
        batch = []
        written = 0
        for d in docs:
            if upsert and "_id" in d:
                batch.append(UpdateOne({"_id": d["_id"]}, {"$set": d}, upsert=True))
            else:
                batch.append(InsertOne(d))

            if len(batch) >= batch_size:
                written += self._flush(coll, batch)
                batch = []

        if batch:
            written += self._flush(coll, batch)

        return written

    def _flush(self, coll, ops):
        try:
            res = coll.bulk_write(ops, ordered=False)
            return (res.inserted_count or 0) + (res.upserted_count or 0) + (res.modified_count or 0)
        except PyMongoError as e:
            print(f"[ERROR] bulk_write failed: {e}")
            return 0


def main():
    parser = argparse.ArgumentParser(description="Custom Python ETL Data Connector (Template)")
    parser.add_argument("--since", type=str, help="ISO8601 timestamp for incremental fetch (optional)")
    parser.add_argument("--limit", type=int, default=None, help="Max records to ingest this run")
    parser.add_argument("--dry-run", action="store_true", help="Extract+Transform without writing to MongoDB")
    parser.add_argument("--upsert", action="store_true", help="Upsert by _id if present in transformed docs")
    args = parser.parse_args()

    c = Connector()

    params = {}
    if args.since:
        # Typical filter param names: "since", "updated_after", etc. Adjust in build_request().
        params["since"] = args.since

    print(f"[INFO] Starting run at {iso_now()} | connector={c.connector_name}")
    print(f"[INFO] Target collection: {c.mongo_db}.{c.collection_name} | dry_run={args.dry_run} upsert={args.upsert}")

    raw_iter = c.fetch(params)
    transformed = c.transform(raw_iter)

    to_load = []
    for i, doc in enumerate(transformed, start=1):
        to_load.append(doc)
        if args.limit and i >= args.limit:
            break

    if args.dry_run:
        print(f"[INFO] DRY RUN — would process {len(to_load)} records.")
        return 0

    written = c.load(to_load, upsert=args.upsert)
    print(f"[INFO] Wrote {written} documents to MongoDB.")
    print(f"[INFO] Completed at {iso_now()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
