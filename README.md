# SSN Software Architecture — Custom Python ETL Data Connector (Starter)

This is a **starter template** for the SSN CSE × Kyureeus EdTech assignment.
Fork/clone this into **your own branch/folder** and implement the ETL logic for the API you chose.

> ✅ Remember: **Do not commit secrets**. Use a local `.env` file for keys/tokens.

---

## Project Structure
```
/your-branch-name/
├── etl_connector.py        # Your main ETL script (edit here)
├── .env                    # Your secrets (NOT committed)
├── requirements.txt        # Python dependencies
├── README.md               # Describe your connector + usage
└── .gitignore              # Prevents secrets and junk from being committed
```

## Quickstart

1) Create & activate a virtual environment
```bash
python -m venv .venv
# Windows
.\.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate
```

2) Install dependencies
```bash
pip install -r requirements.txt
```

3) Create a **.env** (example below) and fill in your values

```
API_BASE_URL=https://api.example.com/v1
API_KEY=your_key_here
API_TOKEN=your_token_here
MONGO_URI=mongodb://localhost:27017
MONGO_DB=ssn_connectors
CONNECTOR_NAME=exampleapi
PAGE_SIZE=100
```

4) Run the ETL
```bash
python etl_connector.py --since "2025-01-01T00:00:00Z" --limit 2000
```

### CLI options
- `--since` ISO8601 timestamp to fetch incremental data from (optional)
- `--limit` maximum number of records to ingest in this run (optional)
- `--dry-run` run extraction+transform **without** writing to MongoDB
- `--upsert` upsert by `_id` if present in transformed docs

---

## What to Edit

- **Extraction**: update `build_request()` + `parse_page()` to match your API.
- **Pagination**: update `get_next_page_params()` to follow your API’s pagination style (cursor/offset/page).
- **Transformation**: edit `transform_record()` to normalize each item for MongoDB.
- **Loading**: choose collection name; default: `<CONNECTOR_NAME>_raw`. Timestamps are auto-added.

---

## Submission Checklist

- [ ] Read and understand your API docs (auth, headers, pagination, rate limits)
- [ ] Secure credentials via `.env`
- [ ] Implement Extract → Transform → Load
- [ ] Handle errors, empty payloads, rate limits, connectivity issues
- [ ] Validate MongoDB inserts locally
- [ ] Write a descriptive `README.md` for your connector (endpoints, params, sample output)
- [ ] Use clear commit messages (include **your name** and **roll number**)
- [ ] Push to your branch and open a Pull Request

---

## Helpful Links

- python-dotenv (PyPI)
- PyMongo Docs
- Requests Docs

**Happy coding! 🚀**