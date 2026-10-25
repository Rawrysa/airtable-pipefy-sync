# Airtable ↔ Pipefy Sync

An event-driven pipeline that keeps employee and sales/project records in step between **Airtable** and **Pipefy**, using PostgreSQL as the system of record in between. I built it during my graduate programme.

## How it works

```
Airtable change ──webhook──▶ Flask listener ──▶ Middleman ──▶ PostgreSQL ──▶ Pipefy (GraphQL)
                              (listener.py)    (middleman.py)  (triggers)     (pipefy_api.py)
```

1. **Webhooks.** `AirtableAPI` registers Airtable webhooks for the employee and sales tables. Airtable notifies the Flask listener on `/employee/webhook`, `/sales/webhook` and `/test/webhook`.
2. **Cursor-based payload fetch.** For each notification the listener reads the last processed cursor from the `webhooks` table and pulls only the newer payloads, so nothing is processed twice.
3. **Middleman.** It maps Airtable field IDs to Pipefy fields, works out whether each change is a create, update or delete, and calls the matching `PipefyAPI` operation. It also has a helper that checks Airtable's `X-Airtable-Content-MAC` HMAC-SHA256 signature.
4. **PostgreSQL.** Stores raw and cleaned records plus webhook payload history. Triggers (`insert_/update_/delete_related_employee`) keep the cleaned tables consistent with the source tables.
5. **Pipefy.** An OAuth client-credentials token plus GraphQL mutations to insert, update, retitle and delete records in Pipefy database tables.

`FileCleaner` handles the one-off initial load: it normalises field values and loads existing records before webhooks take over.

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate          # macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env            # fill in Airtable, Pipefy and database settings
cd scripts
python listener.py              # Flask on :5000
```

Airtable must be able to reach the listener over HTTPS. For local development, expose port 5000 with a tunnel such as ngrok and set `WEBHOOK_BASE_URL` to the tunnel URL before creating the webhooks.

## Modules

| File | Purpose |
| --- | --- |
| `listener.py` | Flask webhook endpoints |
| `middleman.py` | Payload processing, field mapping and sync decisions |
| `airtable_api.py` | Reading Airtable records and creating webhooks (`pyairtable`) |
| `pipefy_api.py` | Pipefy OAuth and GraphQL CRUD |
| `database.py` | PostgreSQL schema, triggers and record operations (`psycopg2`) |
| `filecleaner.py` | Data cleaning and normalisation for the initial load (`pandas`) |

## Stack

Python · Flask · PostgreSQL · Airtable API · Pipefy GraphQL API · pandas
