# `oracle_gap_check` — Oracle vs. Bronze Data-Quality DAG

File: [`airflow/dags/oracle_gap_check.py`](../airflow/dags/oracle_gap_check.py)

## Purpose

Daily data-quality check comparing per-business-date row counts between
Oracle's raw source tables and their parsed Iceberg `bronze` counterparts, to
catch any date where rows exist in Oracle but never made it into bronze.
This matters specifically because the bank's core system (T24) has a short
retention window and purges old rows from Oracle on its own schedule — once
a row is gone from Oracle, the parsed copy in bronze may be the only one
that will ever exist. A missed date caught late may be unrecoverable.

## How it works

Two tasks: `check_gaps` → `alert_gaps`. The gap list is passed between them
as the task's return value (XCom), which is also what orders them.

### Task 1: `check_gaps`

1. `DATASETS` is a fixed list mapping, per table: the Oracle table name, its
   XML date tag (e.g. `c167`), the corresponding bronze table, and what that
   date tag is named once parsed (e.g. `date_last_update`).
2. For each selected dataset (`params.datasets`: `"all"` or a space-separated
   subset of names):
   - `oracle_counts_sql()` builds a Trino passthrough query
     (`oracle.system.query`) that runs an `XMLQUERY`/`XMLCAST` extraction of
     the business date **inside Oracle itself**, grouped and counted per
     date.
   - `iceberg_counts_sql()` runs the equivalent grouped count against the
     already-parsed bronze column.
   - Both optionally take a `(start_date, end_date)` window (see Parameters)
     to narrow the comparison to a date range instead of the whole history.
3. `diff_counts()` compares the two per-date count maps and flags every date
   where **Oracle's count is strictly greater than bronze's** — rows exist in
   the source that are missing from bronze. (See Limitation 2 — the reverse
   case is not flagged.)
4. Every gap is logged per dataset/date, and the full list is returned.
   `check_gaps` does **not** fail on gaps — it only fails when the check
   itself can't run (Trino/Oracle down, unknown dataset, bad date params).

### Task 2: `alert_gaps`

- No gaps → logs "no gaps found" and succeeds. No email.
- Gaps → emails them to every address in `GAP_ALERT_EMAIL_TO` and succeeds.
  - Subject: `[oracle_gap_check] N date(s) missing from bronze`
  - Body: the run id, then one line per gap, e.g.
    `account 20260101: oracle=5 bronze=3 (missing 2)`. At most 50 lines;
    the rest are summarised as "… and N more (see task log)" and are in
    `check_gaps`' log.
- `GAP_ALERT_EMAIL_TO` empty → **fails** with the gap list instead. Oracle
  purges old rows, so a gap nobody sees can become permanent loss; a green
  run with no alert would be worse than a red one.

### What green and red mean

| Run state | Meaning |
|---|---|
| Green, no email | No gaps. |
| Green, email sent | Gaps found and alerted — act on the email. |
| Red `check_gaps` | The check itself broke — no gap result at all. |
| Red `alert_gaps` | Gaps found but the email couldn't be sent (SMTP down, wrong/revoked password, or `GAP_ALERT_EMAIL_TO` unset) — the gap list is in the error. |

Separating the two tasks is also why a failed email only retries
`alert_gaps` (`retries: 1`): the expensive Oracle scan isn't re-run just to
resend an email.

## Parameters

| Param | Default | Meaning |
|---|---|---|
| `datasets` | `"all"` | `"all"`, or a space-separated subset of dataset names (`account customer ...`). |
| `start_date` / `end_date` | `""` / `""` | `YYYYMMDD`, both optional but must be given **together**. Empty = full history (every date ever seen on either side). |

## Email configuration

Set in `.env` (see `.env.example`), passed to every Airflow container by
`docker-compose.yml`. After changing any of them, recreate the containers —
a plain restart doesn't reload `.env`:
`docker compose up -d --force-recreate airflow-scheduler airflow-apiserver airflow-dag-processor`

| Variable | Example | Meaning |
|---|---|---|
| `GAP_ALERT_EMAIL_TO` | `a@bank.com b@bank.com` | Recipients, **space-separated** (not commas). All appear in the same To: line. Empty = fail on gaps instead of emailing. |
| `SMTP_HOST` | `smtp.gmail.com` | Mail server that sends the email. |
| `SMTP_PORT` | `587` | STARTTLS port. |
| `SMTP_USER` | `alerts@gmail.com` | Account that logs in to the mail server — **always also the sender (From)**. Not configurable separately, so alerts can't be sent under another name. |
| `SMTP_PASSWORD` | — | For Gmail, an **App Password** (needs 2-Step Verification on the account), not the account password. |

`SMTP_HOST` depends on whose account `SMTP_USER` is: `smtp.gmail.com` for
Gmail, `smtp.office365.com` for Microsoft 365, or the bank's internal mail
relay (the usual choice for production — ask IT for host, port and a service
account). The code needs a server that supports STARTTLS and a login; a relay
that accepts mail without login would need a small change.

No Airflow provider package is used — `smtplib` from the standard library.

## Limitations, and the fix for each, before production

### 1. This only catches missing rows — not wrong data

A row that parsed incorrectly (a field-mapping bug, a wrong value) but still
landed in bronze counts the same on both sides and will never be flagged.
This check validates *presence/count* per date, not *content*.

**Fix, if content-level assurance matters:** a separate reconciliation pass
(checksums, or a spot comparison of specific fields) — out of scope for what
this DAG is designed to do.

### 2. Real race with Oracle's own purge schedule

Because T24/Oracle purges old rows on its own retention schedule, if this
DAG's daily run happens to execute **after** that day's purge but the
corresponding ingestion run hasn't caught up yet (or failed silently
upstream), the "missing" rows are already gone from Oracle's side too — the
counts look reconciled even though data was genuinely lost before this check
ever saw it.

**Fix:** schedule this DAG with enough margin after the ingestion job's own
daily run that ingestion is guaranteed complete first. If ingestion is ever
wired into Airflow itself, an explicit `ExternalTaskSensor`/DAG dependency
would remove the guesswork entirely.

### 3. Email is the only alert channel

If the mail server is down or the password is revoked, `alert_gaps` goes red
after its retry — the gaps aren't lost, but someone has to notice the red
run. Nothing else (chat, paging) is notified.

**Fix, if needed:** an Airflow failure callback or monitoring on red runs of
this DAG, or a second channel next to the email. A personal Gmail with an
App Password is fine for testing; production should use the bank's mail
relay and a service account.

### 4. No coordination needed with concurrent writes — lower risk than `compact_iceberg` by design

This DAG only reads. Iceberg's snapshot isolation makes that safe regardless
of concurrent writes or compaction happening on the same tables — it never
commits anything to Iceberg itself, so it doesn't carry the commit-conflict
risk documented for `compact_iceberg`.

### 5. `start_date`/`end_date` must be given together, by design

Giving only one raises `"start_date and end_date must be given together"`
rather than silently defaulting the other — so a partial param can't quietly
narrow the check in an unintended way. Worth knowing so it isn't mistaken for
a bug if someone hits it.
