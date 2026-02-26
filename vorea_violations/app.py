from flask import Flask, render_template, jsonify, request, send_file
import requests
import io
import csv
import sqlite3
import os
import json
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta

# ─── Logging setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

try:
    from apscheduler.schedulers.background import BackgroundScheduler
    HAS_SCHEDULER = True
except ImportError:
    HAS_SCHEDULER = False

app = Flask(__name__)

# ─── NYC Open Data Dataset IDs ────────────────────────────────────────────────
DATASETS = {
    # ── DOB ECB/OATH violations (DOB-issued, adjudicated by OATH) ──────────
    "DOB ECB Violations": {
        "id": "6bgk-3dad",
        "search_field": "respondent_name",
        "search_value": "VOREA",
        "display_fields": ["ecb_violation_number", "respondent_house_number", "respondent_street",
                           "boro", "bin",
                           "issue_date", "violation_type", "violation_description",
                           "respondent_name", "ecb_violation_status", "hearing_date",
                           "severity", "penality_imposed", "amount_paid", "balance_due"],
        "label_map": {
            "ecb_violation_number": "ECB Viol #", "respondent_house_number": "House #",
            "respondent_street": "Street", "boro": "Borough", "bin": "BIN",
            "issue_date": "Issue Date", "violation_type": "Type",
            "violation_description": "Description", "respondent_name": "Respondent",
            "ecb_violation_status": "Status", "hearing_date": "Hearing Date",
            "severity": "Severity", "penality_imposed": "Penalty Imposed",
            "amount_paid": "Amount Paid", "balance_due": "Balance Due",
        },
        "address_fields": ["respondent_house_number", "respondent_street"],
        "borough_field": "boro",
        "status_field": "ecb_violation_status",
        "date_field": "issue_date",
        "id_field": "ecb_violation_number",
        "color": "#f97316", "icon": "⚖️"
    },
    # ── OATH Hearings — ALL agencies including DOT, DEP, DSNY, FDNY etc. ───
    # Actual API fields (confirmed live): ticket_number, violation_date, issuing_agency,
    # respondent_first_name, respondent_last_name, hearing_date, hearing_status,
    # penalty_imposed, paid_amount, balance_due, total_violation_amount,
    # violation_location_house, violation_location_street_name, violation_location_borough,
    # charge_1_code, charge_1_code_description, charge_1_infraction_amount
    "OATH Hearings (DOT / All Agencies)": {
        "id": "jz4z-kudi",
        "search_field": "respondent_last_name",
        "search_value": "VOREA",
        "display_fields": ["ticket_number", "respondent_last_name", "respondent_first_name",
                           "violation_location_house", "violation_location_street_name",
                           "violation_location_borough",
                           "violation_date", "issuing_agency",
                           "charge_1_code_description", "charge_1_code",
                           "hearing_status", "hearing_date",
                           "penalty_imposed", "paid_amount", "balance_due"],
        "label_map": {
            "ticket_number": "Ticket #",
            "respondent_last_name": "Last Name", "respondent_first_name": "First Name",
            "violation_location_house": "House #",
            "violation_location_street_name": "Street",
            "violation_location_borough": "Borough",
            "violation_date": "Violation Date", "issuing_agency": "Issuing Agency",
            "charge_1_code_description": "Charge Description", "charge_1_code": "Charge Code",
            "hearing_status": "Hearing Status", "hearing_date": "Hearing Date",
            "penalty_imposed": "Penalty Imposed",
            "paid_amount": "Amount Paid", "balance_due": "Balance Due",
        },
        "address_fields": ["violation_location_house", "violation_location_street_name"],
        "borough_field": "violation_location_borough",
        "status_field": "hearing_status",
        "date_field": "violation_date",
        "id_field": "ticket_number",
        "color": "#3b82f6", "icon": "🏛️", "timeout": 60
    },
    # ── DOT Street Construction Permits (2022–present) ─────────────────────
    # Actual Socrata field names confirmed from API: permitnumber, permitteename, etc.
    # NOTE: DEP Violations removed — dataset ID buex-bi6w was incorrect (points to an
    # appointments/personnel dataset). DEP violations adjudicated by OATH are already
    # captured in "OATH Hearings (DOT / All Agencies)" via the issuing_agency field.
    "DOT Street Construction Permits": {
        "id": "tqtj-sjs8",
        "search_field": "permitteename",
        "search_value": "VOREA",
        "display_fields": ["permitnumber", "permitteename", "permittypedesc",
                           "issuedworkstartdate", "issuedworkenddate",
                           "onstreetname", "fromstreetname", "tostreetname", "boroughname",
                           "permitstatusshortdesc", "equipmenttypedesc"],
        "label_map": {
            "permitnumber": "Permit #", "permitteename": "Permittee",
            "permittypedesc": "Permit Type", "equipmenttypedesc": "Equipment / Work",
            "issuedworkstartdate": "Start Date", "issuedworkenddate": "End Date",
            "onstreetname": "On Street", "fromstreetname": "From Street",
            "tostreetname": "To Street", "boroughname": "Borough",
            "permitstatusshortdesc": "Status",
        },
        "address_fields": ["permithousenumber", "onstreetname"],
        "borough_field": "boroughname",
        "status_field": "permitstatusshortdesc",
        "date_field": "issuedworkstartdate",
        "id_field": "permitnumber",
        "color": "#60a5fa", "icon": "🚧"
    },
}

BASE_URL = "https://data.cityofnewyork.us/resource"

# ─── LOCAL PROJECT DATABASE ──────────────────────────────────────────────────
# On Vercel the file system is read-only except /tmp; data is ephemeral there.
if os.environ.get('VERCEL'):
    DB_PATH = '/tmp/projects.db'
else:
    DB_PATH = os.path.join(os.path.dirname(__file__), 'projects.db')

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _migrate_v1_to_v2(conn):
    """Migrate from single-BIN (v1: projects.bin as PK) to multi-BIN (v2) schema."""
    conn.executescript('''
        ALTER TABLE projects  RENAME TO _projects_v1;
        ALTER TABLE tco_items RENAME TO _tco_items_v1;

        CREATE TABLE projects (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            project_name TEXT NOT NULL,
            address      TEXT DEFAULT '',
            borough      TEXT DEFAULT '',
            notes        TEXT DEFAULT '',
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE project_bins (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id     INTEGER NOT NULL,
            bin            TEXT NOT NULL,
            label          TEXT DEFAULT '',
            dob_job_number TEXT DEFAULT '',
            is_primary     INTEGER DEFAULT 1,
            created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(project_id, bin),
            FOREIGN KEY (project_id) REFERENCES projects(id)
        );
        CREATE TABLE tco_items (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id        INTEGER NOT NULL,
            section           TEXT NOT NULL,
            item_ref          TEXT DEFAULT '',
            description       TEXT NOT NULL,
            responsible_party TEXT DEFAULT '',
            status            TEXT DEFAULT 'open',
            notes             TEXT DEFAULT '',
            created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (project_id) REFERENCES projects(id)
        );

        INSERT INTO projects (project_name, address, borough, notes, created_at)
        SELECT project_name, address, borough, notes, created_at FROM _projects_v1;

        INSERT INTO project_bins (project_id, bin, dob_job_number, is_primary)
        SELECT p.id, o.bin, o.dob_job_number, 1
        FROM projects p JOIN _projects_v1 o ON p.project_name = o.project_name;

        INSERT INTO tco_items (project_id, section, item_ref, description,
                               responsible_party, status, notes, created_at, updated_at)
        SELECT pb.project_id, ti.section, ti.item_ref, ti.description,
               ti.responsible_party, ti.status, ti.notes, ti.created_at, ti.updated_at
        FROM _tco_items_v1 ti JOIN project_bins pb ON pb.bin = ti.bin;

        DROP TABLE _projects_v1;
        DROP TABLE _tco_items_v1;
    ''')
    conn.commit()

def init_db():
    """Create or migrate to the multi-BIN schema."""
    conn = get_db()
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(projects)").fetchall()]
    except Exception:
        cols = []

    if cols and 'bin' in cols and 'id' not in cols:
        # Old single-BIN schema — migrate
        _migrate_v1_to_v2(conn)
    else:
        # Fresh install or already on new schema
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS projects (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                project_name TEXT NOT NULL,
                address      TEXT DEFAULT '',
                borough      TEXT DEFAULT '',
                notes        TEXT DEFAULT '',
                created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS project_bins (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id     INTEGER NOT NULL,
                bin            TEXT NOT NULL,
                label          TEXT DEFAULT '',
                dob_job_number TEXT DEFAULT '',
                is_primary     INTEGER DEFAULT 1,
                created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(project_id, bin),
                FOREIGN KEY (project_id) REFERENCES projects(id)
            );
            CREATE TABLE IF NOT EXISTS tco_items (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id        INTEGER NOT NULL,
                section           TEXT NOT NULL,
                item_ref          TEXT DEFAULT '',
                description       TEXT NOT NULL,
                responsible_party TEXT DEFAULT '',
                status            TEXT DEFAULT 'open',
                notes             TEXT DEFAULT '',
                created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (project_id) REFERENCES projects(id)
            );
            CREATE TABLE IF NOT EXISTS violation_cache (
                source      TEXT PRIMARY KEY,
                data_json   TEXT,
                fetched_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                search_term TEXT DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS app_settings (
                key   TEXT PRIMARY KEY,
                value TEXT DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS email_config (
                id         INTEGER PRIMARY KEY,
                recipient  TEXT DEFAULT '',
                frequency  TEXT DEFAULT 'daily',
                threshold  TEXT DEFAULT 'all',
                sources    TEXT DEFAULT '[]',
                last_sent  TIMESTAMP,
                smtp_user  TEXT DEFAULT '',
                smtp_pass  TEXT DEFAULT ''
            );
        ''')
        conn.commit()
        # Migrate: add search_term column to violation_cache if missing (existing DBs)
        try:
            conn.execute("ALTER TABLE violation_cache ADD COLUMN search_term TEXT DEFAULT ''")
            conn.commit()
        except Exception:
            pass  # Column already exists
        # Migrate: add app_settings table if missing (existing DBs)
        try:
            conn.execute("CREATE TABLE IF NOT EXISTS app_settings (key TEXT PRIMARY KEY, value TEXT DEFAULT '')")
            conn.commit()
        except Exception:
            pass
    conn.close()

init_db()

# ─── Background Cache ─────────────────────────────────────────────────────────
def _enrich_records(records, key):
    """Enrich records with computed fields, same logic as /api/all."""
    ds_config = DATASETS[key]
    enriched = []

    # For DOB ECB Violations: bulk-look up building addresses by BIN so we show the
    # violation building address instead of the company (respondent) address.
    bin_addr_map = {}
    if key == "DOB ECB Violations" and records:
        bin_list = [r.get("bin", "") for r in records]
        bin_addr_map = bulk_fetch_building_addresses(bin_list)
        logger.info("  BIN address lookup: %d unique BINs, %d resolved", len(set(b for b in bin_list if b)), len(bin_addr_map))

    for rec in records:
        status = classify_status(rec, key)
        rec["_status_class"] = status
        rec["_id"]           = rec.get(ds_config["id_field"], "")
        rec["_date"]         = rec.get(ds_config["date_field"], "")[:10] if rec.get(ds_config["date_field"]) else ""
        rec["_status_val"]   = rec.get(ds_config["status_field"], "N/A")
        rec["_source"]       = key

        # For DOB ECB Violations use building address from BIN lookup; fall back to respondent addr
        if key == "DOB ECB Violations" and bin_addr_map:
            b = str(rec.get("bin", "")).strip()
            if b and b in bin_addr_map:
                house, street, boro_code = bin_addr_map[b]
                boro_name = BORO_NAMES.get(boro_code, BORO_NAMES.get(str(rec.get("boro", "")), ""))
                parts = [p for p in [f"{house} {street}".strip(), boro_name] if p]
                rec["_address"] = ", ".join(parts)
            else:
                rec["_address"] = compute_address(rec, ds_config)
        else:
            rec["_address"] = compute_address(rec, ds_config)

        enriched.append(rec)
    return enriched

def refresh_all_cache(search=None):
    """Fetch all datasets and store JSON in violation_cache table.
    Writes an empty list for failed fetches so the cache is always considered complete."""
    if search is None:
        try:
            _c = get_db()
            _r = _c.execute("SELECT value FROM app_settings WHERE key='search_term'").fetchone()
            _c.close()
            search = _r['value'] if _r and _r['value'] else 'VOREA'
        except Exception:
            search = 'VOREA'
    logger.info("=== Starting scheduled cache refresh for all datasets (search=%s) ===", search)
    conn = get_db()
    for key in DATASETS:
        logger.info("  Refreshing dataset: %s …", key)
        result = fetch_violations(key, search_term=search)
        if result["success"]:
            enriched = _enrich_records(result["data"], key)
            logger.info("  ✓ %s — %d records fetched, %d after enrichment", key, result["count"], len(enriched))
        else:
            enriched = []
            logger.warning("  ✗ %s — FAILED: %s", key, result.get("error", "unknown error"))
        conn.execute(
            'INSERT OR REPLACE INTO violation_cache (source, data_json, fetched_at, search_term) VALUES (?, ?, CURRENT_TIMESTAMP, ?)',
            (key, json.dumps(enriched), search)
        )
    conn.commit()
    conn.close()
    logger.info("=== Cache refresh complete ===")


def _cache_age_minutes(fetched_at_str):
    """Return age in minutes from a UTC ISO timestamp string, or 9999 if invalid."""
    try:
        fetched_at = datetime.fromisoformat(fetched_at_str.replace('Z', ''))
        return (datetime.utcnow() - fetched_at).total_seconds() / 60
    except Exception:
        return 9999

# ─── Digest Email ─────────────────────────────────────────────────────────────
def send_digest():
    """Build and send a violation digest email via M365 SMTP. Returns (ok, msg)."""
    conn = get_db()
    cfg  = conn.execute('SELECT * FROM email_config WHERE id=1').fetchone()
    cache_rows = conn.execute('SELECT source, data_json, fetched_at FROM violation_cache').fetchall()
    conn.close()

    if not cfg or not cfg['recipient']:
        return False, "No recipient configured"
    if not cfg['smtp_user'] or not cfg['smtp_pass']:
        return False, "SMTP credentials not configured"
    if not cache_rows:
        return False, "No cached data to report"

    smtp_host = os.environ.get('SMTP_HOST', 'smtp.office365.com')
    smtp_port = int(os.environ.get('SMTP_PORT', 587))

    # Build summary
    total = 0; open_cnt = 0; rows_info = []
    for row in cache_rows:
        data        = json.loads(row['data_json'])
        source_open = sum(1 for r in data if r.get('_status_class') == 'open')
        total      += len(data)
        open_cnt   += source_open
        rows_info.append({'source': row['source'], 'count': len(data), 'open': source_open})

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M')
    subject = f"VOREA Violation Digest — {now_str} — {open_cnt} open items"

    body_text = (
        f"VOREA Violation Tracker — Digest Email\nGenerated: {now_str}\n\n"
        f"SUMMARY\n-------\nTotal Records: {total}\nOpen Items:    {open_cnt}\n\n"
        f"BY SOURCE\n---------\n" +
        "\n".join(f"  {r['source']}: {r['count']} records ({r['open']} open)" for r in rows_info) +
        "\n\nView the full report at your VOREA Violation Tracker."
    )

    tbl_rows_html = "".join(
        f'<tr style="border-bottom:1px solid #eee">'
        f'<td style="padding:8px 10px">{r["source"]}</td>'
        f'<td style="text-align:right;padding:8px 10px">{r["count"]}</td>'
        f'<td style="text-align:right;padding:8px 10px;color:#ef4444">{r["open"]}</td></tr>'
        for r in rows_info
    )
    body_html = f"""<html><body style="font-family:Arial,sans-serif;color:#1a1a2e;background:#f5f5f5;padding:20px">
<div style="max-width:600px;margin:0 auto;background:#fff;border-radius:8px;padding:24px;box-shadow:0 2px 8px rgba(0,0,0,.1)">
  <div style="background:#1a1a2e;color:#f5c518;padding:12px 20px;border-radius:4px;margin-bottom:20px">
    <strong style="font-family:monospace;letter-spacing:.1em">VOREA</strong> — Violation Digest
  </div>
  <p style="color:#666;font-size:13px">Generated: {now_str}</p>
  <div style="display:flex;gap:20px;margin:16px 0">
    <div style="background:#fff4e5;border-left:4px solid #f59e0b;padding:12px 20px;border-radius:0 4px 4px 0;flex:1">
      <div style="font-size:28px;font-weight:bold;font-family:monospace">{total}</div>
      <div style="font-size:11px;color:#999;text-transform:uppercase">Total Records</div>
    </div>
    <div style="background:#fee2e2;border-left:4px solid #ef4444;padding:12px 20px;border-radius:0 4px 4px 0;flex:1">
      <div style="font-size:28px;font-weight:bold;font-family:monospace;color:#ef4444">{open_cnt}</div>
      <div style="font-size:11px;color:#999;text-transform:uppercase">Open Items</div>
    </div>
  </div>
  <table style="width:100%;border-collapse:collapse;font-size:13px">
    <tr style="background:#f5f5f5">
      <th style="text-align:left;padding:8px 10px">Source</th>
      <th style="text-align:right;padding:8px 10px">Total</th>
      <th style="text-align:right;padding:8px 10px">Open</th>
    </tr>
    {tbl_rows_html}
  </table>
  <p style="margin-top:20px;font-size:12px;color:#999">This digest was sent automatically by VOREA Violation Tracker.</p>
</div></body></html>"""

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From']    = cfg['smtp_user']
    msg['To']      = cfg['recipient']
    msg.attach(MIMEText(body_text, 'plain'))
    msg.attach(MIMEText(body_html, 'html'))

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(cfg['smtp_user'], cfg['smtp_pass'])
            server.sendmail(cfg['smtp_user'], cfg['recipient'], msg.as_string())
        conn = get_db()
        conn.execute('UPDATE email_config SET last_sent=CURRENT_TIMESTAMP WHERE id=1')
        conn.commit()
        conn.close()
        return True, "Digest sent successfully"
    except Exception as e:
        return False, str(e)

def check_and_send_digest():
    """Hourly APScheduler job: send digest if frequency threshold is met."""
    conn = get_db()
    cfg  = conn.execute('SELECT * FROM email_config WHERE id=1').fetchone()
    conn.close()
    if not cfg or not cfg['recipient']:
        return
    now  = datetime.utcnow()
    last = cfg['last_sent']
    if not last:
        should_send = True
    else:
        try:
            last_dt     = datetime.fromisoformat(last.replace('Z', ''))
            delta       = now - last_dt
            should_send = {
                'hourly': delta.total_seconds() >= 3600,
                'daily':  delta.days >= 1,
                'weekly': delta.days >= 7,
            }.get(cfg['frequency'], False)
        except Exception:
            should_send = True
    if should_send:
        send_digest()

# ─── BIN / Permit helpers (NYC Open Data) ────────────────────────────────────
def fetch_permits_by_bin(bin_number):
    """DOB BIS Permit Issuance — field bin__ (double underscore)."""
    url = "https://data.cityofnewyork.us/resource/ipu4-2q9a.json"
    params = {
        "bin__": bin_number, "$limit": 500, "$order": "issuance_date DESC",
        "$select": ("job__,job_doc___,job_type,permit_type,work_type,permit_status,"
                    "filing_status,issuance_date,expiration_date,"
                    "permittee_s_business_name,owner_s_business_name,filing_date")
    }
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json() if isinstance(r.json(), list) else []
    except Exception:
        return []

def fetch_dobnow_by_bin(bin_number):
    """DOB NOW Build — Job Application Filings (w9ak-ipjd) by BIN."""
    url = "https://data.cityofnewyork.us/resource/w9ak-ipjd.json"
    params = {
        "bin": bin_number, "$limit": 300, "$order": "filing_date DESC",
        "$select": ("job_filing_number,bin,borough,house_no,street_name,job_type,"
                    "filing_status,filing_date,approved_date,signoff_date,"
                    "owner_s_business_name,general_construction_work_type_,"
                    "mechanical_systems_work_type_,plumbing_work_type,structural_work_type_,"
                    "special_inspection_requirement,special_inspection_agency_number")
    }
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json() if isinstance(r.json(), list) else []
    except Exception:
        return []

def fetch_dobnow_permits_by_bin(bin_number):
    """DOB NOW Build – Approved Permits (rbx6-tga4) by BIN."""
    url = "https://data.cityofnewyork.us/resource/rbx6-tga4.json"
    params = {
        "bin": bin_number, "$limit": 200, "$order": "issued_date DESC",
        "$select": ("job_filing_number,work_permit,sequence_number,permit_status,"
                    "approved_date,issued_date,expired_date,job_description,"
                    "filing_reason,bin,borough,house_no,street_name")
    }
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json() if isinstance(r.json(), list) else []
    except Exception:
        return []

def parse_project_address(address_str):
    """Split '1940 Jerome Ave, Bronx' into house_no='1940', street='JEROME AVE'."""
    if not address_str:
        return None, None
    # Strip borough if present after comma
    base = address_str.split(',')[0].strip()
    parts = base.split(' ', 1)
    if len(parts) == 2 and parts[0].replace('-', '').isdigit():
        return parts[0].upper(), parts[1].upper()
    return None, base.upper()

def fetch_dot_permits_by_address(house_no, street_name, borough=None):
    """DOT Street Construction Permits 2022-Present (tqtj-sjs8) — no BIN field;
    query by house number + street name + borough instead."""
    if not house_no and not street_name:
        return []
    url = "https://data.cityofnewyork.us/resource/tqtj-sjs8.json"
    where_parts = []
    if house_no:
        where_parts.append(f"upper(permithousenumber) = '{house_no}'")
    if street_name:
        first_word = street_name.split()[0] if street_name else ''
        if first_word:
            where_parts.append(f"upper(onstreetname) like '%{first_word}%'")
    if borough:
        boro_map = {
            'BRONX': 'BRONX', 'BROOKLYN': 'BROOKLYN', 'MANHATTAN': 'MANHATTAN',
            'QUEENS': 'QUEENS', 'STATEN ISLAND': 'STATEN IS'
        }
        b = boro_map.get(borough.upper(), borough.upper())
        where_parts.append(f"upper(boroughname) like '%{b}%'")
    params = {
        "$where": " AND ".join(where_parts),
        "$limit": 200, "$order": "permitissuedate DESC",
        "$select": ("permitnumber,permitteename,permittypedesc,equipmenttypedesc,"
                    "permitstatusshortdesc,permitissuedate,issuedworkstartdate,"
                    "issuedworkenddate,boroughname,permithousenumber,onstreetname,"
                    "fromstreetname,tostreetname,permitpurposecomments")
    }
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json() if isinstance(r.json(), list) else []
    except Exception:
        return []

def fetch_violations_by_bin(bin_number):
    """DOB civil violations (3h2n-5cm9) by BIN — all respondents."""
    url = "https://data.cityofnewyork.us/resource/3h2n-5cm9.json"
    try:
        r = requests.get(url, params={"bin": bin_number, "$limit": 1000, "$order": "issue_date DESC"}, timeout=30)
        r.raise_for_status()
        return r.json() if isinstance(r.json(), list) else []
    except Exception:
        return []

def fetch_dob_safety_by_bin(bin_number):
    """DOB Safety Violations (855j-jady, post-2022) by BIN — all respondents."""
    url = "https://data.cityofnewyork.us/resource/855j-jady.json"
    try:
        r = requests.get(url, params={"bin": bin_number, "$limit": 500, "$order": "violation_issue_date DESC"}, timeout=30)
        r.raise_for_status()
        return r.json() if isinstance(r.json(), list) else []
    except Exception:
        return []

def fetch_ecb_by_bin(bin_number):
    """DOB ECB violations by BIN."""
    url = "https://data.cityofnewyork.us/resource/6bgk-3dad.json"
    try:
        r = requests.get(url, params={"bin": bin_number, "$limit": 500, "$order": "issue_date DESC"}, timeout=30)
        r.raise_for_status()
        return r.json() if isinstance(r.json(), list) else []
    except Exception:
        return []

def fetch_elevator_permits_by_bin(bin_number):
    """DOB NOW Build – Elevator Permit Applications (kfp4-dz4h) by BIN."""
    url = "https://data.cityofnewyork.us/resource/kfp4-dz4h.json"
    try:
        r = requests.get(url, params={"bin": bin_number, "$limit": 200, "$order": "filing_date DESC"}, timeout=30)
        r.raise_for_status()
        return r.json() if isinstance(r.json(), list) else []
    except Exception:
        return []

def fetch_dob_complaints_by_bin(bin_number):
    """DOB Complaints Received (eabe-havv) by BIN."""
    url = "https://data.cityofnewyork.us/resource/eabe-havv.json"
    try:
        r = requests.get(url, params={"bin": bin_number, "$limit": 500, "$order": "date_entered DESC"}, timeout=30)
        r.raise_for_status()
        return r.json() if isinstance(r.json(), list) else []
    except Exception:
        return []

def fetch_electrical_permits_by_bin(bin_number):
    """DOB NOW Electrical Permit Applications (dm9a-ab7w) by BIN."""
    url = "https://data.cityofnewyork.us/resource/dm9a-ab7w.json"
    try:
        r = requests.get(url, params={"bin": bin_number, "$limit": 200, "$order": "filing_date DESC"}, timeout=30)
        r.raise_for_status()
        return r.json() if isinstance(r.json(), list) else []
    except Exception:
        return []

# Borough code → name mapping for DOB datasets
BORO_NAMES = {
    "1": "Manhattan", "2": "Bronx", "3": "Brooklyn", "4": "Queens", "5": "Staten Island",
    "MANHATTAN": "Manhattan", "BRONX": "Bronx", "BROOKLYN": "Brooklyn",
    "QUEENS": "Queens", "STATEN ISLAND": "Staten Island",
}

def bulk_fetch_building_addresses(bin_list):
    """Look up building addresses for a list of BINs via DOB Permit Issuance dataset.
    Returns a dict mapping bin → (house_no, street_name, boro_code)."""
    if not bin_list:
        return {}
    url = "https://data.cityofnewyork.us/resource/ipu4-2q9a.json"
    unique_bins = list({str(b).strip() for b in bin_list if b})
    if not unique_bins:
        return {}
    # Build a batched $where clause — query up to 200 BINs at a time
    addr_map = {}
    batch_size = 200
    for i in range(0, len(unique_bins), batch_size):
        batch = unique_bins[i:i + batch_size]
        bins_quoted = ",".join(f"'{b}'" for b in batch)
        params = {
            "$where": f"bin__ in ({bins_quoted})",
            "$select": "bin__,house_no,street_name,borough",
            "$limit": batch_size * 2,
        }
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            for rec in r.json():
                b = str(rec.get("bin__", "")).strip()
                if b and b not in addr_map:
                    addr_map[b] = (
                        rec.get("house_no", ""),
                        rec.get("street_name", ""),
                        rec.get("borough", ""),
                    )
        except Exception as e:
            logger.warning("BIN address lookup failed for batch starting %s: %s", batch[0], e)
    return addr_map


def fetch_violations(dataset_key, search_term=None, limit=5000):
    """Fetch violations from NYC Open Data for a given dataset."""
    ds = DATASETS[dataset_key]
    search = search_term or ds["search_value"]
    timeout = ds.get("timeout", 30)

    url = f"{BASE_URL}/{ds['id']}.json"
    params = {
        "$where": f"upper({ds['search_field']}) like '%{search.upper()}%'",
        "$limit": limit,
    }
    if ds.get('date_field'):
        params["$order"] = f"{ds['date_field']} DESC"

    logger.info("  Fetching %-40s  url=%s search=%s timeout=%ss", dataset_key, url, search, timeout)
    try:
        resp = requests.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        # If API returned an error dict
        if isinstance(data, dict) and "error" in data:
            logger.warning("  ✗ API error for %s: %s", dataset_key, data)
            return {"success": False, "error": str(data), "data": [], "count": 0, "source": dataset_key}
        logger.info("  ✓ %s — %d records returned", dataset_key, len(data))
        return {"success": True, "data": data, "count": len(data), "source": dataset_key}
    except requests.exceptions.RequestException as e:
        resp_obj = getattr(e, 'response', None)
        status_code = getattr(resp_obj, 'status_code', 'N/A')
        body = (getattr(resp_obj, 'text', '') or '')[:400]
        logger.error("  ✗ Request failed for %s: HTTP %s — %s%s",
                     dataset_key, status_code, e,
                     f"\n      Socrata error: {body}" if body else "")
        return {"success": False, "error": str(e), "data": [], "count": 0, "source": dataset_key}


def compute_address(record, ds_config):
    """Build a human-readable address string from available fields."""
    addr_parts = [str(record.get(f, "")).strip() for f in ds_config.get("address_fields", [])]
    addr = " ".join(p for p in addr_parts if p)

    # Resolve borough code → name
    boro_field = ds_config.get("borough_field", "")
    boro_raw = str(record.get(boro_field, "")).strip()
    boro_name = BORO_NAMES.get(boro_raw, boro_raw)  # If already a name, pass through

    if boro_name and boro_name not in addr:
        addr = f"{addr}, {boro_name}" if addr else boro_name
    return addr.strip(", ")


def classify_status(record, dataset_key):
    """Return open/resolved/pending classification."""
    ds = DATASETS.get(dataset_key, {})
    status_field = ds.get("status_field", "")
    status_val = str(record.get(status_field, "")).upper()

    resolved_kw = ["RESOLVED", "CLOSED", "DISMISSED", "SATISFIED", "PAID IN FULL",
                   "COMPLIED", "RESCINDED", "DELETED", "EXPIRE", "NOT GUILTY",
                   "IN COMPLIANCE", "PENALTY PAID"]
    open_kw     = ["OPEN", "ACTIVE", "PENDING", "OUTSTANDING", "ISSUED", "NEW ISSUANCE",
                   "UNRESOLVED", "V-DOB VIOLATION - ACTIVE", "DEFAULT", "GUILTY",
                   "SCHEDULED", "IN VIOLATION", "UNPAID", "AMOUNT DUE",
                   "CERTIFICATE REJECTED"]

    for kw in resolved_kw:
        if kw in status_val:
            return "resolved"
    for kw in open_kw:
        if kw in status_val:
            return "open"
    return "pending"


@app.route("/")
def index():
    return render_template("index.html", datasets=DATASETS)


@app.route("/api/violations")
def get_violations():
    dataset = request.args.get("dataset", "DOB Violations")
    search = request.args.get("search", "VOREA")

    if dataset not in DATASETS:
        return jsonify({"error": "Unknown dataset"}), 400

    result = fetch_violations(dataset, search_term=search)

    if result["success"]:
        ds_config = DATASETS[dataset]
        enriched = []
        for rec in result["data"]:
            rec["_status_class"] = classify_status(rec, dataset)
            rec["_id"] = rec.get(ds_config["id_field"], "")
            rec["_date"] = rec.get(ds_config["date_field"], "")[:10] if rec.get(ds_config["date_field"]) else ""
            rec["_status_val"] = rec.get(ds_config["status_field"], "N/A")
            rec["_address"] = compute_address(rec, ds_config)
            enriched.append(rec)
        result["data"] = enriched

    return jsonify(result)


@app.route("/api/all")
def get_all_violations():
    """Serve from DB cache if fresh (<= 15 min); otherwise live-fetch and warm cache."""
    search        = request.args.get("search", "VOREA")
    force_refresh = request.args.get("refresh", "false").lower() == "true"

    conn       = get_db()
    cache_rows = conn.execute(
        'SELECT source, data_json, fetched_at, search_term FROM violation_cache'
    ).fetchall()
    conn.close()

    # Check if all sources are cached and fresh
    cache_map      = {r['source']: r for r in cache_rows}
    all_cached     = len(cache_map) == len(DATASETS)
    oldest_age     = max((_cache_age_minutes(r['fetched_at']) for r in cache_rows), default=9999)
    # Invalidate cache if search term changed
    cached_search  = cache_rows[0]['search_term'] if cache_rows else ''
    search_changed = cached_search.upper() != search.upper()
    if search_changed and cache_rows:
        logger.info("  Cache search_term mismatch ('%s' vs '%s') — bypassing cache", cached_search, search)
    use_cache    = not force_refresh and all_cached and oldest_age <= 15 and not search_changed

    all_results  = {}
    summary      = {"total": 0, "open": 0, "resolved": 0, "pending": 0, "by_source": {}}
    from_cache   = False
    cached_at    = None

    if use_cache:
        from_cache = True
        cached_at  = min(r['fetched_at'] for r in cache_rows)
        for key, row in cache_map.items():
            enriched      = json.loads(row['data_json'])
            source_counts = {"open": 0, "resolved": 0, "pending": 0}
            for rec in enriched:
                s = rec.get("_status_class", "pending")
                source_counts[s] = source_counts.get(s, 0) + 1
                summary[s]       = summary.get(s, 0) + 1
            summary["total"]            += len(enriched)
            summary["by_source"][key]    = {"count": len(enriched), **source_counts}
            all_results[key]             = enriched
    else:
        # Live fetch — also warm the cache (write [] for failed sources)
        logger.info("=== Live fetch triggered for all datasets (search=%s, force=%s) ===", search, force_refresh)
        conn = get_db()
        for key in DATASETS:
            result        = fetch_violations(key, search_term=search)
            if result["success"]:
                enriched = _enrich_records(result["data"], key)
                logger.info("  ✓ %s — %d records, enriched %d", key, result["count"], len(enriched))
            else:
                enriched = []
                logger.warning("  ✗ %s FAILED: %s", key, result.get("error", "unknown"))
            source_counts = {"open": 0, "resolved": 0, "pending": 0}
            for rec in enriched:
                s = rec.get("_status_class", "pending")
                source_counts[s] = source_counts.get(s, 0) + 1
                summary[s]       = summary.get(s, 0) + 1
            summary["total"]         += len(enriched)
            summary["by_source"][key] = {"count": len(enriched), **source_counts}
            all_results[key]          = enriched
            conn.execute(
                'INSERT OR REPLACE INTO violation_cache (source, data_json, fetched_at, search_term) VALUES (?, ?, CURRENT_TIMESTAMP, ?)',
                (key, json.dumps(enriched), search)
            )
        conn.commit()
        conn.close()
        logger.info("=== Live fetch complete — total %d records ===", summary["total"])

    return jsonify({
        "results":    all_results,
        "summary":    summary,
        "from_cache": from_cache,
        "cached_at":  cached_at,
    })


@app.route("/api/export")
def export_csv():
    dataset = request.args.get("dataset", "all")
    search = request.args.get("search", "VOREA")

    rows = []
    if dataset == "all":
        for key in DATASETS:
            result = fetch_violations(key, search_term=search)
            if result["success"]:
                for rec in result["data"]:
                    rec["_source"] = key
                    rows.append(rec)
    else:
        result = fetch_violations(dataset, search_term=search)
        if result["success"]:
            rows = result["data"]

    if not rows:
        return "No data found", 404

    output = io.StringIO()
    # Gather all keys
    all_keys = set()
    for r in rows:
        all_keys.update(r.keys())
    fieldnames = sorted([k for k in all_keys if not k.startswith("_")])
    if "_source" in all_keys:
        fieldnames = ["_source"] + fieldnames

    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({k: row.get(k, "") for k in fieldnames})

    output.seek(0)
    filename = f"vorea_violations_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    return send_file(
        io.BytesIO(output.getvalue().encode()),
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename
    )


# ─── Projects / BIN Tracker ──────────────────────────────────────────────────

def _project_with_bins(conn, project):
    """Return project dict enriched with its BINs and TCO item counts."""
    bins = conn.execute(
        'SELECT * FROM project_bins WHERE project_id=? ORDER BY is_primary DESC, created_at',
        (project['id'],)
    ).fetchall()
    counts_rows = conn.execute(
        'SELECT status, COUNT(*) AS cnt FROM tco_items WHERE project_id=? GROUP BY status',
        (project['id'],)
    ).fetchall()
    return {**dict(project),
            'bins': [dict(b) for b in bins],
            'tco_counts': {r['status']: r['cnt'] for r in counts_rows}}


@app.route('/api/projects', methods=['GET'])
def get_projects():
    conn = get_db()
    projects = conn.execute('SELECT * FROM projects ORDER BY project_name').fetchall()
    result = [_project_with_bins(conn, p) for p in projects]
    conn.close()
    return jsonify(result)


@app.route('/api/projects', methods=['POST'])
def add_project():
    d = request.json or {}
    name = d.get('project_name', '').strip()
    if not name:
        return jsonify({'error': 'Project name required'}), 400
    conn = get_db()
    try:
        conn.execute(
            'INSERT INTO projects (project_name,address,borough,notes) VALUES (?,?,?,?)',
            (name, d.get('address', ''), d.get('borough', ''), d.get('notes', ''))
        )
        conn.commit()
        project_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
        bin_num = str(d.get('bin', '')).strip()
        if bin_num:
            conn.execute(
                'INSERT INTO project_bins (project_id,bin,label,dob_job_number,is_primary) VALUES (?,?,?,?,1)',
                (project_id, bin_num, d.get('bin_label', ''), d.get('dob_job_number', ''))
            )
            conn.commit()
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)}), 500
    conn.close()
    return jsonify({'success': True, 'id': project_id})


@app.route('/api/projects/<int:project_id>', methods=['PUT'])
def update_project(project_id):
    d = request.json or {}
    sets, vals = [], []
    for f in ['project_name', 'address', 'borough', 'notes']:
        if f in d:
            sets.append(f'{f}=?')
            vals.append(d[f])
    if not sets:
        return jsonify({'error': 'Nothing to update'}), 400
    vals.append(project_id)
    conn = get_db()
    conn.execute(f'UPDATE projects SET {", ".join(sets)} WHERE id=?', vals)
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/projects/<int:project_id>', methods=['DELETE'])
def delete_project(project_id):
    conn = get_db()
    conn.execute('DELETE FROM tco_items   WHERE project_id=?', (project_id,))
    conn.execute('DELETE FROM project_bins WHERE project_id=?', (project_id,))
    conn.execute('DELETE FROM projects    WHERE id=?',          (project_id,))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/projects/<int:project_id>/bins', methods=['POST'])
def add_bin(project_id):
    d = request.json or {}
    bin_num = str(d.get('bin', '')).strip()
    if not bin_num:
        return jsonify({'error': 'BIN required'}), 400
    conn = get_db()
    try:
        conn.execute(
            'INSERT INTO project_bins (project_id,bin,label,dob_job_number,is_primary) VALUES (?,?,?,?,0)',
            (project_id, bin_num, d.get('label', ''), d.get('dob_job_number', ''))
        )
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        return jsonify({'error': f'BIN {bin_num} already in this project'}), 409
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)}), 500
    conn.close()
    return jsonify({'success': True})


@app.route('/api/projects/<int:project_id>/bins/<path:bin_num>', methods=['PUT'])
def update_bin(project_id, bin_num):
    d = request.json or {}
    new_bin = str(d.get('bin', bin_num)).strip()
    conn = get_db()
    try:
        conn.execute(
            'UPDATE project_bins SET bin=?,label=?,dob_job_number=? WHERE project_id=? AND bin=?',
            (new_bin, d.get('label', ''), d.get('dob_job_number', ''), project_id, bin_num)
        )
        conn.commit()
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)}), 500
    conn.close()
    return jsonify({'success': True})


@app.route('/api/projects/<int:project_id>/bins/<path:bin_num>', methods=['DELETE'])
def remove_bin(project_id, bin_num):
    conn = get_db()
    count = conn.execute(
        'SELECT COUNT(*) FROM project_bins WHERE project_id=?', (project_id,)
    ).fetchone()[0]
    if count <= 1:
        conn.close()
        return jsonify({'error': 'Cannot remove the last BIN — delete the project instead.'}), 400
    conn.execute('DELETE FROM project_bins WHERE project_id=? AND bin=?', (project_id, bin_num))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/projects/<int:project_id>/report')
def get_project_report(project_id):
    """Aggregate DOB NOW + BIS permits + DOT permits + violations across all project BINs."""
    conn = get_db()
    project  = conn.execute('SELECT * FROM projects WHERE id=?', (project_id,)).fetchone()
    bins     = conn.execute(
        'SELECT * FROM project_bins WHERE project_id=? ORDER BY is_primary DESC', (project_id,)
    ).fetchall()
    tco_rows = conn.execute(
        'SELECT * FROM tco_items WHERE project_id=? ORDER BY section, item_ref', (project_id,)
    ).fetchall()
    conn.close()

    if not project:
        return jsonify({'error': 'Project not found'}), 404

    bin_list       = [b['bin'] for b in bins]
    permits        = []
    dobnow         = []
    dobnow_permits = []
    violations     = []
    safety_viols   = []
    ecb            = []
    complaints     = []
    electrical     = []
    elevator_perm  = []

    for b in bin_list:
        permits        += fetch_permits_by_bin(b)
        dobnow         += fetch_dobnow_by_bin(b)
        dobnow_permits += fetch_dobnow_permits_by_bin(b)
        violations     += fetch_violations_by_bin(b)
        safety_viols   += fetch_dob_safety_by_bin(b)
        ecb            += fetch_ecb_by_bin(b)
        complaints     += fetch_dob_complaints_by_bin(b)
        electrical     += fetch_electrical_permits_by_bin(b)
        elevator_perm  += fetch_elevator_permits_by_bin(b)

    # DOT permits: query by project address (no BIN field in this dataset)
    house_no, street_name = parse_project_address(project['address'])
    dot_permits = fetch_dot_permits_by_address(house_no, street_name, project['borough'])

    def dedup(records, key):
        seen, out = set(), []
        for r in records:
            k = r.get(key)
            if k and k in seen:
                continue
            if k:
                seen.add(k)
            out.append(r)
        return out

    permits        = dedup(permits,        'job__')
    dobnow         = dedup(dobnow,         'job_filing_number')
    dobnow_permits = dedup(dobnow_permits, 'work_permit')
    violations     = dedup(violations,     'isn_dob_bis_viol')
    safety_viols   = dedup(safety_viols,   'violation_number')
    ecb            = dedup(ecb,            'ecb_violation_number')
    dot_permits    = dedup(dot_permits,    'permitnumber')
    complaints     = dedup(complaints,     'complaint_number')
    electrical     = dedup(electrical,     'job_filing_number')
    elevator_perm  = dedup(elevator_perm,  'job_filing_number')

    CLOSED = {'SIGNED OFF', 'EXPIRED', 'CANCELLED', 'WITHDRAWN'}
    open_permits = [p for p in permits if p.get('permit_status', '').upper() not in CLOSED]

    NOW_CLOSED = {'APPROVED', 'SIGNED OFF', 'WITHDRAWN', 'COMPLETED', 'DISAPPROVED'}
    active_dobnow = [d for d in dobnow if d.get('filing_status', '').upper() not in NOW_CLOSED]

    open_viol = [v for v in violations
                 if 'RESOLVED' not in str(v.get('violation_category', '')).upper()
                 and 'CLOSED'   not in str(v.get('violation_category', '')).upper()]

    SAFETY_CLOSED = {'RESOLVED', 'CLOSED', 'COMPLIED', 'DISMISSED'}
    open_safety = [v for v in safety_viols
                   if str(v.get('violation_status', '')).upper() not in SAFETY_CLOSED]

    ECB_RESOLVED = {'RESOLVE', 'RESOLVED', 'DISMISSED', 'DISMISSED BY ALJ', 'PENALTY PAID'}
    open_ecb = [e for e in ecb
                if str(e.get('ecb_violation_status', '')).upper() not in ECB_RESOLVED]

    DOT_CLOSED = {'EXPIRED', 'EXPIRED UNDER GUARANTEE', 'CANCELLED', 'REVOKED', 'VOIDED'}
    open_dot = [d for d in dot_permits if d.get('permitstatusshortdesc', '').upper() not in DOT_CLOSED]

    def _permit_is_closed(status):
        s = str(status or '').upper()
        return ('EXPIRE' in s or 'CANCEL' in s or 'WITHDRAWN' in s or
                'SUPERSED' in s or 'SIGNED' in s or 'SIGNOFF' in s)
    open_dobnow_permits = [p for p in dobnow_permits
                           if not _permit_is_closed(p.get('permit_status', ''))]

    open_complaints = [c for c in complaints if str(c.get('status', '')).upper() != 'CLOSED']
    ELEC_CLOSED = {'PERMIT EXPIRED', 'JOB SIGNED-OFF', 'WITHDRAWN', 'DISAPPROVED', 'CANCELLED'}
    open_electrical = [e for e in electrical
                       if str(e.get('filing_status', '')).upper() not in ELEC_CLOSED]

    return jsonify({
        'project':       dict(project),
        'bins':          [dict(b) for b in bins],
        'tco_items':     [dict(t) for t in tco_rows],
        'dobnow':        {'all': dobnow,     'active': active_dobnow,
                          'total': len(dobnow),     'active_count': len(active_dobnow)},
        'dobnow_permits':{'all': dobnow_permits, 'open': open_dobnow_permits,
                          'total': len(dobnow_permits), 'open_count': len(open_dobnow_permits)},
        'permits':       {'all': permits,    'open': open_permits,
                          'total': len(permits),    'open_count': len(open_permits)},
        'dot_permits':   {'all': dot_permits,'open': open_dot,
                          'total': len(dot_permits),'open_count': len(open_dot)},
        'violations':    {'dob': violations,     'dob_open': open_viol,
                          'safety': safety_viols, 'safety_open': open_safety,
                          'ecb': ecb,             'ecb_open': open_ecb,
                          'dob_total': len(violations),     'dob_open_count': len(open_viol),
                          'safety_total': len(safety_viols),'safety_open_count': len(open_safety),
                          'ecb_total': len(ecb),            'ecb_open_count': len(open_ecb)},
        'complaints':    {'all': complaints, 'open': open_complaints,
                          'total': len(complaints), 'open_count': len(open_complaints)},
        'electrical':    {'all': electrical, 'open': open_electrical,
                          'total': len(electrical), 'open_count': len(open_electrical)},
        'elevator':      {'all': elevator_perm,
                          'total': len(elevator_perm)},
    })


@app.route('/api/projects/<int:project_id>/tco_items', methods=['POST'])
def add_tco_item(project_id):
    d = request.json or {}
    if not d.get('description', '').strip():
        return jsonify({'error': 'Description required'}), 400
    conn = get_db()
    try:
        conn.execute(
            'INSERT INTO tco_items '
            '(project_id,section,item_ref,description,responsible_party,status,notes) '
            'VALUES (?,?,?,?,?,?,?)',
            (project_id, d.get('section', 'other'), d.get('item_ref', ''),
             d['description'].strip(), d.get('responsible_party', ''),
             d.get('status', 'open'), d.get('notes', ''))
        )
        conn.commit()
        new_id = conn.execute('SELECT last_insert_rowid()').fetchone()[0]
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)}), 500
    conn.close()
    return jsonify({'success': True, 'id': new_id})


@app.route('/api/tco_items/<int:item_id>', methods=['PUT'])
def update_tco_item(item_id):
    d = request.json or {}
    sets, vals = [], []
    for f in ['status', 'notes', 'responsible_party', 'description']:
        if f in d:
            sets.append(f'{f}=?')
            vals.append(d[f])
    if not sets:
        return jsonify({'error': 'Nothing to update'}), 400
    sets.append('updated_at=CURRENT_TIMESTAMP')
    vals.append(item_id)
    conn = get_db()
    conn.execute(f'UPDATE tco_items SET {", ".join(sets)} WHERE id=?', vals)
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/tco_items/<int:item_id>', methods=['DELETE'])
def delete_tco_item(item_id):
    conn = get_db()
    conn.execute('DELETE FROM tco_items WHERE id=?', (item_id,))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/cache/status')
def cache_status():
    """Return age (seconds) of each cached dataset."""
    conn = get_db()
    rows = conn.execute('SELECT source, fetched_at FROM violation_cache').fetchall()
    conn.close()
    result = {}
    for row in rows:
        age_sec = _cache_age_minutes(row['fetched_at']) * 60
        result[row['source']] = {
            'fetched_at': row['fetched_at'],
            'age_sec':    round(age_sec),
            'age_min':    round(age_sec / 60, 1),
        }
    return jsonify(result)


@app.route('/api/email_config', methods=['GET'])
def get_email_config():
    conn = get_db()
    row  = conn.execute('SELECT * FROM email_config WHERE id=1').fetchone()
    conn.close()
    if row:
        d = dict(row)
        d.pop('smtp_pass', None)   # Never send password back to client
        return jsonify(d)
    return jsonify({'recipient': '', 'frequency': 'daily', 'threshold': 'all',
                    'sources': '[]', 'smtp_user': '', 'last_sent': None})


@app.route('/api/email_config', methods=['POST'])
def save_email_config():
    d    = request.json or {}
    conn = get_db()
    # Only update smtp_pass if a new one was provided
    existing = conn.execute('SELECT smtp_pass FROM email_config WHERE id=1').fetchone()
    smtp_pass = d.get('smtp_pass') or (existing['smtp_pass'] if existing else '')
    conn.execute(
        '''INSERT OR REPLACE INTO email_config
           (id, recipient, frequency, threshold, sources, smtp_user, smtp_pass)
           VALUES (1, ?, ?, ?, ?, ?, ?)''',
        (d.get('recipient', ''), d.get('frequency', 'daily'), d.get('threshold', 'all'),
         json.dumps(d.get('sources', [])), d.get('smtp_user', ''), smtp_pass)
    )
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/send_digest', methods=['POST'])
def api_send_digest():
    """Manually trigger a digest email (for testing)."""
    ok, msg = send_digest()
    return jsonify({'success': ok, 'message': msg})


@app.route('/api/settings', methods=['GET'])
def get_app_settings():
    conn = get_db()
    rows = conn.execute('SELECT key, value FROM app_settings').fetchall()
    conn.close()
    settings = {r['key']: r['value'] for r in rows}
    return jsonify({
        'company_name': settings.get('company_name', 'VOREA CONSTRUCTION'),
        'search_term':  settings.get('search_term',  'VOREA'),
    })


@app.route('/api/settings', methods=['POST'])
def save_app_settings():
    d = request.json or {}
    conn = get_db()
    for key in ['company_name', 'search_term']:
        if key in d:
            conn.execute(
                'INSERT OR REPLACE INTO app_settings (key, value) VALUES (?, ?)',
                (key, str(d[key]).strip())
            )
    # Clear violation_cache so the next /api/all forces a fresh fetch with the new search term
    conn.execute('DELETE FROM violation_cache')
    conn.commit()
    conn.close()
    logger.info("Settings updated — company_name=%s, search_term=%s; cache cleared",
                d.get('company_name'), d.get('search_term'))
    return jsonify({'success': True})


# ─── APScheduler startup ──────────────────────────────────────────────────────
if HAS_SCHEDULER:
    _sched = BackgroundScheduler(daemon=True)
    _sched.add_job(refresh_all_cache, 'interval', minutes=10, id='cache_refresh',
                   max_instances=1, coalesce=True)
    _sched.add_job(check_and_send_digest, 'interval', hours=1, id='digest_check',
                   max_instances=1, coalesce=True)
    _sched.start()
    import atexit
    atexit.register(lambda: _sched.shutdown(wait=False))


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(debug=True, port=port)