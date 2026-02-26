from flask import Flask, render_template, jsonify, request, send_file, session, redirect, url_for
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
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

try:
    import psycopg2
    import psycopg2.extras
    HAS_PG = True
    _INTEGRITY_ERRORS = (sqlite3.IntegrityError, psycopg2.IntegrityError)
except ImportError:
    HAS_PG = False
    _INTEGRITY_ERRORS = (sqlite3.IntegrityError,)

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
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-change-in-prod')

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

# Fields monitored for field-level change detection (per source)
_TRACKED_FIELDS = {
    "DOB ECB Violations":                  ["ecb_violation_status", "penality_imposed", "certificate_status"],
    "OATH Hearings (DOT / All Agencies)":  ["hearing_status", "hearing_date", "civil_penalty_must_pay_amount"],
    "DOT Street Construction Permits":     ["permitstatusshortdesc", "expirationdate"],
}

# ─── DATABASE ABSTRACTION (SQLite locally, Postgres on Vercel) ───────────────
DB_PATH = os.path.join(os.path.dirname(__file__), 'projects.db')

# Tables that have a SERIAL/AUTOINCREMENT primary key named 'id'
_PG_SERIAL_TABLES = {'projects', 'project_bins', 'tco_items', 'violation_snapshots', 'violation_changes', 'users'}

# ON CONFLICT clauses for each upsert table
_PG_UPSERT = {
    'violation_cache':     'ON CONFLICT (source, domain) DO UPDATE SET data_json=EXCLUDED.data_json, fetched_at=EXCLUDED.fetched_at, search_term=EXCLUDED.search_term',
    'violation_snapshots': 'ON CONFLICT (source, snapshot_date, domain) DO UPDATE SET data_json=EXCLUDED.data_json, record_count=EXCLUDED.record_count, fetched_at=EXCLUDED.fetched_at',
    'email_config':        'ON CONFLICT (id) DO UPDATE SET recipient=EXCLUDED.recipient, frequency=EXCLUDED.frequency, threshold=EXCLUDED.threshold, sources=EXCLUDED.sources, smtp_user=EXCLUDED.smtp_user, smtp_pass=EXCLUDED.smtp_pass',
    'app_settings':        'ON CONFLICT (key, domain) DO UPDATE SET value=EXCLUDED.value',
    'company_domains':     'ON CONFLICT (domain) DO NOTHING',
}

def _pg_table(sql):
    import re
    m = re.search(r'(?:INTO|UPDATE|FROM)\s+(\w+)', sql, re.IGNORECASE)
    return m.group(1).lower() if m else ''

class _FakeRow:
    """Row-like object supporting both row[0] and row['key'] — used for last_insert_rowid()."""
    def __init__(self, val):
        self._val = val
    def __getitem__(self, key):
        return self._val

class _DictRow:
    """Wraps a psycopg2 RealDictRow to also support integer index access (like sqlite3.Row)."""
    def __init__(self, row):
        self._row  = row
        self._vals = list(row.values()) if row else []
    def __getitem__(self, key):
        return self._vals[key] if isinstance(key, int) else self._row[key]
    def get(self, key, default=None):
        return self._row.get(key, default)
    def __bool__(self):
        return bool(self._row)
    def keys(self):
        return self._row.keys()

class _PgConn:
    """Thin wrapper around psycopg2 that mimics the sqlite3 connection interface."""

    def __init__(self, pg_conn):
        self._conn = pg_conn
        self._cur  = pg_conn.cursor()
        self._lastrowid_val = None
        self._fake_row = None

    @staticmethod
    def _to_pg(sql):
        s = sql.replace('?', '%s')
        s = s.replace('INTEGER PRIMARY KEY AUTOINCREMENT', 'SERIAL PRIMARY KEY')
        return s

    def execute(self, sql, params=()):
        pg_sql = self._to_pg(sql.strip())
        upper  = pg_sql.upper().lstrip()
        self._fake_row = None

        # SELECT last_insert_rowid() → return stored id without hitting DB
        if 'LAST_INSERT_ROWID' in upper:
            self._fake_row = _FakeRow(self._lastrowid_val)
            return self

        # PRAGMA table_info → information_schema (used in init_db migration check)
        if upper.startswith('PRAGMA'):
            table = sql.split('(')[1].strip().rstrip(')')
            pg_sql = (f"SELECT column_name AS name FROM information_schema.columns "
                      f"WHERE table_name = '{table.lower()}'")
            self._cur.execute(pg_sql)
            return self

        # INSERT OR REPLACE → upsert
        if 'INSERT OR REPLACE' in upper:
            pg_sql = pg_sql.replace('OR REPLACE ', '', 1).replace('or replace ', '', 1)
            pg_sql += ' ' + _PG_UPSERT.get(_pg_table(pg_sql), 'ON CONFLICT DO NOTHING')

        # INSERT OR IGNORE → do nothing on conflict
        elif 'INSERT OR IGNORE' in upper:
            pg_sql = pg_sql.replace('OR IGNORE ', '', 1).replace('or ignore ', '', 1)
            pg_sql += ' ON CONFLICT DO NOTHING'

        # Auto-add RETURNING id for serial tables so lastrowid works
        is_insert = upper.startswith('INSERT')
        table = _pg_table(pg_sql) if is_insert else ''
        if is_insert and table in _PG_SERIAL_TABLES and 'RETURNING' not in upper:
            pg_sql += ' RETURNING id'

        self._cur.execute(pg_sql, params if params else None)

        if is_insert and table in _PG_SERIAL_TABLES:
            try:
                row = self._cur.fetchone()
                if row:
                    self._lastrowid_val = row.get('id') or list(row.values())[0]
            except Exception:
                pass

        return self

    def executemany(self, sql, params_list):
        for p in params_list:
            self.execute(sql, p)
        return self

    def executescript(self, script):
        for stmt in script.split(';'):
            stmt = stmt.strip()
            if stmt:
                try:
                    self._cur.execute(self._to_pg(stmt))
                except Exception as e:
                    logger.debug("executescript stmt skipped: %s", e)

    def fetchone(self):
        if self._fake_row is not None:
            r, self._fake_row = self._fake_row, None
            return r
        row = self._cur.fetchone()
        return _DictRow(row) if row is not None else None

    def fetchall(self):
        return [_DictRow(r) for r in (self._cur.fetchall() or [])]

    def __iter__(self):
        return iter(self.fetchall())

    @property
    def lastrowid(self):
        return self._lastrowid_val

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()


def get_db():
    db_url = os.environ.get('DATABASE_URL')
    if db_url and HAS_PG:
        pg = psycopg2.connect(db_url, cursor_factory=psycopg2.extras.RealDictCursor)
        return _PgConn(pg)
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


_PG_SCHEMA = [
    """CREATE TABLE IF NOT EXISTS company_domains (
        domain       TEXT PRIMARY KEY,
        company_name TEXT NOT NULL,
        search_term  TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS users (
        id            SERIAL PRIMARY KEY,
        email         TEXT NOT NULL UNIQUE,
        password_hash TEXT NOT NULL,
        domain        TEXT NOT NULL,
        created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""",
    """CREATE TABLE IF NOT EXISTS projects (
        id           SERIAL PRIMARY KEY,
        project_name TEXT NOT NULL,
        address      TEXT DEFAULT '',
        borough      TEXT DEFAULT '',
        notes        TEXT DEFAULT '',
        domain       TEXT NOT NULL DEFAULT '',
        created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""",
    """CREATE TABLE IF NOT EXISTS project_bins (
        id             SERIAL PRIMARY KEY,
        project_id     INTEGER NOT NULL,
        bin            TEXT NOT NULL,
        label          TEXT DEFAULT '',
        dob_job_number TEXT DEFAULT '',
        is_primary     INTEGER DEFAULT 1,
        created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(project_id, bin),
        FOREIGN KEY (project_id) REFERENCES projects(id)
    )""",
    """CREATE TABLE IF NOT EXISTS tco_items (
        id                SERIAL PRIMARY KEY,
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
    )""",
    """CREATE TABLE IF NOT EXISTS violation_cache (
        source      TEXT NOT NULL,
        domain      TEXT NOT NULL DEFAULT '',
        data_json   TEXT,
        fetched_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        search_term TEXT DEFAULT '',
        PRIMARY KEY (source, domain)
    )""",
    """CREATE TABLE IF NOT EXISTS violation_snapshots (
        id            SERIAL PRIMARY KEY,
        source        TEXT NOT NULL,
        domain        TEXT NOT NULL DEFAULT '',
        snapshot_date TEXT NOT NULL,
        data_json     TEXT NOT NULL,
        record_count  INTEGER DEFAULT 0,
        search_term   TEXT DEFAULT '',
        fetched_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(source, snapshot_date, domain)
    )""",
    """CREATE TABLE IF NOT EXISTS violation_changes (
        id           SERIAL PRIMARY KEY,
        detected_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        domain       TEXT NOT NULL DEFAULT '',
        source       TEXT NOT NULL,
        record_id    TEXT NOT NULL,
        change_type  TEXT NOT NULL,
        old_value    TEXT DEFAULT '',
        new_value    TEXT DEFAULT '',
        address      TEXT DEFAULT '',
        description  TEXT DEFAULT ''
    )""",
    """CREATE TABLE IF NOT EXISTS app_settings (
        key    TEXT NOT NULL,
        domain TEXT NOT NULL DEFAULT '',
        value  TEXT DEFAULT '',
        PRIMARY KEY (key, domain)
    )""",
    """CREATE TABLE IF NOT EXISTS email_config (
        id         INTEGER PRIMARY KEY,
        recipient  TEXT DEFAULT '',
        frequency  TEXT DEFAULT 'daily',
        threshold  TEXT DEFAULT 'all',
        sources    TEXT DEFAULT '[]',
        last_sent  TIMESTAMP,
        smtp_user  TEXT DEFAULT '',
        smtp_pass  TEXT DEFAULT ''
    )""",
]

# Pre-configured company domains
_DEFAULT_DOMAINS = [
    ('domaincos.com', 'VOREA', 'VOREA'),
    ('vorea.com',     'VOREA', 'VOREA'),
    ('schimenti.com', 'Schimenti', 'Schimenti'),
]


def _seed_projects(conn):
    """Insert default projects/BINs for domaincos.com when that domain has no projects."""
    count = conn.execute("SELECT COUNT(*) AS cnt FROM projects WHERE domain=?", ('domaincos.com',)).fetchone()['cnt']
    if count == 0:
        c1   = conn.execute("INSERT INTO projects (project_name, address, borough, domain) VALUES (?,?,?,?)",
                            ('1940 Jerome Ave', '1940 Jerome Ave', 'Bronx', 'domaincos.com'))
        pid1 = c1.lastrowid
        c2   = conn.execute("INSERT INTO projects (project_name, address, borough, domain) VALUES (?,?,?,?)",
                            ('291 Livingston', '291 Livingston St', 'Brooklyn', 'domaincos.com'))
        pid2 = c2.lastrowid
        conn.execute("INSERT INTO project_bins (project_id,bin,label,is_primary) VALUES (?,?,?,?)", (pid1,'2008251','',1))
        conn.execute("INSERT INTO project_bins (project_id,bin,label,is_primary) VALUES (?,?,?,?)", (pid1,'2129813','1940J - New BIN',0))
        conn.execute("INSERT INTO project_bins (project_id,bin,label,is_primary) VALUES (?,?,?,?)", (pid2,'3000479','291L',1))
        conn.commit()
        logger.info("Seeded default projects for domaincos.com")


def init_db():
    """Create or migrate schema, then seed default data if empty."""
    conn = get_db()

    if os.environ.get('DATABASE_URL') and HAS_PG:
        # ── Postgres: create tables individually with Postgres syntax ──────────
        for stmt in _PG_SCHEMA:
            conn._cur.execute(stmt)
        conn.commit()

        # ── Postgres migrations: add columns / recreate tables with new schema ─
        # These are safe to run on every startup (IF NOT EXISTS / ADD COLUMN IF NOT EXISTS)

        # 1. Add domain column to projects
        conn._cur.execute("ALTER TABLE projects ADD COLUMN IF NOT EXISTS domain TEXT NOT NULL DEFAULT ''")
        conn._cur.execute("UPDATE projects SET domain='domaincos.com' WHERE domain=''")
        conn.commit()

        # Helper: check whether a column exists in a PG table
        def _pg_has_column(table, column):
            conn._cur.execute(
                "SELECT 1 FROM information_schema.columns WHERE table_name=%s AND column_name=%s",
                (table, column)
            )
            return conn._cur.fetchone() is not None

        # 2. violation_cache — needs composite PK (source, domain); drop+recreate if domain missing
        if not _pg_has_column('violation_cache', 'domain'):
            conn._cur.execute("DROP TABLE IF EXISTS violation_cache")
            conn._cur.execute("""
                CREATE TABLE violation_cache (
                    source      TEXT NOT NULL,
                    domain      TEXT NOT NULL DEFAULT '',
                    data_json   TEXT,
                    fetched_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    search_term TEXT DEFAULT '',
                    PRIMARY KEY (source, domain)
                )
            """)
            conn.commit()

        # 3. violation_snapshots — needs domain column + updated UNIQUE constraint
        if not _pg_has_column('violation_snapshots', 'domain'):
            conn._cur.execute("DROP TABLE IF EXISTS violation_snapshots")
            conn._cur.execute("""
                CREATE TABLE violation_snapshots (
                    id            SERIAL PRIMARY KEY,
                    source        TEXT NOT NULL,
                    domain        TEXT NOT NULL DEFAULT '',
                    snapshot_date TEXT NOT NULL,
                    data_json     TEXT NOT NULL,
                    record_count  INTEGER DEFAULT 0,
                    search_term   TEXT DEFAULT '',
                    fetched_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(source, snapshot_date, domain)
                )
            """)
            conn.commit()

        # 4. violation_changes — needs domain column
        if not _pg_has_column('violation_changes', 'domain'):
            conn._cur.execute("DROP TABLE IF EXISTS violation_changes")
            conn._cur.execute("""
                CREATE TABLE violation_changes (
                    id           SERIAL PRIMARY KEY,
                    detected_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    domain       TEXT NOT NULL DEFAULT '',
                    source       TEXT NOT NULL,
                    record_id    TEXT NOT NULL,
                    change_type  TEXT NOT NULL,
                    old_value    TEXT DEFAULT '',
                    new_value    TEXT DEFAULT '',
                    address      TEXT DEFAULT '',
                    description  TEXT DEFAULT ''
                )
            """)
            conn.commit()

        # 5. app_settings — needs composite PK (key, domain); migrate data if domain missing
        if not _pg_has_column('app_settings', 'domain'):
            conn._cur.execute("ALTER TABLE app_settings RENAME TO app_settings_old")
            conn._cur.execute("""
                CREATE TABLE app_settings (
                    key    TEXT NOT NULL,
                    domain TEXT NOT NULL DEFAULT '',
                    value  TEXT DEFAULT '',
                    PRIMARY KEY (key, domain)
                )
            """)
            conn._cur.execute("""
                INSERT INTO app_settings (key, domain, value)
                SELECT key, 'domaincos.com', value FROM app_settings_old
                ON CONFLICT (key, domain) DO NOTHING
            """)
            conn._cur.execute("DROP TABLE app_settings_old")
            conn.commit()
    else:
        # ── SQLite ────────────────────────────────────────────────────────────
        try:
            cols = [r[1] for r in conn.execute("PRAGMA table_info(projects)").fetchall()]
        except Exception:
            cols = []

        if cols and 'bin' in cols and 'id' not in cols:
            _migrate_v1_to_v2(conn)

        # Base tables (idempotent)
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS company_domains (
                domain       TEXT PRIMARY KEY,
                company_name TEXT NOT NULL,
                search_term  TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS users (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                email         TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                domain        TEXT NOT NULL,
                created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS projects (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                project_name TEXT NOT NULL,
                address      TEXT DEFAULT '',
                borough      TEXT DEFAULT '',
                notes        TEXT DEFAULT '',
                domain       TEXT NOT NULL DEFAULT '',
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

        # Migrations: add domain column to projects (safe if already exists)
        try:
            conn.execute("ALTER TABLE projects ADD COLUMN domain TEXT NOT NULL DEFAULT ''")
            conn.execute("UPDATE projects SET domain='domaincos.com' WHERE domain=''")
            conn.commit()
        except Exception:
            pass

        # Recreate domain-aware tables (drop + recreate — these are cache/log tables)
        conn.executescript('''
            DROP TABLE IF EXISTS violation_cache;
            DROP TABLE IF EXISTS violation_snapshots;
            DROP TABLE IF EXISTS violation_changes;
            CREATE TABLE violation_cache (
                source      TEXT NOT NULL,
                domain      TEXT NOT NULL DEFAULT '',
                data_json   TEXT,
                fetched_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                search_term TEXT DEFAULT '',
                PRIMARY KEY (source, domain)
            );
            CREATE TABLE violation_snapshots (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                source        TEXT NOT NULL,
                domain        TEXT NOT NULL DEFAULT '',
                snapshot_date TEXT NOT NULL,
                data_json     TEXT NOT NULL,
                record_count  INTEGER DEFAULT 0,
                search_term   TEXT DEFAULT '',
                fetched_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(source, snapshot_date, domain)
            );
            CREATE TABLE violation_changes (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                detected_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                domain       TEXT NOT NULL DEFAULT '',
                source       TEXT NOT NULL,
                record_id    TEXT NOT NULL,
                change_type  TEXT NOT NULL,
                old_value    TEXT DEFAULT '',
                new_value    TEXT DEFAULT '',
                address      TEXT DEFAULT '',
                description  TEXT DEFAULT ''
            );
        ''')
        conn.commit()

        # Recreate app_settings with composite (key, domain) PK — migrate existing rows
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS app_settings_new (
                key    TEXT NOT NULL,
                domain TEXT NOT NULL DEFAULT '',
                value  TEXT DEFAULT '',
                PRIMARY KEY (key, domain)
            );
            INSERT OR IGNORE INTO app_settings_new (key, domain, value)
                SELECT key, 'domaincos.com', value FROM app_settings WHERE typeof(key)='text';
            DROP TABLE IF EXISTS app_settings;
            ALTER TABLE app_settings_new RENAME TO app_settings;
        ''')
        conn.commit()

    # Seed company domains (both PG and SQLite)
    for domain, company_name, search_term in _DEFAULT_DOMAINS:
        try:
            conn.execute(
                'INSERT OR IGNORE INTO company_domains (domain, company_name, search_term) VALUES (?,?,?)',
                (domain, company_name, search_term)
            )
        except Exception:
            pass
    conn.commit()

    try:
        _seed_projects(conn)
    except Exception as e:
        logger.warning("Seed skipped: %s", e)

    conn.close()

init_db()

# ─── Auth helpers ─────────────────────────────────────────────────────────────
def login_required(f):
    """Decorator: redirect to /login for pages, 401 JSON for API routes."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session:
            if request.path.startswith('/api/'):
                return jsonify({'error': 'Not authenticated', 'redirect': '/login'}), 401
            return redirect('/login')
        return f(*args, **kwargs)
    return decorated


def _get_domain_config(domain, conn):
    """Return company_domains row for a given domain, or None."""
    return conn.execute('SELECT * FROM company_domains WHERE domain=?', (domain,)).fetchone()


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'GET':
        if 'user_id' in session:
            return redirect('/')
        return render_template('login.html', mode='login')
    # POST
    data     = request.get_json(silent=True) or {}
    email    = (data.get('email') or '').strip().lower()
    password = data.get('password', '')
    if not email or not password:
        return jsonify({'error': 'Email and password are required'}), 400
    conn = get_db()
    user = conn.execute('SELECT * FROM users WHERE email=?', (email,)).fetchone()
    if not user or not check_password_hash(user['password_hash'], password):
        conn.close()
        return jsonify({'error': 'Invalid email or password'}), 401
    domain  = user['domain']
    company = _get_domain_config(domain, conn)
    conn.close()
    session.clear()
    session['user_id']      = user['id']
    session['email']        = user['email']
    session['domain']       = domain
    session['company_name'] = company['company_name'] if company else domain
    session['search_term']  = company['search_term']  if company else domain
    return jsonify({'ok': True})


@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'GET':
        if 'user_id' in session:
            return redirect('/')
        return render_template('login.html', mode='register')
    # POST
    data     = request.get_json(silent=True) or {}
    email    = (data.get('email') or '').strip().lower()
    password = data.get('password', '')
    confirm  = data.get('confirm', '')
    if not email or not password:
        return jsonify({'error': 'Email and password are required'}), 400
    if password != confirm:
        return jsonify({'error': 'Passwords do not match'}), 400
    if '@' not in email:
        return jsonify({'error': 'Invalid email address'}), 400
    domain = email.split('@')[1]
    conn   = get_db()
    company = _get_domain_config(domain, conn)
    if not company:
        conn.close()
        return jsonify({'error': f'The domain @{domain} is not authorized. Contact your administrator.'}), 403
    existing = conn.execute('SELECT id FROM users WHERE email=?', (email,)).fetchone()
    if existing:
        conn.close()
        return jsonify({'error': 'An account with this email already exists'}), 409
    conn.execute(
        'INSERT INTO users (email, password_hash, domain) VALUES (?,?,?)',
        (email, generate_password_hash(password), domain)
    )
    conn.commit()
    user = conn.execute('SELECT * FROM users WHERE email=?', (email,)).fetchone()
    conn.close()
    session.clear()
    session['user_id']      = user['id']
    session['email']        = user['email']
    session['domain']       = domain
    session['company_name'] = company['company_name']
    session['search_term']  = company['search_term']
    return jsonify({'ok': True})


@app.route('/logout')
def logout():
    session.clear()
    return redirect('/login')


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

def _detect_and_save_changes(conn, domain, source, old_records, new_records):
    """Compare old vs new enriched records; write detected changes to violation_changes."""
    if not old_records:
        return  # No baseline (first run) — skip
    tracked = _TRACKED_FIELDS.get(source, [])
    old_map = {r['_id']: r for r in old_records if r.get('_id')}
    new_map = {r['_id']: r for r in new_records if r.get('_id')}
    inserts = []
    for rid, rec in new_map.items():
        addr = rec.get('_address', '')
        desc = rec.get('_status_val', '')
        if rid not in old_map:
            inserts.append((domain, source, rid, 'new', '', '', addr, desc))
        else:
            old = old_map[rid]
            old_sc = old.get('_status_class', '')
            new_sc = rec.get('_status_class', '')
            if old_sc != new_sc:
                ctype = 'resolved' if new_sc == 'resolved' else 'status_changed'
                inserts.append((domain, source, rid, ctype,
                                json.dumps({'status': old_sc}),
                                json.dumps({'status': new_sc}),
                                addr, desc))
            else:
                changed = {f: {'old': old.get(f, ''), 'new': rec.get(f, '')}
                           for f in tracked if str(old.get(f, '')) != str(rec.get(f, ''))}
                if changed:
                    inserts.append((domain, source, rid, 'field_changed',
                                    json.dumps({f: v['old'] for f, v in changed.items()}),
                                    json.dumps({f: v['new'] for f, v in changed.items()}),
                                    addr, desc))
    if inserts:
        for row in inserts:
            conn.execute(
                'INSERT INTO violation_changes '
                '(domain, source, record_id, change_type, old_value, new_value, address, description) '
                'VALUES (?,?,?,?,?,?,?,?)', row
            )
        logger.info("  Detected %d change(s) for %s [%s]", len(inserts), source, domain)


def _refresh_domain(conn, domain, search, today):
    """Fetch all datasets for a single domain, update cache + snapshots + change log."""
    for key in DATASETS:
        old_row = conn.execute(
            'SELECT data_json FROM violation_cache WHERE source=? AND domain=?', (key, domain)
        ).fetchone()
        old_enriched = json.loads(old_row['data_json']) if old_row and old_row['data_json'] else []
        result = fetch_violations(key, search_term=search)
        if result["success"]:
            enriched = _enrich_records(result["data"], key)
            logger.info("  ✓ [%s] %s — %d records", domain, key, len(enriched))
            _detect_and_save_changes(conn, domain, key, old_enriched, enriched)
            conn.execute(
                'INSERT OR REPLACE INTO violation_snapshots '
                '(source, domain, snapshot_date, data_json, record_count, search_term) VALUES (?,?,?,?,?,?)',
                (key, domain, today, json.dumps(enriched), len(enriched), search)
            )
            conn.execute(
                'INSERT OR REPLACE INTO violation_cache '
                '(source, domain, data_json, fetched_at, search_term) VALUES (?,?,?,CURRENT_TIMESTAMP,?)',
                (key, domain, json.dumps(enriched), search)
            )
        else:
            logger.warning("  ✗ [%s] %s — FAILED (cache unchanged): %s", domain, key, result.get("error"))


def refresh_all_cache():
    """Fetch all datasets for every configured domain; save snapshots and detect changes."""
    logger.info("=== Starting scheduled cache refresh (all domains) ===")
    conn  = get_db()
    today = datetime.utcnow().strftime('%Y-%m-%d')
    try:
        domain_rows = conn.execute('SELECT domain, search_term FROM company_domains').fetchall()
    except Exception:
        domain_rows = [{'domain': 'domaincos.com', 'search_term': 'VOREA'}]
    for row in domain_rows:
        logger.info("  Domain: %s (search=%s)", row['domain'], row['search_term'])
        _refresh_domain(conn, row['domain'], row['search_term'], today)
    conn.commit()
    conn.close()
    logger.info("=== Scheduled cache refresh complete ===")


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
@login_required
def index():
    return render_template("index.html", datasets=DATASETS,
                           company_name=session.get('company_name', 'VOREA'),
                           user_email=session.get('email', ''))


@app.route("/api/violations")
@login_required
def get_violations():
    dataset = request.args.get("dataset", "DOB Violations")
    search  = session.get('search_term', 'VOREA')

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
@login_required
def get_all_violations():
    """Serve from DB cache if fresh (<= 24 h); otherwise live-fetch, snapshot, detect changes."""
    domain        = session['domain']
    search        = session['search_term']
    force_refresh = request.args.get("refresh", "false").lower() == "true"

    conn       = get_db()
    cache_rows = conn.execute(
        'SELECT source, data_json, fetched_at, search_term FROM violation_cache WHERE domain=?', (domain,)
    ).fetchall()
    conn.close()

    # Check if all sources are cached and fresh
    cache_map  = {r['source']: r for r in cache_rows}
    all_cached = len(cache_map) == len(DATASETS)
    oldest_age = max((_cache_age_minutes(r['fetched_at']) for r in cache_rows), default=9999)
    use_cache  = not force_refresh and all_cached and oldest_age <= 1440

    all_results = {}
    summary     = {"total": 0, "open": 0, "resolved": 0, "pending": 0, "by_source": {}}
    from_cache  = False
    cached_at   = None

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
            summary["total"]         += len(enriched)
            summary["by_source"][key] = {"count": len(enriched), **source_counts}
            all_results[key]          = enriched
    else:
        # Live fetch — warm cache, write daily snapshot, detect changes
        logger.info("=== Live fetch [%s] (search=%s, force=%s) ===", domain, search, force_refresh)
        conn  = get_db()
        today = datetime.utcnow().strftime('%Y-%m-%d')
        _refresh_domain(conn, domain, search, today)
        conn.commit()
        # Re-read fresh data from cache for response
        cache_rows = conn.execute(
            'SELECT source, data_json, fetched_at FROM violation_cache WHERE domain=?', (domain,)
        ).fetchall()
        conn.close()
        for row in cache_rows:
            key      = row['source']
            enriched = json.loads(row['data_json'] or '[]')
            source_counts = {"open": 0, "resolved": 0, "pending": 0}
            for rec in enriched:
                s = rec.get("_status_class", "pending")
                source_counts[s] = source_counts.get(s, 0) + 1
                summary[s]       = summary.get(s, 0) + 1
            summary["total"]         += len(enriched)
            summary["by_source"][key] = {"count": len(enriched), **source_counts}
            all_results[key]          = enriched
        logger.info("=== Live fetch complete — total %d records ===", summary["total"])

    return jsonify({
        "results":    all_results,
        "summary":    summary,
        "from_cache": from_cache,
        "cached_at":  cached_at,
    })


@app.route("/api/export")
@login_required
def export_csv():
    dataset = request.args.get("dataset", "all")
    search  = session['search_term']

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
@login_required
def get_projects():
    domain = session['domain']
    conn   = get_db()
    projects = conn.execute('SELECT * FROM projects WHERE domain=? ORDER BY project_name', (domain,)).fetchall()
    result = [_project_with_bins(conn, p) for p in projects]
    conn.close()
    return jsonify(result)


@app.route('/api/projects', methods=['POST'])
@login_required
def add_project():
    d      = request.json or {}
    name   = d.get('project_name', '').strip()
    domain = session['domain']
    if not name:
        return jsonify({'error': 'Project name required'}), 400
    conn = get_db()
    try:
        conn.execute(
            'INSERT INTO projects (project_name,address,borough,notes,domain) VALUES (?,?,?,?,?)',
            (name, d.get('address', ''), d.get('borough', ''), d.get('notes', ''), domain)
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
@login_required
def update_project(project_id):
    d      = request.json or {}
    domain = session['domain']
    sets, vals = [], []
    for f in ['project_name', 'address', 'borough', 'notes']:
        if f in d:
            sets.append(f'{f}=?')
            vals.append(d[f])
    if not sets:
        return jsonify({'error': 'Nothing to update'}), 400
    vals.extend([project_id, domain])
    conn = get_db()
    conn.execute(f'UPDATE projects SET {", ".join(sets)} WHERE id=? AND domain=?', vals)
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/projects/<int:project_id>', methods=['DELETE'])
@login_required
def delete_project(project_id):
    domain = session['domain']
    conn   = get_db()
    project = conn.execute('SELECT id FROM projects WHERE id=? AND domain=?', (project_id, domain)).fetchone()
    if not project:
        conn.close()
        return jsonify({'error': 'Project not found'}), 404
    conn.execute('DELETE FROM tco_items   WHERE project_id=?', (project_id,))
    conn.execute('DELETE FROM project_bins WHERE project_id=?', (project_id,))
    conn.execute('DELETE FROM projects    WHERE id=? AND domain=?', (project_id, domain))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/projects/<int:project_id>/bins', methods=['POST'])
@login_required
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
    except _INTEGRITY_ERRORS:
        conn.close()
        return jsonify({'error': f'BIN {bin_num} already in this project'}), 409
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)}), 500
    conn.close()
    return jsonify({'success': True})


@app.route('/api/projects/<int:project_id>/bins/<path:bin_num>', methods=['PUT'])
@login_required
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
@login_required
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
@login_required
def get_project_report(project_id):
    """Aggregate DOB NOW + BIS permits + DOT permits + violations across all project BINs."""
    domain = session['domain']
    conn   = get_db()
    project  = conn.execute('SELECT * FROM projects WHERE id=? AND domain=?', (project_id, domain)).fetchone()
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
@login_required
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
@login_required
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
@login_required
def delete_tco_item(item_id):
    conn = get_db()
    conn.execute('DELETE FROM tco_items WHERE id=?', (item_id,))
    conn.commit()
    conn.close()
    return jsonify({'success': True})


@app.route('/api/cache/status')
@login_required
def cache_status():
    """Return age (seconds) of each cached dataset."""
    domain = session['domain']
    conn   = get_db()
    rows   = conn.execute('SELECT source, fetched_at FROM violation_cache WHERE domain=?', (domain,)).fetchall()
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


@app.route('/api/changes/count')
@login_required
def get_changes_count():
    """Return count of change events detected in the last 7 days (for badge display)."""
    domain = session['domain']
    cutoff = (datetime.utcnow() - timedelta(days=7)).isoformat()
    conn   = get_db()
    row    = conn.execute(
        'SELECT COUNT(*) AS cnt FROM violation_changes WHERE domain=? AND detected_at >= ?', (domain, cutoff)
    ).fetchone()
    conn.close()
    return jsonify({'count': row['cnt'] if row else 0})


@app.route('/api/changes')
@login_required
def get_changes():
    """Return recent change log entries, newest first."""
    domain = session['domain']
    days   = int(request.args.get('days', 7))
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    conn   = get_db()
    rows   = conn.execute(
        'SELECT id, detected_at, source, record_id, change_type, old_value, new_value, address, description '
        'FROM violation_changes WHERE domain=? AND detected_at >= ? ORDER BY detected_at DESC LIMIT 500',
        (domain, cutoff)
    ).fetchall()
    conn.close()
    return jsonify({
        'changes': [dict(r) for r in rows],
        'count':   len(rows),
        'days':    days,
    })


@app.route('/api/email_config', methods=['GET'])
@login_required
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
@login_required
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
@login_required
def api_send_digest():
    """Manually trigger a digest email (for testing)."""
    ok, msg = send_digest()
    return jsonify({'success': ok, 'message': msg})


@app.route('/api/settings', methods=['GET'])
@login_required
def get_app_settings():
    domain = session['domain']
    conn   = get_db()
    rows   = conn.execute('SELECT key, value FROM app_settings WHERE domain=?', (domain,)).fetchall()
    conn.close()
    settings = {r['key']: r['value'] for r in rows}
    return jsonify({
        'company_name': settings.get('company_name', session.get('company_name', 'VOREA')),
        'search_term':  settings.get('search_term',  session.get('search_term', 'VOREA')),
        'user_email':   session.get('email', ''),
        'domain':       domain,
    })


@app.route('/api/settings', methods=['POST'])
@login_required
def save_app_settings():
    d      = request.json or {}
    domain = session['domain']
    conn   = get_db()
    for key in ['company_name', 'search_term']:
        if key in d:
            conn.execute(
                'INSERT OR REPLACE INTO app_settings (key, domain, value) VALUES (?,?,?)',
                (key, domain, str(d[key]).strip())
            )
    # If search_term changed, update session and clear this domain's cache
    if 'search_term' in d:
        session['search_term'] = str(d['search_term']).strip()
        conn.execute('DELETE FROM violation_cache WHERE domain=?', (domain,))
    if 'company_name' in d:
        session['company_name'] = str(d['company_name']).strip()
    conn.commit()
    conn.close()
    logger.info("Settings updated [%s] — company_name=%s, search_term=%s",
                domain, d.get('company_name'), d.get('search_term'))
    return jsonify({'success': True})


# ─── APScheduler startup (disabled on Vercel — no background threads in serverless) ──
if HAS_SCHEDULER and not os.environ.get('VERCEL'):
    _sched = BackgroundScheduler(daemon=True)
    _sched.add_job(refresh_all_cache, 'interval', hours=24, id='cache_refresh',
                   max_instances=1, coalesce=True)
    _sched.add_job(check_and_send_digest, 'interval', hours=1, id='digest_check',
                   max_instances=1, coalesce=True)
    _sched.start()
    import atexit
    atexit.register(lambda: _sched.shutdown(wait=False))


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    app.run(debug=True, port=port)