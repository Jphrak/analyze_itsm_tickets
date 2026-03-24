#!/usr/bin/env python3
"""IT Support Interactions ETL Pipeline.

Provides a clean Extract-Transform-Load pipeline for IT support interaction data.
Reads CSV and JSON exports, transforms the data into a normalized star schema,
and loads it into a SQLite database.

Usage:
    python ingest.py                    # Process all exports
    python ingest.py --latest           # Process only latest files
    python ingest.py --stats            # Show database statistics
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING

import pendulum

if TYPE_CHECKING:
    import pendulum as pendulum_type

# =============================================================================
# Configuration
# =============================================================================

CURRENT_DIR = Path(__file__).resolve().parent
EXPORTS_DIR = CURRENT_DIR / "exports"
DB_PATH = CURRENT_DIR / "interactions.db"

MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]
DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# =============================================================================
# Database Schema
# =============================================================================

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS dim_users (
    user_id TEXT PRIMARY KEY,
    user_name TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS dim_technicians (
    tech_id TEXT PRIMARY KEY,
    tech_name TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS dim_locations (
    location_id INTEGER PRIMARY KEY AUTOINCREMENT,
    location_name TEXT UNIQUE NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS dim_states (
    state_id INTEGER PRIMARY KEY AUTOINCREMENT,
    state_name TEXT UNIQUE NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS dim_dates (
    date_id INTEGER PRIMARY KEY,
    full_date TEXT NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name TEXT NOT NULL,
    week_of_year INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name TEXT NOT NULL,
    is_weekend INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS fact_interactions (
    interaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
    interaction_number TEXT UNIQUE NOT NULL,
    short_description TEXT,
    interaction_type TEXT,
    work_notes TEXT,
    user_id TEXT REFERENCES dim_users(user_id),
    tech_id TEXT REFERENCES dim_technicians(tech_id),
    location_id INTEGER REFERENCES dim_locations(location_id),
    state_id INTEGER REFERENCES dim_states(state_id),
    opened_date_id INTEGER REFERENCES dim_dates(date_id),
    opened_at TEXT,
    updated_at TEXT,
    ingested_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS bridge_ims_inc (
    link_id INTEGER PRIMARY KEY AUTOINCREMENT,
    interaction_number TEXT NOT NULL,
    incident_number TEXT,
    interaction_sysid TEXT,
    incident_sysid TEXT,
    created_by TEXT,
    created_on TEXT,
    interaction_url TEXT,
    incident_url TEXT,
    ingested_at TEXT DEFAULT (datetime('now')),
    UNIQUE(interaction_number, incident_number)
);

CREATE INDEX IF NOT EXISTS idx_fact_opened_date ON fact_interactions(opened_date_id);
CREATE INDEX IF NOT EXISTS idx_fact_location ON fact_interactions(location_id);
CREATE INDEX IF NOT EXISTS idx_fact_tech ON fact_interactions(tech_id);
CREATE INDEX IF NOT EXISTS idx_fact_state ON fact_interactions(state_id);
CREATE INDEX IF NOT EXISTS idx_bridge_ims ON bridge_ims_inc(interaction_number);
CREATE INDEX IF NOT EXISTS idx_bridge_inc ON bridge_ims_inc(incident_number);
"""


# =============================================================================
# Extract
# =============================================================================

def find_latest_file(pattern: str, exports_dir: Path | None = None) -> Path | None:
    """Find the most recent file matching a glob pattern in the exports directory.

    Args:
        pattern: Glob pattern to match filenames.
        exports_dir: Directory to search. Defaults to EXPORTS_DIR.

    Returns:
        Path to the most recently named file, or None if no match found.
    """
    search_dir = exports_dir or EXPORTS_DIR
    files = sorted(search_dir.glob(pattern), reverse=True)
    return files[0] if files else None


def extract_interactions_csv(file_path: Path) -> list[dict[str, str]]:
    """Extract interaction records from a CSV file.

    Args:
        file_path: Path to the interactions CSV file.

    Returns:
        List of row dicts with raw string values.
    """
    records: list[dict[str, str]] = []
    with file_path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(dict(row))
    logger.info("Extracted %d interactions from %s", len(records), file_path.name)
    return records


def extract_ims_inc_csv(file_path: Path) -> list[dict[str, str]]:
    """Extract IMS-INC mapping records from a CSV file.

    Args:
        file_path: Path to the IMS-INC CSV file.

    Returns:
        List of row dicts with keys: interaction, task, sys_created_by,
        sys_created_on, document_id.
    """
    records: list[dict[str, str]] = []
    with file_path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(dict(row))
    logger.info("Extracted %d IMS-INC links from %s", len(records), file_path.name)
    return records


def extract_sysid_json(file_path: Path) -> list[dict[str, str]]:
    """Extract sys_id records from a JSON file.

    Supports multiple formats:
    - JSON array: [{...}, {...}]
    - Wrapped format: {"records": [{...}, ...]}
    - NDJSON (newline-delimited): one JSON object per line

    Args:
        file_path: Path to the sysid JSON file.

    Returns:
        List of record dicts.
    """
    content = file_path.read_text(encoding="utf-8").strip()

    try:
        raw_data = json.loads(content)
        if isinstance(raw_data, dict) and "records" in raw_data:
            data: list[dict[str, str]] = raw_data["records"]
        elif isinstance(raw_data, list):
            data = raw_data
        else:
            data = [raw_data]
    except json.JSONDecodeError:
        data = []
        for line in content.splitlines():
            line = line.strip()
            if line:
                data.append(json.loads(line))

    logger.info("Extracted %d sys_id records from %s", len(data), file_path.name)
    return data


# =============================================================================
# Transform
# =============================================================================

def parse_user_field(value: str) -> tuple[str | None, str | None]:
    """Parse 'Name (user_id)' format into (user_id, name).

    Args:
        value: Raw field value from CSV, e.g. "Jackie Phrakousonh (j0p0u94)".

    Returns:
        Tuple of (user_id, display_name), or (None, None) if empty or unmatched.
    """
    if not value or not value.strip():
        return None, None
    match = re.match(r"^(.+?)\s*\(([^)]+)\)$", value.strip())
    if match:
        return match.group(2), match.group(1).strip()
    return None, value.strip()


def parse_datetime(dt_str: str) -> pendulum.DateTime | None:
    """Parse a datetime string into a Pendulum DateTime.

    Supports formats:
    - MM-DD-YYYY HH:MM:SS
    - YYYY-MM-DD HH:MM:SS

    Args:
        dt_str: Raw datetime string from CSV.

    Returns:
        Parsed DateTime in UTC, or None if parsing fails.
    """
    if not dt_str or not dt_str.strip():
        return None
    for fmt in ("%m-%d-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            import datetime as _dt
            native = _dt.datetime.strptime(dt_str.strip(), fmt)  # noqa: DTZ007 - source data has no tz info
            return pendulum.instance(native, tz="UTC")
        except ValueError:
            continue
    return None


def create_date_key(dt: pendulum.DateTime) -> int:
    """Create an integer date key in YYYYMMDD format.

    Args:
        dt: Pendulum DateTime instance.

    Returns:
        Integer date key, e.g. 20250318.
    """
    return int(dt.format("YYYYMMDD"))


def transform_interaction(row: dict[str, str]) -> dict[str, str | int | None]:
    """Transform a raw CSV row into a normalized interaction record.

    Extracts user_id and tech_id from formatted strings, parses timestamps,
    and prepares dimension lookups.

    Args:
        row: Raw CSV row dict.

    Returns:
        Transformed record dict ready for database loading.
    """
    user_id, user_name = parse_user_field(row.get("opened_for", ""))
    tech_id, tech_name = parse_user_field(row.get("assigned_to", ""))
    opened_dt = parse_datetime(row.get("opened_at", ""))
    updated_dt = parse_datetime(row.get("sys_updated_on", ""))

    return {
        "interaction_number": row.get("number", "").strip(),
        "short_description": row.get("short_description", "").strip(),
        "interaction_type": row.get("type", "").strip(),
        "work_notes": row.get("work_notes", "").strip(),
        "state": row.get("state", "").strip(),
        "location": row.get("location", "").strip(),
        "user_id": user_id,
        "user_name": user_name,
        "tech_id": tech_id,
        "tech_name": tech_name,
        "opened_at": opened_dt.to_iso8601_string() if opened_dt else None,
        "updated_at": updated_dt.to_iso8601_string() if updated_dt else None,
        "opened_date_key": create_date_key(opened_dt) if opened_dt else None,
    }


def transform_ims_inc_link(
    csv_row: dict[str, str],
    sysid_lookup: dict[tuple[str, str], dict[str, str]],
) -> dict[str, str | None]:
    """Transform an IMS-INC CSV row with optional sys_id enrichment.

    Args:
        csv_row: Dict from IMS-INC CSV.
        sysid_lookup: Dict mapping (created_by, created_on) to sys_id record.

    Returns:
        Transformed link record with URLs if sys_ids are available.
    """
    key = (csv_row.get("sys_created_by", ""), csv_row.get("sys_created_on", ""))
    sysid_data = sysid_lookup.get(key, {})

    interaction_sysid = sysid_data.get("interaction", "")
    incident_sysid = sysid_data.get("task", "")

    base_url = "https://example.service-now.com"
    interaction_url = (
        f"{base_url}/interaction.do?sys_id={interaction_sysid}"
        if interaction_sysid
        else None
    )
    incident_url = (
        f"{base_url}/incident.do?sys_id={incident_sysid}"
        if incident_sysid
        else None
    )

    return {
        "interaction_number": csv_row.get("interaction", "").strip(),
        "incident_number": csv_row.get("task", "").strip(),
        "interaction_sysid": interaction_sysid,
        "incident_sysid": incident_sysid,
        "created_by": csv_row.get("sys_created_by", "").strip(),
        "created_on": csv_row.get("sys_created_on", "").strip(),
        "interaction_url": interaction_url,
        "incident_url": incident_url,
    }


def build_sysid_lookup(
    sysid_records: list[dict[str, str]],
) -> dict[tuple[str, str], dict[str, str]]:
    """Build a lookup dict from sys_id records keyed by (created_by, created_on).

    Args:
        sysid_records: List of raw sysid record dicts.

    Returns:
        Dict mapping (created_by, created_on) tuples to record dicts.
    """
    lookup: dict[tuple[str, str], dict[str, str]] = {}
    for record in sysid_records:
        key = (record.get("sys_created_by", ""), record.get("sys_created_on", ""))
        lookup[key] = record
    return lookup


# =============================================================================
# Load
# =============================================================================

def init_database(db_path: Path) -> sqlite3.Connection:
    """Initialize the SQLite database with the star schema.

    Args:
        db_path: Path where the SQLite database file will be created.

    Returns:
        Open database connection with WAL mode and foreign keys enabled.
    """
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    logger.info("Database initialized: %s", db_path)
    return conn


def load_dimension_date(conn: sqlite3.Connection, date_key: int | None) -> None:
    """Insert a date dimension record if it does not already exist.

    Args:
        conn: Active SQLite connection.
        date_key: Integer date in YYYYMMDD format, or None to skip.
    """
    if not date_key:
        return

    cursor = conn.execute("SELECT 1 FROM dim_dates WHERE date_id = ?", (date_key,))
    if cursor.fetchone():
        return

    import datetime as _dt
    dt = _dt.datetime.strptime(str(date_key), "%Y%m%d")  # noqa: DTZ007 - no tz needed for date-only keys

    conn.execute(
        """
        INSERT OR IGNORE INTO dim_dates
        (date_id, full_date, year, quarter, month, month_name, week_of_year,
         day_of_month, day_of_week, day_name, is_weekend)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            date_key,
            dt.strftime("%Y-%m-%d"),
            dt.year,
            (dt.month - 1) // 3 + 1,
            dt.month,
            MONTH_NAMES[dt.month - 1],
            dt.isocalendar()[1],
            dt.day,
            dt.weekday(),
            DAY_NAMES[dt.weekday()],
            1 if dt.weekday() >= 5 else 0,
        ),
    )


def load_interactions(
    conn: sqlite3.Connection,
    interactions: list[dict[str, str | int | None]],
) -> None:
    """Load transformed interaction records into the database.

    Upserts dimension records for users, technicians, locations, states, and
    dates, then inserts fact rows.

    Args:
        conn: Active SQLite connection.
        interactions: List of transformed interaction dicts.
    """
    count = 0
    for record in interactions:
        if record["user_id"] and record["user_name"]:
            conn.execute(
                "INSERT OR IGNORE INTO dim_users (user_id, user_name) VALUES (?, ?)",
                (record["user_id"], record["user_name"]),
            )

        if record["tech_id"] and record["tech_name"]:
            conn.execute(
                "INSERT OR IGNORE INTO dim_technicians (tech_id, tech_name) VALUES (?, ?)",
                (record["tech_id"], record["tech_name"]),
            )

        location_id = None
        if record["location"]:
            conn.execute(
                "INSERT OR IGNORE INTO dim_locations (location_name) VALUES (?)",
                (record["location"],),
            )
            cursor = conn.execute(
                "SELECT location_id FROM dim_locations WHERE location_name = ?",
                (record["location"],),
            )
            row = cursor.fetchone()
            if row:
                location_id = row[0]

        state_id = None
        if record["state"]:
            conn.execute(
                "INSERT OR IGNORE INTO dim_states (state_name) VALUES (?)",
                (record["state"],),
            )
            cursor = conn.execute(
                "SELECT state_id FROM dim_states WHERE state_name = ?",
                (record["state"],),
            )
            row = cursor.fetchone()
            if row:
                state_id = row[0]

        load_dimension_date(conn, record["opened_date_key"])  # type: ignore[arg-type]

        conn.execute(
            """
            INSERT OR REPLACE INTO fact_interactions
            (interaction_number, short_description, interaction_type, work_notes,
             user_id, tech_id, location_id, state_id, opened_date_id,
             opened_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record["interaction_number"],
                record["short_description"],
                record["interaction_type"],
                record["work_notes"],
                record["user_id"],
                record["tech_id"],
                location_id,
                state_id,
                record["opened_date_key"],
                record["opened_at"],
                record["updated_at"],
            ),
        )
        count += 1

    conn.commit()
    logger.info("Loaded %d interactions", count)


def load_ims_inc_links(
    conn: sqlite3.Connection,
    links: list[dict[str, str | None]],
) -> None:
    """Load IMS-INC link records into the bridge table.

    Args:
        conn: Active SQLite connection.
        links: List of transformed link dicts.
    """
    inserted = 0
    for link in links:
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO bridge_ims_inc
                (interaction_number, incident_number, interaction_sysid, incident_sysid,
                 created_by, created_on, interaction_url, incident_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    link["interaction_number"],
                    link["incident_number"],
                    link["interaction_sysid"],
                    link["incident_sysid"],
                    link["created_by"],
                    link["created_on"],
                    link["interaction_url"],
                    link["incident_url"],
                ),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            pass  # Duplicate — skip silently

    conn.commit()
    logger.info("Loaded %d IMS-INC links", inserted)


# =============================================================================
# ETL Pipeline
# =============================================================================

def run_etl(
    interactions_csv: Path | None = None,
    ims_inc_csv: Path | None = None,
    sysid_json: Path | None = None,
    exports_dir: Path | None = None,
) -> None:
    """Run the complete ETL pipeline.

    Finds export files, initializes the database, and runs extract-transform-load
    for interactions, sys_ids, and IMS-INC links.

    Args:
        interactions_csv: Path to interactions CSV. Auto-detected if None.
        ims_inc_csv: Path to IMS-INC CSV. Auto-detected if None.
        sysid_json: Path to sys_id JSON. Auto-detected if None.
        exports_dir: Directory to search for export files. Defaults to exports/.
    """
    logger.info("=" * 60)
    logger.info("Starting ETL Pipeline")
    logger.info("=" * 60)

    if not interactions_csv:
        interactions_csv = find_latest_file("interaction_*.csv", exports_dir)
    if not ims_inc_csv:
        ims_inc_csv = find_latest_file("ims_inc_*.csv", exports_dir)
    if not sysid_json:
        sysid_json = find_latest_file("sysid_*.json", exports_dir)

    conn = init_database(DB_PATH)

    try:
        if interactions_csv and interactions_csv.exists():
            logger.info("[1/3] Processing interactions: %s", interactions_csv.name)
            raw = extract_interactions_csv(interactions_csv)
            transformed = [transform_interaction(r) for r in raw]
            load_interactions(conn, transformed)
        else:
            logger.warning("No interactions CSV found")

        sysid_lookup: dict[tuple[str, str], dict[str, str]] = {}
        if sysid_json and sysid_json.exists():
            logger.info("[2/3] Processing sys_ids: %s", sysid_json.name)
            sysid_records = extract_sysid_json(sysid_json)
            sysid_lookup = build_sysid_lookup(sysid_records)
            logger.info("Built lookup with %d sys_id records", len(sysid_lookup))
        else:
            logger.warning("No sys_id JSON found")

        if ims_inc_csv and ims_inc_csv.exists():
            logger.info("[3/3] Processing IMS-INC links: %s", ims_inc_csv.name)
            raw_links = extract_ims_inc_csv(ims_inc_csv)
            transformed_links = [transform_ims_inc_link(r, sysid_lookup) for r in raw_links]
            load_ims_inc_links(conn, transformed_links)
        else:
            logger.warning("No IMS-INC CSV found")

        logger.info("=" * 60)
        logger.info("ETL Pipeline Complete")
        logger.info("=" * 60)
    finally:
        conn.close()


def show_stats() -> None:
    """Display row counts for all database tables."""
    conn = sqlite3.connect(DB_PATH)
    tables = [
        ("dim_users", "Users"),
        ("dim_technicians", "Technicians"),
        ("dim_locations", "Locations"),
        ("dim_states", "States"),
        ("dim_dates", "Dates"),
        ("fact_interactions", "Interactions"),
        ("bridge_ims_inc", "IMS-INC Links"),
    ]

    print("\n" + "=" * 40)
    print("Database Statistics")
    print("=" * 40)
    for table, label in tables:
        try:
            cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")  # noqa: S608 - table names are internal constants
            count = cursor.fetchone()[0]
            print(f"{label:20} {count:>10,}")
        except sqlite3.OperationalError:
            print(f"{label:20} {'N/A':>10}")
    print("=" * 40)
    conn.close()


# =============================================================================
# CLI
# =============================================================================

def main() -> None:
    """Parse arguments and run the ETL pipeline or show stats."""
    parser = argparse.ArgumentParser(description="IT Support Interactions ETL Pipeline")
    parser.add_argument("--stats", action="store_true", help="Show database statistics")
    parser.add_argument("--latest", action="store_true", help="Process only latest export files")
    parser.add_argument("--interactions", type=Path, help="Path to interactions CSV file")
    parser.add_argument("--ims-inc", type=Path, help="Path to IMS-INC CSV file")
    parser.add_argument("--sysid", type=Path, help="Path to sys_id JSON file")
    parser.add_argument(
        "--exports-dir",
        type=Path,
        help="Directory to search for export files (default: exports/)",
    )

    args = parser.parse_args()

    if args.stats:
        show_stats()
    else:
        run_etl(
            interactions_csv=args.interactions,
            ims_inc_csv=args.ims_inc,
            sysid_json=args.sysid,
            exports_dir=args.exports_dir,
        )
        show_stats()


if __name__ == "__main__":
    main()
