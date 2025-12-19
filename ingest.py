#!/usr/bin/env python3
"""
IT Support Interactions ETL Pipeline

This module provides a clean Extract-Transform-Load pipeline for IT support
interaction data. It reads CSV and JSON exports, transforms the data into
a normalized star schema, and loads it into a SQLite database.

Data Sources:
    - interactions CSV: Main ticket data (IMS numbers, users, technicians, etc.)
    - ims_inc CSV: Interaction-to-Incident mapping (IMS → INC links)
    - sysid JSON: System IDs for permanent URL generation

Usage:
    python ingest.py                    # Process all exports
    python ingest.py --latest           # Process only latest files
    python ingest.py --stats            # Show database statistics
"""

import argparse
import csv
import json
import logging
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

# =============================================================================
# Configuration
# =============================================================================

CURRENT_DIR = Path(__file__).resolve().parent
EXPORTS_DIR = CURRENT_DIR / "exports"
DB_PATH = CURRENT_DIR / "interactions.db"

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# =============================================================================
# Database Schema Setup
# =============================================================================

SCHEMA_SQL = """
-- Dimension Tables
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
    date_id INTEGER PRIMARY KEY,  -- YYYYMMDD format
    full_date TEXT NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name TEXT NOT NULL,
    week_of_year INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,  -- 0=Monday, 6=Sunday
    day_name TEXT NOT NULL,
    is_weekend INTEGER DEFAULT 0
);

-- Fact Table: Interactions
CREATE TABLE IF NOT EXISTS fact_interactions (
    interaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
    interaction_number TEXT UNIQUE NOT NULL,  -- IMS0001234
    short_description TEXT,
    interaction_type TEXT,
    work_notes TEXT,
    
    -- Foreign keys to dimensions
    user_id TEXT REFERENCES dim_users(user_id),
    tech_id TEXT REFERENCES dim_technicians(tech_id),
    location_id INTEGER REFERENCES dim_locations(location_id),
    state_id INTEGER REFERENCES dim_states(state_id),
    opened_date_id INTEGER REFERENCES dim_dates(date_id),
    
    -- Timestamps
    opened_at TEXT,
    updated_at TEXT,
    
    -- Metadata
    ingested_at TEXT DEFAULT (datetime('now'))
);

-- Bridge Table: Interaction-Incident Links
CREATE TABLE IF NOT EXISTS bridge_ims_inc (
    link_id INTEGER PRIMARY KEY AUTOINCREMENT,
    interaction_number TEXT NOT NULL,  -- IMS0001234
    incident_number TEXT,              -- INC0005678
    interaction_sysid TEXT,            -- 32-char UUID
    incident_sysid TEXT,               -- 32-char UUID
    created_by TEXT,
    created_on TEXT,
    
    -- Computed URLs
    interaction_url TEXT,
    incident_url TEXT,
    
    -- Metadata
    ingested_at TEXT DEFAULT (datetime('now')),
    UNIQUE(interaction_number, incident_number)
);

-- Indexes for query performance
CREATE INDEX IF NOT EXISTS idx_fact_opened_date ON fact_interactions(opened_date_id);
CREATE INDEX IF NOT EXISTS idx_fact_location ON fact_interactions(location_id);
CREATE INDEX IF NOT EXISTS idx_fact_tech ON fact_interactions(tech_id);
CREATE INDEX IF NOT EXISTS idx_fact_state ON fact_interactions(state_id);
CREATE INDEX IF NOT EXISTS idx_bridge_ims ON bridge_ims_inc(interaction_number);
CREATE INDEX IF NOT EXISTS idx_bridge_inc ON bridge_ims_inc(incident_number);
"""


# =============================================================================
# Extract Functions
# =============================================================================

def find_latest_file(pattern: str) -> Optional[Path]:
    """Find the most recent file matching the pattern in exports directory."""
    files = sorted(EXPORTS_DIR.glob(pattern), reverse=True)
    return files[0] if files else None


def extract_interactions_csv(file_path: Path) -> List[Dict]:
    """
    Extract interaction records from CSV file.
    
    Returns list of dicts with keys:
        number, opened_at, short_description, opened_for, state,
        type, assigned_to, sys_updated_on, location, work_notes
    """
    records = []
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(dict(row))
    logger.info(f"Extracted {len(records)} interactions from {file_path.name}")
    return records


def extract_ims_inc_csv(file_path: Path) -> List[Dict]:
    """
    Extract IMS-INC mapping records from CSV file.
    
    Returns list of dicts with keys:
        interaction, task, sys_created_by, sys_created_on, document_id
    """
    records = []
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(dict(row))
    logger.info(f"Extracted {len(records)} IMS-INC links from {file_path.name}")
    return records


def extract_sysid_json(file_path: Path) -> List[Dict]:
    """
    Extract sys_id records from JSON file.
    
    Returns list of dicts with keys including:
        interaction, task, sys_created_by, sys_created_on, sys_id
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read().strip()
        raw_data = json.loads(content)
        
        # Handle wrapped format: {"records": [...]}
        if isinstance(raw_data, dict) and 'records' in raw_data:
            data = raw_data['records']
        elif isinstance(raw_data, list):
            data = raw_data
        else:
            data = [raw_data]
    
    logger.info(f"Extracted {len(data)} sys_id records from {file_path.name}")
    return data


# =============================================================================
# Transform Functions
# =============================================================================

def parse_user_field(value: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse 'Name (user_id)' format into (user_id, name).
    
    Examples:
        "Jackie Phrakousonh (j0p0u94)" -> ("j0p0u94", "Jackie Phrakousonh")
        "" -> (None, None)
    """
    if not value or not value.strip():
        return None, None
    
    match = re.match(r'^(.+?)\s*\(([^)]+)\)$', value.strip())
    if match:
        return match.group(2), match.group(1).strip()
    return None, value.strip()


def parse_datetime(dt_str: str) -> Optional[datetime]:
    """
    Parse datetime string in format 'MM-DD-YYYY HH:MM:SS'.
    
    Returns datetime object or None if parsing fails.
    """
    if not dt_str or not dt_str.strip():
        return None
    try:
        return datetime.strptime(dt_str.strip(), '%m-%d-%Y %H:%M:%S')
    except ValueError:
        try:
            # Try alternative format YYYY-MM-DD HH:MM:SS
            return datetime.strptime(dt_str.strip(), '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return None


def create_date_key(dt: datetime) -> int:
    """Create integer date key in YYYYMMDD format."""
    return int(dt.strftime('%Y%m%d'))


def transform_interaction(row: Dict) -> Dict:
    """
    Transform raw CSV row into normalized interaction record.
    
    Extracts user_id and tech_id from formatted strings,
    parses timestamps, and prepares dimension lookups.
    """
    user_id, user_name = parse_user_field(row.get('opened_for', ''))
    tech_id, tech_name = parse_user_field(row.get('assigned_to', ''))
    opened_dt = parse_datetime(row.get('opened_at', ''))
    updated_dt = parse_datetime(row.get('sys_updated_on', ''))
    
    return {
        'interaction_number': row.get('number', '').strip(),
        'short_description': row.get('short_description', '').strip(),
        'interaction_type': row.get('type', '').strip(),
        'work_notes': row.get('work_notes', '').strip(),
        'state': row.get('state', '').strip(),
        'location': row.get('location', '').strip(),
        'user_id': user_id,
        'user_name': user_name,
        'tech_id': tech_id,
        'tech_name': tech_name,
        'opened_at': opened_dt.isoformat() if opened_dt else None,
        'updated_at': updated_dt.isoformat() if updated_dt else None,
        'opened_date_key': create_date_key(opened_dt) if opened_dt else None,
    }


def transform_ims_inc_link(csv_row: Dict, sysid_lookup: Dict) -> Dict:
    """
    Transform IMS-INC CSV row with optional sys_id enrichment.
    
    Args:
        csv_row: Dict from ims_inc CSV
        sysid_lookup: Dict mapping (created_by, created_on) -> sys_id record
    
    Returns:
        Transformed link record with URLs if sys_ids available
    """
    key = (csv_row.get('sys_created_by', ''), csv_row.get('sys_created_on', ''))
    sysid_data = sysid_lookup.get(key, {})
    
    interaction_sysid = sysid_data.get('interaction', '')
    incident_sysid = sysid_data.get('task', '')
    
    # Build URLs from sys_ids
    base_url = "https://example.service-now.com"
    interaction_url = f"{base_url}/interaction.do?sys_id={interaction_sysid}" if interaction_sysid else None
    incident_url = f"{base_url}/incident.do?sys_id={incident_sysid}" if incident_sysid else None
    
    return {
        'interaction_number': csv_row.get('interaction', '').strip(),
        'incident_number': csv_row.get('task', '').strip(),
        'interaction_sysid': interaction_sysid,
        'incident_sysid': incident_sysid,
        'created_by': csv_row.get('sys_created_by', '').strip(),
        'created_on': csv_row.get('sys_created_on', '').strip(),
        'interaction_url': interaction_url,
        'incident_url': incident_url,
    }


def build_sysid_lookup(sysid_records: List[Dict]) -> Dict:
    """
    Build lookup dict from sys_id records keyed by (created_by, created_on).
    """
    lookup = {}
    for record in sysid_records:
        key = (record.get('sys_created_by', ''), record.get('sys_created_on', ''))
        lookup[key] = record
    return lookup


# =============================================================================
# Load Functions
# =============================================================================

def init_database(db_path: Path) -> sqlite3.Connection:
    """Initialize database with schema."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    logger.info(f"Database initialized: {db_path}")
    return conn


def load_dimension_date(conn: sqlite3.Connection, date_key: int):
    """Insert date dimension record if not exists."""
    if not date_key:
        return
    
    # Check if exists
    cursor = conn.execute("SELECT 1 FROM dim_dates WHERE date_id = ?", (date_key,))
    if cursor.fetchone():
        return
    
    # Parse and insert
    dt = datetime.strptime(str(date_key), '%Y%m%d')
    month_names = ['January', 'February', 'March', 'April', 'May', 'June',
                   'July', 'August', 'September', 'October', 'November', 'December']
    day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    conn.execute("""
        INSERT OR IGNORE INTO dim_dates 
        (date_id, full_date, year, quarter, month, month_name, week_of_year,
         day_of_month, day_of_week, day_name, is_weekend)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        date_key,
        dt.strftime('%Y-%m-%d'),
        dt.year,
        (dt.month - 1) // 3 + 1,
        dt.month,
        month_names[dt.month - 1],
        dt.isocalendar()[1],
        dt.day,
        dt.weekday(),
        day_names[dt.weekday()],
        1 if dt.weekday() >= 5 else 0
    ))


def load_interactions(conn: sqlite3.Connection, interactions: List[Dict]):
    """Load transformed interactions into database."""
    users_inserted = 0
    techs_inserted = 0
    locations_inserted = 0
    states_inserted = 0
    interactions_inserted = 0
    
    for record in interactions:
        # Load user dimension
        if record['user_id'] and record['user_name']:
            conn.execute("""
                INSERT OR IGNORE INTO dim_users (user_id, user_name)
                VALUES (?, ?)
            """, (record['user_id'], record['user_name']))
            users_inserted += conn.total_changes
        
        # Load technician dimension
        if record['tech_id'] and record['tech_name']:
            conn.execute("""
                INSERT OR IGNORE INTO dim_technicians (tech_id, tech_name)
                VALUES (?, ?)
            """, (record['tech_id'], record['tech_name']))
            techs_inserted += conn.total_changes
        
        # Load location dimension
        location_id = None
        if record['location']:
            conn.execute("""
                INSERT OR IGNORE INTO dim_locations (location_name)
                VALUES (?)
            """, (record['location'],))
            cursor = conn.execute(
                "SELECT location_id FROM dim_locations WHERE location_name = ?",
                (record['location'],)
            )
            location_id = cursor.fetchone()[0]
        
        # Load state dimension
        state_id = None
        if record['state']:
            conn.execute("""
                INSERT OR IGNORE INTO dim_states (state_name)
                VALUES (?)
            """, (record['state'],))
            cursor = conn.execute(
                "SELECT state_id FROM dim_states WHERE state_name = ?",
                (record['state'],)
            )
            state_id = cursor.fetchone()[0]
        
        # Load date dimension
        load_dimension_date(conn, record['opened_date_key'])
        
        # Load fact table
        conn.execute("""
            INSERT OR REPLACE INTO fact_interactions
            (interaction_number, short_description, interaction_type, work_notes,
             user_id, tech_id, location_id, state_id, opened_date_id,
             opened_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            record['interaction_number'],
            record['short_description'],
            record['interaction_type'],
            record['work_notes'],
            record['user_id'],
            record['tech_id'],
            location_id,
            state_id,
            record['opened_date_key'],
            record['opened_at'],
            record['updated_at'],
        ))
        interactions_inserted += 1
    
    conn.commit()
    logger.info(f"Loaded {interactions_inserted} interactions")
    logger.info(f"Dimensions: {users_inserted} users, {techs_inserted} technicians")


def load_ims_inc_links(conn: sqlite3.Connection, links: List[Dict]):
    """Load IMS-INC link records into bridge table."""
    inserted = 0
    for link in links:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO bridge_ims_inc
                (interaction_number, incident_number, interaction_sysid, incident_sysid,
                 created_by, created_on, interaction_url, incident_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                link['interaction_number'],
                link['incident_number'],
                link['interaction_sysid'],
                link['incident_sysid'],
                link['created_by'],
                link['created_on'],
                link['interaction_url'],
                link['incident_url'],
            ))
            inserted += 1
        except sqlite3.IntegrityError:
            pass  # Duplicate, skip
    
    conn.commit()
    logger.info(f"Loaded {inserted} IMS-INC links")


# =============================================================================
# Main ETL Pipeline
# =============================================================================

def run_etl(interactions_csv: Path = None, ims_inc_csv: Path = None, 
            sysid_json: Path = None):
    """
    Run the complete ETL pipeline.
    
    Args:
        interactions_csv: Path to interactions CSV (auto-detect if None)
        ims_inc_csv: Path to IMS-INC CSV (auto-detect if None)
        sysid_json: Path to sys_id JSON (auto-detect if None)
    """
    logger.info("=" * 60)
    logger.info("Starting ETL Pipeline")
    logger.info("=" * 60)
    
    # Find latest files if not specified
    if not interactions_csv:
        interactions_csv = find_latest_file("interaction_*.csv")
    if not ims_inc_csv:
        ims_inc_csv = find_latest_file("ims_inc_*.csv")
    if not sysid_json:
        sysid_json = find_latest_file("sysid_*.json")
    
    # Initialize database
    conn = init_database(DB_PATH)
    
    try:
        # Extract & Transform & Load Interactions
        if interactions_csv and interactions_csv.exists():
            logger.info(f"\n[1/3] Processing interactions: {interactions_csv.name}")
            raw_interactions = extract_interactions_csv(interactions_csv)
            transformed = [transform_interaction(r) for r in raw_interactions]
            load_interactions(conn, transformed)
        else:
            logger.warning("No interactions CSV found")
        
        # Build sys_id lookup
        sysid_lookup = {}
        if sysid_json and sysid_json.exists():
            logger.info(f"\n[2/3] Processing sys_ids: {sysid_json.name}")
            sysid_records = extract_sysid_json(sysid_json)
            sysid_lookup = build_sysid_lookup(sysid_records)
            logger.info(f"Built lookup with {len(sysid_lookup)} sys_id records")
        else:
            logger.warning("No sys_id JSON found")
        
        # Extract & Transform & Load IMS-INC Links
        if ims_inc_csv and ims_inc_csv.exists():
            logger.info(f"\n[3/3] Processing IMS-INC links: {ims_inc_csv.name}")
            raw_links = extract_ims_inc_csv(ims_inc_csv)
            transformed_links = [transform_ims_inc_link(r, sysid_lookup) for r in raw_links]
            load_ims_inc_links(conn, transformed_links)
        else:
            logger.warning("No IMS-INC CSV found")
        
        logger.info("\n" + "=" * 60)
        logger.info("ETL Pipeline Complete")
        logger.info("=" * 60)
        
    finally:
        conn.close()


def show_stats():
    """Display database statistics."""
    conn = sqlite3.connect(DB_PATH)
    
    tables = [
        ('dim_users', 'Users'),
        ('dim_technicians', 'Technicians'),
        ('dim_locations', 'Locations'),
        ('dim_states', 'States'),
        ('dim_dates', 'Dates'),
        ('fact_interactions', 'Interactions'),
        ('bridge_ims_inc', 'IMS-INC Links'),
    ]
    
    print("\n" + "=" * 40)
    print("Database Statistics")
    print("=" * 40)
    
    for table, label in tables:
        try:
            cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"{label:20} {count:>10,}")
        except sqlite3.OperationalError:
            print(f"{label:20} {'N/A':>10}")
    
    print("=" * 40)
    conn.close()


# =============================================================================
# CLI Entry Point
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='IT Support Interactions ETL Pipeline'
    )
    parser.add_argument('--stats', action='store_true',
                        help='Show database statistics')
    parser.add_argument('--latest', action='store_true',
                        help='Process only latest export files')
    parser.add_argument('--interactions', type=Path,
                        help='Path to interactions CSV file')
    parser.add_argument('--ims-inc', type=Path,
                        help='Path to IMS-INC CSV file')
    parser.add_argument('--sysid', type=Path,
                        help='Path to sys_id JSON file')
    
    args = parser.parse_args()
    
    if args.stats:
        show_stats()
    else:
        run_etl(
            interactions_csv=args.interactions,
            ims_inc_csv=args.ims_inc,
            sysid_json=args.sysid,
        )
        show_stats()


if __name__ == '__main__':
    main()
