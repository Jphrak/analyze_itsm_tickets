"""Generate anonymized sample data for IT Support Interactions demonstration.

Creates realistic-looking but entirely fake data for CSV/JSON exports.
No real user, technician, or ticket data is used.

Usage:
    python generate_sample_data.py
    python generate_sample_data.py -n 500 -o exports/sample --seed 42
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
from pathlib import Path

import pendulum

# =============================================================================
# Constants
# =============================================================================

FIRST_NAMES = [
    "Alex", "Jordan", "Taylor", "Morgan", "Casey", "Riley", "Quinn", "Avery",
    "Sage", "River", "Parker", "Drew", "Jamie", "Blake", "Cameron", "Dakota",
    "Reese", "Hayden", "Peyton", "Charlie", "Skylar", "Finley", "Rowan", "Ellis",
    "Sam", "Robin", "Jesse", "Dana", "Kerry", "Pat", "Lee", "Kim", "Terry", "Corey",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Anderson", "Taylor", "Thomas", "Moore", "Jackson",
    "Martin", "Lee", "Thompson", "White", "Harris", "Clark", "Lewis", "Robinson",
    "Walker", "Hall", "Young", "Allen", "King", "Wright", "Scott", "Green", "Baker",
]

LOCATIONS = [
    "100 MAIN CAMPUS DR",
    "200 TECHNOLOGY BLVD",
    "300 INNOVATION WAY",
    "400 ENTERPRISE AVE",
    "500 CORPORATE CENTER",
    "600 TECH PARK LANE",
    "700 BUSINESS SQUARE",
]

TECH_SUPPORT_DESCRIPTIONS = [
    "Tech Support | VPN connection issues",
    "Tech Support | Password reset assistance",
    "Tech Support | Email not syncing on mobile device",
    "Tech Support | Unable to connect to network printer",
    "Tech Support | Software installation request",
    "Tech Support | Slow computer performance",
    "Tech Support | Monitor display issues",
    "Tech Support | Keyboard not working properly",
    "Tech Support | Multi-factor authentication setup",
    "Tech Support | Teams audio not working in calls",
    "Tech Support | Outlook calendar sync issues",
    "Tech Support | Remote desktop connection failed",
    "Tech Support | Application keeps crashing",
    "Tech Support | Wi-Fi connectivity problems",
    "Tech Support | File access permissions issue",
    "Tech Support | Browser not loading pages",
    "Tech Support | USB device not recognized",
    "Tech Support | Bluetooth pairing issues",
    "Tech Support | System update stuck",
    "Tech Support | OneDrive sync errors",
    "Tech Support | New device setup assistance",
    "Tech Support | Account lockout recovery",
    "Tech Support | Screen sharing not working",
    "Tech Support | Zoom sign-in problems",
    "Tech Support | Badge access not working",
]

EQUIPMENT_PICKUP_DESCRIPTIONS = [
    "Equipment Pickup - Computer or Monitor | Laptop pickup",
    "Equipment Pickup - Computer or Monitor | Desktop computer",
    "Equipment Pickup - Tech Accessory | Wireless mouse",
    "Equipment Pickup - Tech Accessory | Wired headset",
    "Equipment Pickup - Tech Accessory | USB-C hub",
    "Equipment Pickup - Tech Accessory | Laptop charger",
    "Equipment Pickup - Tech Accessory | Privacy screen",
    "Equipment Pickup - Tech Accessory | Wireless keyboard",
    "Equipment Pickup - Tech Accessory | Monitor stand",
    "Equipment Pickup - Tech Accessory | Docking station",
]

RETURN_EQUIPMENT_DESCRIPTIONS = [
    "Return Equipment - Computer or Monitor | Old laptop",
    "Return Equipment - Computer or Monitor | Desktop computer",
    "Return Equipment - Tech Accessory | Old keyboard and mouse",
    "Return Equipment - Tech Accessory | Broken headset",
    "Return Equipment - Tech Accessory | Old charger",
]

OTHER_DESCRIPTIONS = [
    "Cancel | User resolved issue",
    "Abandon | User not present",
    "Cancel | Issue fixed while waiting",
]

STATES: list[tuple[str, int]] = [
    ("Closed Complete", 75),
    ("Work in Progress", 15),
    ("Closed Abandoned", 5),
    ("Pending", 5),
]


# =============================================================================
# Generators
# =============================================================================

def generate_user_id() -> str:
    """Generate a fake alphanumeric user ID like 'a0b1c2d'.

    Returns:
        Seven-character string alternating letters and digits.
    """
    letters = "abcdefghijklmnopqrstuvwxyz"
    digits = "0123456789"
    return "".join([
        random.choice(letters), random.choice(digits),
        random.choice(letters), random.choice(digits),
        random.choice(letters), random.choice(digits),
        random.choice(letters),
    ])


def generate_fake_name() -> tuple[str, str]:
    """Generate a fake full name in 'First Last (user_id)' format.

    Returns:
        Tuple of (formatted_name, user_id).
    """
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    user_id = generate_user_id()
    return f"{first} {last} ({user_id})", user_id


def weighted_choice(choices: list[tuple[str, int]]) -> str:
    """Select a string from weighted choices.

    Args:
        choices: List of (value, weight) tuples.

    Returns:
        Selected value based on weight distribution.
    """
    total = sum(weight for _, weight in choices)
    r = random.uniform(0, total)
    upto = 0.0
    for choice, weight in choices:
        upto += weight
        if r <= upto:
            return choice
    return choices[-1][0]


def generate_short_description() -> str:
    """Generate a random ticket short description.

    Returns:
        Short description string from one of the predefined categories.
    """
    roll = random.random()
    if roll < 0.60:
        return random.choice(TECH_SUPPORT_DESCRIPTIONS)
    if roll < 0.85:
        return random.choice(EQUIPMENT_PICKUP_DESCRIPTIONS)
    if roll < 0.95:
        return random.choice(RETURN_EQUIPMENT_DESCRIPTIONS)
    return random.choice(OTHER_DESCRIPTIONS)


# =============================================================================
# Data Generation
# =============================================================================

def generate_interactions(
    num_records: int = 100,
    base_date: pendulum.DateTime | None = None,
) -> tuple[list[dict[str, str]], list[tuple[str, str]]]:
    """Generate fake interaction records and a technician pool.

    Args:
        num_records: Number of interaction records to generate.
        base_date: Base date for record timestamps. Defaults to today.

    Returns:
        Tuple of (interaction records list, technician name/id tuples list).
    """
    if base_date is None:
        base_date = pendulum.today(tz="UTC")

    technicians = [generate_fake_name() for _ in range(8)]

    records: list[dict[str, str]] = []
    ims_number = 1350000 + random.randint(0, 5000)

    for _ in range(num_records):
        ims_number += random.randint(1, 5)

        hours_offset = random.uniform(0, 8)
        minutes_offset = random.randint(0, 59)
        record_time = base_date.set(hour=8, minute=0, second=0).add(
            hours=int(hours_offset),
            minutes=minutes_offset,
        )
        updated_time = record_time.add(minutes=random.randint(5, 45))

        opened_for_name, _ = generate_fake_name()
        tech_name, _ = random.choice(technicians)

        records.append({
            "number": f"IMS{ims_number}",
            "opened_at": record_time.format("MM-DD-YYYY HH:mm:ss"),
            "short_description": generate_short_description(),
            "opened_for": opened_for_name,
            "state": weighted_choice(STATES),
            "type": "Walk-up",
            "assigned_to": tech_name,
            "sys_updated_on": updated_time.format("MM-DD-YYYY HH:mm:ss"),
            "location": random.choice(LOCATIONS),
            "work_notes": "",
        })

    return records, technicians


def generate_ims_inc_links(
    interactions: list[dict[str, str]],
    technicians: list[tuple[str, str]],
    link_rate: float = 0.3,
) -> list[dict[str, str]]:
    """Generate fake IMS to Incident link records.

    Args:
        interactions: List of interaction record dicts.
        technicians: List of (formatted_name, user_id) tuples.
        link_rate: Fraction of interactions that get an incident link.

    Returns:
        List of IMS-INC link dicts.
    """
    links: list[dict[str, str]] = []
    inc_number = 50980000 + random.randint(0, 5000)

    for interaction in interactions:
        if random.random() >= link_rate:
            continue

        inc_number += random.randint(1, 10)
        opened_at = pendulum.from_format(
            interaction["opened_at"], "MM-DD-YYYY HH:mm:ss", tz="UTC"
        )
        created_time = opened_at.add(minutes=random.randint(2, 30))
        _, tech_id = random.choice(technicians)

        links.append({
            "interaction": interaction["number"],
            "task": f"INC{inc_number}",
            "sys_created_by": tech_id,
            "sys_created_on": created_time.format("MM-DD-YYYY HH:mm:ss"),
            "document_id": f"Incident: INC{inc_number}",
        })

    return links


def generate_sysid_data(
    ims_inc_links: list[dict[str, str]],
) -> list[dict[str, str]]:
    """Generate fake sys_id JSON records for IMS-INC links.

    Args:
        ims_inc_links: List of IMS-INC link dicts.

    Returns:
        List of sysid record dicts with fake 32-char hex IDs.
    """
    sysid_records: list[dict[str, str]] = []
    for link in ims_inc_links:
        interaction_sysid = hashlib.md5(f"{link['interaction']}_fake".encode()).hexdigest()  # noqa: S324 - not used for security, just fake IDs
        task_sysid = hashlib.md5(f"{link['task']}_fake".encode()).hexdigest()  # noqa: S324 - not used for security, just fake IDs
        sysid_records.append({
            "interaction": interaction_sysid,
            "task": task_sysid,
            "sys_created_by": link["sys_created_by"],
            "sys_created_on": link["sys_created_on"],
        })
    return sysid_records


# =============================================================================
# Save
# =============================================================================

def save_sample_data(
    output_dir: str = "exports/sample",
    num_records: int = 100,
) -> dict[str, int]:
    """Generate and save all sample data files to disk.

    Creates three files in output_dir:
    - interaction_<timestamp>.csv
    - ims_inc_<timestamp>.csv
    - sysid_<timestamp>.json (NDJSON format)

    Args:
        output_dir: Directory path for output files.
        num_records: Number of interaction records to generate.

    Returns:
        Dict with counts for interactions, ims_inc_links, and sysid_records.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    base_date = pendulum.datetime(2025, 12, 18, 8, 0, 0, tz="UTC")
    interactions, technicians = generate_interactions(num_records=num_records, base_date=base_date)
    ims_inc_links = generate_ims_inc_links(interactions=interactions, technicians=technicians)
    sysid_data = generate_sysid_data(ims_inc_links=ims_inc_links)

    timestamp = base_date.format("YYYYMMDD_HHmmss")

    interactions_file = output_path / f"interaction_{timestamp}.csv"
    interaction_fields = [
        "number", "opened_at", "short_description", "opened_for",
        "state", "type", "assigned_to", "sys_updated_on", "location", "work_notes",
    ]
    with interactions_file.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=interaction_fields, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(interactions)
    print(f"Created {interactions_file} ({len(interactions)} records)")

    ims_inc_file = output_path / f"ims_inc_{timestamp}.csv"
    ims_inc_fields = ["interaction", "task", "sys_created_by", "sys_created_on", "document_id"]
    with ims_inc_file.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=ims_inc_fields, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(ims_inc_links)
    print(f"Created {ims_inc_file} ({len(ims_inc_links)} records)")

    sysid_file = output_path / f"sysid_{timestamp}.json"
    with sysid_file.open("w") as f:
        for record in sysid_data:
            f.write(json.dumps(record) + "\n")
    print(f"Created {sysid_file} ({len(sysid_data)} records)")

    return {
        "interactions": len(interactions),
        "ims_inc_links": len(ims_inc_links),
        "sysid_records": len(sysid_data),
    }


# =============================================================================
# CLI
# =============================================================================

def main() -> None:
    """Parse arguments and generate sample data files."""
    parser = argparse.ArgumentParser(
        description="Generate sample IT support interaction data",
    )
    parser.add_argument(
        "-n", "--num-records",
        type=int,
        default=100,
        help="Number of interaction records to generate",
    )
    parser.add_argument(
        "-o", "--output-dir",
        default="exports/sample",
        help="Output directory for sample files",
    )
    parser.add_argument(
        "--seed",
        type=int,
        help="Random seed for reproducibility",
    )

    args = parser.parse_args()

    if args.seed:
        random.seed(args.seed)
        print(f"Using random seed: {args.seed}")

    print(f"\nGenerating {args.num_records} sample records...\n")
    stats = save_sample_data(output_dir=args.output_dir, num_records=args.num_records)

    print(f"\nSample data generation complete!")
    print(f"   Interactions:  {stats['interactions']}")
    print(f"   IMS->INC Links: {stats['ims_inc_links']}")
    print(f"   SysID Records:  {stats['sysid_records']}")
    print(f"\nFiles saved to: {args.output_dir}/")


if __name__ == "__main__":
    main()
