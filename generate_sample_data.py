"""
Generate anonymized sample data for IT Support Interactions demonstration.
Creates realistic-looking but fake data for CSV/JSON exports.
"""

import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

# Fake first names
FIRST_NAMES = [
    "Alex", "Jordan", "Taylor", "Morgan", "Casey", "Riley", "Quinn", "Avery",
    "Sage", "River", "Parker", "Drew", "Jamie", "Blake", "Cameron", "Dakota",
    "Reese", "Hayden", "Peyton", "Charlie", "Skylar", "Finley", "Rowan", "Ellis",
    "Sam", "Robin", "Jesse", "Dana", "Kerry", "Pat", "Lee", "Kim", "Terry", "Corey"
]

# Fake last names
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Anderson", "Taylor", "Thomas", "Moore", "Jackson",
    "Martin", "Lee", "Thompson", "White", "Harris", "Clark", "Lewis", "Robinson",
    "Walker", "Hall", "Young", "Allen", "King", "Wright", "Scott", "Green", "Baker"
]

# Sample locations (fictional building names)
LOCATIONS = [
    "100 MAIN CAMPUS DR",
    "200 TECHNOLOGY BLVD",
    "300 INNOVATION WAY",
    "400 ENTERPRISE AVE",
    "500 CORPORATE CENTER",
    "600 TECH PARK LANE",
    "700 BUSINESS SQUARE"
]

# Tech Support short descriptions (realistic but generic)
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

# Interaction states
STATES = [
    ("Closed Complete", 75),
    ("Work in Progress", 15),
    ("Closed Abandoned", 5),
    ("Pending", 5),
]


def generate_user_id():
    """Generate a fake user ID like 'a0b1c2d'"""
    letters = 'abcdefghijklmnopqrstuvwxyz'
    digits = '0123456789'
    return f"{random.choice(letters)}{random.choice(digits)}{random.choice(letters)}{random.choice(digits)}{random.choice(letters)}{random.choice(digits)}{random.choice(letters)}"


def generate_fake_name():
    """Generate a fake full name and user ID"""
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    user_id = generate_user_id()
    return f"{first} {last} ({user_id})", user_id


def generate_tech_name():
    """Generate a technician name"""
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    user_id = generate_user_id()
    return f"{first} {last} ({user_id})", user_id


def weighted_choice(choices):
    """Select from choices based on weights"""
    total = sum(weight for _, weight in choices)
    r = random.uniform(0, total)
    upto = 0
    for choice, weight in choices:
        upto += weight
        if r <= upto:
            return choice
    return choices[-1][0]


def generate_short_description():
    """Generate a random short description"""
    roll = random.random()
    if roll < 0.60:
        return random.choice(TECH_SUPPORT_DESCRIPTIONS)
    elif roll < 0.85:
        return random.choice(EQUIPMENT_PICKUP_DESCRIPTIONS)
    elif roll < 0.95:
        return random.choice(RETURN_EQUIPMENT_DESCRIPTIONS)
    else:
        return random.choice(OTHER_DESCRIPTIONS)


def generate_interactions(num_records=100, base_date=None):
    """Generate fake interaction records"""
    if base_date is None:
        base_date = datetime.now()
    
    # Create a pool of technicians (fixed team)
    technicians = [generate_tech_name() for _ in range(8)]
    
    records = []
    ims_number = 1350000 + random.randint(0, 5000)
    
    for i in range(num_records):
        ims_number += random.randint(1, 5)
        
        # Random time within the day
        hours_offset = random.uniform(0, 8)  # 8 hour work day
        minutes_offset = random.randint(0, 59)
        record_time = base_date.replace(hour=8, minute=0, second=0) + timedelta(hours=hours_offset, minutes=minutes_offset)
        
        opened_for_name, opened_for_id = generate_fake_name()
        tech_name, tech_id = random.choice(technicians)
        
        # Updated time is a few minutes after opened
        updated_time = record_time + timedelta(minutes=random.randint(5, 45))
        
        record = {
            "number": f"IMS{ims_number}",
            "opened_at": record_time.strftime("%m-%d-%Y %H:%M:%S"),
            "short_description": generate_short_description(),
            "opened_for": opened_for_name,
            "state": weighted_choice(STATES),
            "type": "Walk-up",
            "assigned_to": tech_name,
            "sys_updated_on": updated_time.strftime("%m-%d-%Y %H:%M:%S"),
            "location": random.choice(LOCATIONS),
            "work_notes": ""
        }
        records.append(record)
    
    return records, technicians


def generate_ims_inc_links(interactions, technicians, link_rate=0.3):
    """Generate fake IMS to Incident links"""
    links = []
    inc_number = 50980000 + random.randint(0, 5000)
    
    for interaction in interactions:
        # Only some interactions get linked to incidents
        if random.random() < link_rate:
            inc_number += random.randint(1, 10)
            
            # Parse the interaction time
            opened_at = datetime.strptime(interaction["opened_at"], "%m-%d-%Y %H:%M:%S")
            created_time = opened_at + timedelta(minutes=random.randint(2, 30))
            
            # Get a random technician ID
            _, tech_id = random.choice(technicians)
            
            link = {
                "interaction": interaction["number"],
                "task": f"INC{inc_number}",
                "sys_created_by": tech_id,
                "sys_created_on": created_time.strftime("%m-%d-%Y %H:%M:%S"),
                "document_id": f"Incident: INC{inc_number}"
            }
            links.append(link)
    
    return links


def generate_sysid_data(interactions, ims_inc_links):
    """Generate fake sysid JSON data"""
    import hashlib
    
    sysid_records = []
    
    for link in ims_inc_links:
        # Generate fake sys_ids (32 char hex strings)
        interaction_sysid = hashlib.md5(f"{link['interaction']}_fake".encode()).hexdigest()
        task_sysid = hashlib.md5(f"{link['task']}_fake".encode()).hexdigest()
        
        record = {
            "interaction": interaction_sysid,
            "task": task_sysid,
            "sys_created_by": link["sys_created_by"],
            "sys_created_on": link["sys_created_on"].replace("-", "-")
        }
        sysid_records.append(record)
    
    return sysid_records


def save_sample_data(output_dir="exports/sample", num_records=100):
    """Generate and save all sample data files"""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Generate base date
    base_date = datetime(2025, 12, 18, 8, 0, 0)
    
    # Generate interactions
    interactions, technicians = generate_interactions(num_records, base_date)
    
    # Generate IMS-INC links
    ims_inc_links = generate_ims_inc_links(interactions, technicians)
    
    # Generate sysid data
    sysid_data = generate_sysid_data(interactions, ims_inc_links)
    
    timestamp = base_date.strftime("%Y%m%d_%H%M%S")
    
    # Save interactions CSV
    interactions_file = output_path / f"interaction_{timestamp}.csv"
    with open(interactions_file, "w", newline="") as f:
        fieldnames = ["number", "opened_at", "short_description", "opened_for", "state", "type", "assigned_to", "sys_updated_on", "location", "work_notes"]
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(interactions)
    print(f"✓ Created {interactions_file} ({len(interactions)} records)")
    
    # Save IMS-INC CSV
    ims_inc_file = output_path / f"ims_inc_{timestamp}.csv"
    with open(ims_inc_file, "w", newline="") as f:
        fieldnames = ["interaction", "task", "sys_created_by", "sys_created_on", "document_id"]
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(ims_inc_links)
    print(f"✓ Created {ims_inc_file} ({len(ims_inc_links)} records)")
    
    # Save sysid JSON
    sysid_file = output_path / f"sysid_{timestamp}.json"
    with open(sysid_file, "w") as f:
        for record in sysid_data:
            f.write(json.dumps(record) + "\n")
    print(f"✓ Created {sysid_file} ({len(sysid_data)} records)")
    
    return {
        "interactions": len(interactions),
        "ims_inc_links": len(ims_inc_links),
        "sysid_records": len(sysid_data)
    }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate sample IT support interaction data")
    parser.add_argument("-n", "--num-records", type=int, default=100, help="Number of interaction records to generate")
    parser.add_argument("-o", "--output-dir", default="exports/sample", help="Output directory for sample files")
    parser.add_argument("--seed", type=int, help="Random seed for reproducibility")
    
    args = parser.parse_args()
    
    if args.seed:
        random.seed(args.seed)
        print(f"Using random seed: {args.seed}")
    
    print(f"\n🔄 Generating {args.num_records} sample records...\n")
    stats = save_sample_data(args.output_dir, args.num_records)
    
    print(f"\n✅ Sample data generation complete!")
    print(f"   Interactions: {stats['interactions']}")
    print(f"   IMS→INC Links: {stats['ims_inc_links']}")
    print(f"   SysID Records: {stats['sysid_records']}")
    print(f"\n📁 Files saved to: {args.output_dir}/")
