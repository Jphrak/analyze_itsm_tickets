# IT Support Interactions Analytics

A data analytics portfolio project demonstrating ETL pipeline design, star schema data modeling, and SQL-based analytics for IT support ticket data.

## 🎯 Project Overview

This project showcases core data engineering and analytics skills:

- **ETL Pipeline**: Python-based Extract, Transform, Load workflow
- **Data Modeling**: Normalized star schema design for analytical queries
- **SQL Analytics**: Complex queries for business insights
- **Data Visualization**: Jupyter notebooks with matplotlib visualizations

## 📊 What This Project Analyzes

IT support interaction data including:
- Support ticket volume trends
- Technician workload distribution
- Location-based support demand
- Ticket resolution metrics
- IMS to Incident conversion rates

## 🏗️ Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   CSV Exports   │────▶│   ingest.py     │────▶│ interactions.db │
│   JSON Exports  │     │   (ETL Pipeline)│     │ (Star Schema)   │
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                         │
                                                         ▼
                                                ┌─────────────────┐
                                                │ analysis.ipynb  │
                                                │ (SQL Analytics) │
                                                └─────────────────┘
```

## 📁 Project Structure

```
analyze_itsm_tickets/
├── ingest.py                  # Main ETL pipeline
├── generate_sample_data.py    # Anonymized sample data generator
├── analysis.ipynb             # SQL analytics notebook
├── interactions.db            # SQLite database (generated, gitignored)
├── exports/                   # Source data files (gitignored)
│   ├── interaction_*.csv      # Main interaction data
│   ├── ims_inc_*.csv          # Interaction-Incident links
│   └── sysid_*.json           # System IDs for URLs
├── SCHEMA.md                  # Star schema documentation
├── requirements.txt           # Python dependencies
└── pyproject.toml             # Ruff linting configuration
```

## ⭐ Star Schema Design

The database uses a dimensional model optimized for analytical queries:

### Dimension Tables
| Table | Description |
|-------|-------------|
| `dim_users` | End users who created support tickets |
| `dim_technicians` | IT support staff |
| `dim_locations` | Office locations |
| `dim_states` | Ticket status values |
| `dim_dates` | Date dimension for time analysis |

### Fact Table
| Table | Description |
|-------|-------------|
| `fact_interactions` | Central fact table with metrics |

### Bridge Table
| Table | Description |
|-------|-------------|
| `bridge_ims_inc` | Links interactions to incidents |

See [docs/SCHEMA.md](docs/SCHEMA.md) for the complete ER diagram.

## 🚀 Quick Start

### 1. Install Dependencies

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Generate Sample Data

No real data is committed to this repo. Generate anonymized sample data first:

```bash
# Generate 100 sample records (default)
python generate_sample_data.py

# Generate more records with a fixed seed for reproducibility
python generate_sample_data.py -n 500 --seed 42 -o exports/sample
```

### 3. Run the ETL Pipeline

```bash
# Process sample data (auto-detects latest files in exports/)
python ingest.py --exports-dir exports/sample

# View database statistics
python ingest.py --stats
```

### 4. Explore Analytics

Open `analysis.ipynb` in Jupyter or VS Code to explore:
- Daily interaction trends
- Location-based analysis
- Technician workload metrics
- Conversion rate analysis

## 📈 Sample Analytics

### Daily Interaction Volume
```sql
SELECT 
    d.full_date,
    d.day_name,
    COUNT(*) as interactions
FROM fact_interactions f
JOIN dim_dates d ON f.opened_date_id = d.date_id
GROUP BY d.date_id
ORDER BY d.full_date;
```

### Technician Workload
```sql
SELECT 
    t.tech_name,
    COUNT(*) as total_tickets,
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT f.opened_date_id), 1) as avg_per_day
FROM fact_interactions f
JOIN dim_technicians t ON f.tech_id = t.tech_id
GROUP BY f.tech_id
ORDER BY total_tickets DESC;
```

### IMS to Incident Conversion
```sql
SELECT 
    t.tech_name,
    COUNT(DISTINCT f.interaction_number) as total_ims,
    COUNT(DISTINCT b.incident_number) as total_incidents,
    ROUND(100.0 * COUNT(DISTINCT b.incident_number) / 
          COUNT(DISTINCT f.interaction_number), 2) as conversion_pct
FROM fact_interactions f
JOIN dim_technicians t ON f.tech_id = t.tech_id
LEFT JOIN bridge_ims_inc b ON f.interaction_number = b.interaction_number
GROUP BY f.tech_id
HAVING total_ims >= 50;
```

## 🛠️ Technologies Used

- **Python 3.x** - ETL pipeline and data processing
- **SQLite** - Lightweight analytical database
- **pandas** - Data manipulation and analysis
- **matplotlib** - Data visualization
- **Jupyter** - Interactive analytics notebooks

## 📚 Key Concepts Demonstrated

1. **ETL Design Patterns**
   - Modular extract/transform/load functions
   - Idempotent data loading
   - Error handling and logging

2. **Data Modeling**
   - Star schema for analytics
   - Dimension vs fact tables
   - Bridge tables for many-to-many relationships

3. **SQL Analytics**
   - Window functions
   - CTEs (Common Table Expressions)
   - Aggregations and grouping
   - JOIN patterns

## 📄 License

This project is for educational and portfolio demonstration purposes.

---

*Built as a portfolio project demonstrating data analytics and engineering skills.*
