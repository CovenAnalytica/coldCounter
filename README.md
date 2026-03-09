# coldCounter

**coldCounter** is an open-source data infrastructure project that aggregates, normalizes, and documents immigration detention data in the United States.

The project produces a portable **SQLite data mart** that allows researchers, journalists, policy analysts, and civil rights organizations to explore immigration detention records in a transparent and reproducible way.

coldCounter was created on **March 4, 2026** by **No Concentration Camps in Colorado**.

---

# Purpose

Immigration enforcement datasets are often difficult to analyze due to:

- inconsistent formatting
- fragmented publication
- differing classification systems

coldCounter addresses these issues by providing a reproducible database that:

- Aggregates immigration detention datasets published by **deportationdata.org**
- Normalizes offense classification systems
- Structures data into analytical tables
- Enables reproducible research workflows

The goal is to make enforcement data **transparent, analyzable, and portable**.

---

# System Overview

coldCounter consists of two primary components:

### 1. Database Construction
Python ETL scripts build and populate a SQLite database containing normalized detention data sourced from **deportationdata.org**.

### 2. Analytical Reporting
The database structure enables researchers to generate statistical summaries and facility-level reports using datasets published by **deportationdata.org**.

---

# Repository Structure

```
coldCounter
│
├─ Setup
│  ├─ build script to create or refresh coldCounter.db
│├─ Beekeeper Portable Data Browser
│  ├─ data exploration tool
├─ coldcounter.db
│
└─ README.md
```

---

# Database Design

coldCounter uses a dimensional modeling approach designed for analytical queries.

Primary tables include:

### Dimension Tables

dim_person  
Basic demographic and classification attributes associated with a detention record.

dim_facility  
Information about detention facilities.

dim_offense  
Normalized offense classification codes derived from datasets published by deportationdata.org.

### Fact Tables

fact_detention_events  
Event level detention records linked to dimension tables through foreign keys.

This structure allows analysts to efficiently perform queries across large enforcement datasets.

---

# Data Sources

coldCounter aggregates immigration detention data published by deportationdata.org.

The project relies on publicly documented datasets that have been compiled and structured by deportationdata.org for research and public transparency purposes.

All ingestion scripts reference datasets available through deportationdata.org.

---

# ETL Pipeline

The ETL workflow follows three stages.

### 1. Ingest

Raw datasets are downloaded and stored in the data directory.

### 2. Normalize

Scripts standardize column names, convert inconsistent formats, and map offense codes to normalized classification tables.

### 3. Load

Normalized datasets are inserted into the SQLite data mart.

The entire process can be executed from a single build script.

Example:

```
python setup/build_coldCounter.py
```

---

# Reproducibility

coldCounter is designed to allow anyone to rebuild the database from source datasets.

Steps:

1. Clone the repository  
2. Download the datasets referenced from deportationdata.org  
3. Run the ETL build script  
4. The SQLite database will be generated locally  

This workflow ensures that analytical results can be independently verified.

---

# Intended Users

This project is designed for:

- journalists
- academic researchers
- policy analysts
- civil rights organizations
- data scientists studying immigration enforcement

---

# License

This project is released under an open source license.

Users should verify the licensing terms of datasets published by deportationdata.org before redistribution.

---

# Disclaimer

coldCounter is an independent data infrastructure project.

It organizes datasets published by deportationdata.org for the purpose of analysis and transparency.

The project does not modify the underlying records beyond normalization required for database structure.

---

# Contributing

Contributions are welcome.

Suggested areas for contribution include:

- additional normalization scripts
- improved database schema design
- analytical query libraries
- data validation tools
- documentation improvements

Pull requests and issue reports are encouraged.

nocampscolorado.org