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

# Data Exploration Tool

This repository includes a distrobution of BeeKeeper Studio Portable. This project is in no way affiliated with Beekeeper Studio,
nor is any person presently involved with the development of this project, but they make a very easy to use SQLite browser that does
not require you to install any additional software locally to your computer to use.   

Run /Beekeper Portable Data Browser/Beekeeper-Studio-5.6.0-portable.exe  

If prompted enter the following:  

Connection type: SQLite  

Database File: *Choose your coldCounter.db file*  

Save your connection for later use.  
*After running for first time, you can just open the coldCounter.db file directly* 


# Database Design

coldCounter uses a dimensional modeling approach designed for analytical queries.

sample coldCounter.db provided in repository is updated as of most recent commit.

Primary tables include:

### Raw Tables

arrests 
    - arrest data from the Deportation Data Project

detainers
    - data from the Deportation Data Project pertaining to ICE contact with law enforcement.

detention_stays
    - individual based reporting of detention data from the Deportation Data Project

detention_stints
    - encounter based reporting of detention data from the Deportation Data Project

### Fact Tables

fact_hold_rooms
    - contains aggregate data regarding "hold room" usage
    - Compares against ICE National Detention Standards for hold room usage. 
        - “A detainee may not be held in a hold room for more than 12 hours.”
        U.S. Immigration & Customs Enf’t, National Detention Standards 2025, § 2.5 Hold Rooms in Detention Facilities, at 32 (2025), https://www.ice.gov/doclib/detention-standards/2025/nds2025.pdf
        - “Unaccompanied minors (under 18 years), persons over the age of 70 years, females with children, and family groups will not be placed in hold rooms, unless they have shown or threatened violent behavior, have criminal convictions involving violence, or have given staff articulable grounds to expect an escape attempt.”
        U.S. Immigration & Customs Enf’t, National Detention Standards 2025, § 2.5 Hold Rooms in Detention Facilities, at 32 (2025), https://www.ice.gov/doclib/detention-standards/2025/nds2025.pdf

fact_detention_facilities
    - contains aggregate detention information from within the scope of the currently available data by facility


### Dimension Tables

dim_ncic_offense_codes
    - contains categorized NCIC offense codes used in ICOTS, obtained from ICAOS
    
 
---

# Data Sources

coldCounter aggregates immigration detention data published by The Deportation Data Project (https://deportationdata.org/).

The project relies on publicly documented datasets that have been compiled and structured for research and public transparency purposes.

All ingestion reference datasets available through deportationdata.org, and the NCIC offense code classifications found at https://support.interstatecompact.org/hc/en-us/articles/360046201293-What-NCIC-Offense-Codes-are-used-in-ICOTS.  
  
### Resources 

  - deportation data project webinar: https://deportationdata.org/docs/ice.html#webinar
  - deportation data project FAQ: https://deportationdata.org/docs/ice.html#faq
  - deporation data project data guide: https://deportationdata.org/guide.html

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
python *your directory*/setup/build_coldCounter.py
```

---

# Reproducibility

coldCounter is designed to allow anyone to rebuild the database from source datasets.

Steps:

1. Clone the repository  
2. Extract to your desired installation directory  
3. Run the install_or_refresh_coldCounter.bat batch file
4. The SQLite database will be generated locally  

This workflow ensures that analytical results can be independently verified, and ensures transparency of data accuracy through consistency.  
The coldCounter.db included in this repository is up to date as of the most recent commit. The batch file can be run at any time to rebuild the data with the most recently available datasets from the Deportation Data Project.

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

- additional dimension tables
- improved database schema design
- SQLite query libraries
- data validation tools
- documentation improvements

Pull requests and issue reports are encouraged.

https://nocampscolorado.org