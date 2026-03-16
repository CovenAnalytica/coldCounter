# -*- coding: utf-8 -*-
"""
build_coldCounter.py

Unified ETL pipeline for the coldCounter database.

Pipeline Stages
----------------
0. Load NCIC offense code dimension table
1. Ingest public ICE datasets from the Deportation Data Project
2. Build hold room statistics fact table
3. Build facility statistics fact table
4. Run sanity checks

Author: DG - NOCCC
"""

import sqlite3
import uuid
import pandas as pd
import requests
from io import BytesIO
from datetime import datetime
from pathlib import Path
import os
from pyfiglet import Figlet
from colorama import Fore, Style, init
import sys
import time
import requests
from bs4 import BeautifulSoup
import re

# --------------------------------------------------
# PATH CONFIGURATION
# --------------------------------------------------

current_dir = Path(__file__).parent
workspace_root = current_dir.parent

db_path = workspace_root / "coldCounter.db"
ncic_excel = workspace_root / "data" / "ICOTS_NCIC_OffenseCodesList.xlsx"
holdroom_research_csv = workspace_root / "data" / "noccc_holdroom_research.csv"
holdroom_office_mapping_csv = workspace_root / "data" / "holdroom_office_mapping.csv"


# --------------------------------------------------
# DATASETS
# --------------------------------------------------

datasets = [
    {"url": "https://github.com/deportationdata/ice/raw/refs/heads/main/data/arrests-latest.xlsx", "table": "raw_arrests"},
    {"url": "https://github.com/deportationdata/ice/raw/refs/heads/main/data/detainers-latest.xlsx", "table": "raw_detainers"},
    {"url": "https://github.com/deportationdata/ice/raw/refs/heads/main/data/detention-stints-latest.xlsx", "table": "raw_detention_stints"},
    {"url": "https://github.com/deportationdata/ice/raw/refs/heads/main/data/detention-stays-latest.xlsx", "table": "raw_detention_stays"},
    {"url": "https://github.com/deportationdata/ice/raw/refs/heads/main/data/facilities-daily-population-latest.xlsx", "table": "raw_facility_population"},
    {"url": "https://github.com/deportationdata/ice/raw/refs/heads/main/data/ice-offices.xlsx", "table": "dim_ice_offices"}
]

#---------------------------------------------------
# NAMESPACES
#---------------------------------------------------

NAMESPACE_ICE_OFFICES = uuid.UUID("12345678-1234-5678-1234-567812345678")
NAMESPACE_RESEARCH_HOLDROOMS = uuid.UUID("12345678-1234-5678-1234-567812345678")
NAMESPACE_HOLD_ROOMS = uuid.UUID("87654321-4321-6789-4321-678943216789")


# --------------------------------------------------
# LOGGING UTILITIES
# --------------------------------------------------

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def banner(msg):
    print("\n" + "="*65)
    print(msg)
    print("="*65)

#---------------------------------------------------
# HELPER FUNCTIONS
#---------------------------------------------------
def holdroom_uuid(code):
    return str(uuid.uuid5(NAMESPACE_HOLD_ROOMS, str(code).upper()))

# --------------------------------------------------
# STAGE 0 — LOAD NCIC DIMENSION TABLE
# --------------------------------------------------

def load_ncic_dimension(conn):
    banner("STAGE 0 — LOADING NCIC OFFENSE CODE DIMENSION")
    if not ncic_excel.exists():
        log(f"ERROR: NCIC offense code file not found: {ncic_excel}")
        return
    log(f"Reading offense code list from {ncic_excel.name}")
    df = pd.read_excel(ncic_excel)
    log(f"Offense codes loaded: {len(df)} rows")
    df.to_sql("dim_ncic_offense_codes", conn, if_exists="replace", index=False)

    log("dim_ncic_offense_codes table refreshed")
    if "Code" in df.columns:
        log(f"Unique offense codes available: {df['Code'].nunique()}")


# --------------------------------------------------
# STAGE 1 — INGEST PUBLIC DATASETS
# --------------------------------------------------

def ingest_datasets(conn):

    banner("STAGE 1 — DOWNLOADING CURRENT PROCESSED DATA FROM DEPORTATION DATA PROJECT DATASETS")

    for dataset in datasets:

        url = dataset["url"]
        table_name = dataset["table"]

        try:
            log(f"Fetching {table_name} dataset...")

            r = requests.get(url)
            r.raise_for_status()

            log(f"Download successful ({len(r.content):,} bytes)")
            log("Loading dataframe into memory...")
            df = pd.read_excel(BytesIO(r.content))
            if table_name == "dim_ice_offices":
                def make_uuid(row):
                    key = f"{row.get('office_name','')}_{row.get('city','')}_{row.get('state','')}"
                    return str(uuid.uuid5(NAMESPACE_ICE_OFFICES, key))
                df.insert(0, "office_id", df.apply(make_uuid, axis=1))
                log("Deterministic UUIDs generated for dim_ice_offices")
            log(f"{table_name}: {len(df):,} rows loaded")
            log("Writing data to coldCounter...")
            df.to_sql(table_name, conn, if_exists="replace", index=False)

            log(f"Table '{table_name}' updated")

        except Exception as e:
            log(f"ERROR loading {table_name}: {e}")

    log("Raw ingestion stage completed")

#--------------------------------------------------
# STAGE 1B - AUGMENTING HOLD ROOM LOCATIONS WITH ADDITIONAL SOURCES
#--------------------------------------------------

def load_holdroom_research(conn):

    banner("STAGE 1B — AUGMENTING HOLD ROOM LOCATIONS WITH ADDITIONAL SOURCES")

    if not holdroom_research_csv.exists():
        log(f"ERROR: file not found: {holdroom_research_csv}")
        return

    try:
        log(f"Reading {holdroom_research_csv.name}")

        df = pd.read_csv(holdroom_research_csv)

        if "holdroom_detention_facility_code" not in df.columns:
            log("ERROR: holdroom_detention_facility_code column missing")
            return

        # deterministic namespace
        NAMESPACE_RESEARCH_HOLDROOMS = uuid.UUID("12345678-1234-5678-1234-567812345678")

        # generate UUID from facility code
        df["research_id"] = df["holdroom_detention_facility_code"].apply(
            lambda x: str(uuid.uuid5(NAMESPACE_RESEARCH_HOLDROOMS, str(x)))
        )

        df["holdroom_id"] = df["holdroom_detention_facility_code"].apply(holdroom_uuid)

        log("Deterministic UUIDs generated from holdroom_detention_facility_code")

        log(f"{len(df):,} rows loaded")

        df.to_sql(
            "dim_noccc_holdroom_research",
            conn,
            if_exists="replace",
            index=False
        )


        log("dim_noccc_holdroom_research table refreshed")

    except Exception as e:
        log(f"ERROR loading hold room research: {e}")

#----------------------------------------------------
# STAGE 1C - LOAD HOLD ROOM OFFICE MAPPING
#----------------------------------------------------

def load_holdroom_office_mapping(conn):

    banner("STAGE 1C — LOADING HOLD ROOM OFFICE MAPPING")

    if not holdroom_office_mapping_csv.exists():
        log(f"ERROR: file not found: {holdroom_office_mapping_csv}")
        return

    try:
        log(f"Reading {holdroom_office_mapping_csv.name}")

        df = pd.read_csv(holdroom_office_mapping_csv)

        if "holdroom_detention_facility_code" not in df.columns:
            log("ERROR: holdroom_detention_facility_code column missing")
            return

        df["holdroom_id"] = df["holdroom_detention_facility_code"].apply(
            holdroom_uuid
        )

        log(f"{len(df):,} mappings loaded")

        df.to_sql(
            "bridge_holdroom_office",
            conn,
            if_exists="replace",
            index=False
        )

        log("bridge_holdroom_office table refreshed")

    except Exception as e:
        log(f"ERROR loading holdroom office mapping: {e}")

#---------------------------------------------------
# STAGE 1D - BUILD HOLD ROOM DIMENSION
#---------------------------------------------------
def build_holdroom_dimension(conn):

    banner("STAGE 1C — BUILD HOLD ROOM DIMENSION")

    df = pd.read_sql_query("""
        SELECT DISTINCT
            detention_facility_code,
            detention_facility,
            state
        FROM raw_detention_stints
        WHERE detention_facility_code LIKE '%HOLD'
    """, conn)

    log(f"{len(df)} hold rooms discovered")

    df["holdroom_id"] = df["detention_facility_code"].apply(holdroom_uuid)

    df.rename(columns={"state":"state_code"}, inplace=True)

    df.to_sql(
        "dim_hold_rooms_base",
        conn,
        if_exists="replace",
        index=False
    )

    conn.execute("""

    CREATE TABLE dim_hold_rooms AS

    SELECT DISTINCT
        hr.*,
        IFNULL(o.office_name, r.research_name) as holdroom_name,
        ifnull(o.address, r.address) as holdroom_address,
        IFNULL(o.city, r.address_city) as holdroom_city,
        IFNULL(o.state, r.address_state) as holdroom_state,
        IFNULL(o.zip, r.address_zip) as holdroom_zip,
        o.office_id,
        r.research_id

    FROM dim_hold_rooms_base hr

    LEFT JOIN bridge_holdroom_office br
        ON hr.holdroom_id = br.holdroom_id

    LEFT JOIN dim_noccc_holdroom_research r
        ON hr.holdroom_id = r.holdroom_id

    LEFT JOIN dim_ice_offices o
        ON br.office_id = o.office_id

    """)

    log("dim_hold_rooms built and enriched")

# --------------------------------------------------
# STAGE 2 — BUILD HOLD ROOM FACT TABLE
# --------------------------------------------------

def build_hold_room_facts(conn):

    banner("STAGE 2 — BUILD HOLD ROOM FACT TABLE")

    query = """
    SELECT
        st.*,
        cd.*,

        hr.holdroom_id,
        hr.holdroom_name,
        hr.holdroom_address,
        hr.holdroom_city,
        hr.holdroom_state,
        hr.holdroom_zip,
        hr.office_id,
        hr.research_id,
        hr.detention_facility_code as holdroom_detention_facility_code,
        hr.detention_facility as holdroom_detention_facility_name

    FROM raw_detention_stints st

    LEFT JOIN dim_ncic_offense_codes cd
        ON st.most_serious_conviction_code = cd.Code

    JOIN dim_hold_rooms hr
        ON st.detention_facility_code = hr.detention_facility_code
    """

    log("Collecting evidence for the Hague...")
    df = pd.read_sql_query(
        query,
        conn,
        parse_dates=["book_in_date_time", "book_out_date_time"]
    )

    log(f"Analyzing: {len(df):,} encounters for Hold Room use...")

    # Ensure facility codes behave properly for string ops
    df["detention_facility_code"] = df["detention_facility_code"].astype(str)

    filtered = df[
        (df.likely_duplicate == 0) &
        (df.book_out_date_time.notna())
    ].copy()

    log(f"Hold Rooms used: {len(filtered):,} times in dataset.")
    log("Identifying ICE National Detention Standards violations...")
    nds_art()

    # ----------------------------
    # Derived fields
    # ----------------------------

    filtered["age"] = filtered["book_out_date_time"].dt.year - filtered.birth_year

    filtered["hours_imprisoned"] = (
        filtered.book_out_date_time - filtered.book_in_date_time
    ).dt.total_seconds() / 3600.0

    filtered["nights_without_bed"] = (filtered["hours_imprisoned"] >= 12).astype(int)

    filtered["child_prisoner"] = filtered["age"] < 18
    filtered["is_elderly"] = filtered["age"] >= 70

    filtered["criminality_code"] = filtered["book_in_criminality"].astype(str).str[0]

    filtered["violent_offender"] = (
        filtered['Type of Offense Code (V=violent, D=drug-related, Blank = nonviolent or not drug related)'] == 'V'
    )

    filtered["criminal"] = filtered["criminality_code"] == "1"
    filtered["non_criminal"] = filtered["criminality_code"].isin(["3", "2"])
    filtered["charges_filed"] = filtered["criminality_code"] == "2"
    filtered["detained_without_charge"] = filtered["criminality_code"] == "3"

    filtered["violation_hold_over_12_hours"] = (
        (filtered["hours_imprisoned"] > 12) &
        (filtered["book_in_date_time"] < pd.Timestamp("2025-06-22"))
    )

    filtered["violation_held_over_72_hours"] = (
        filtered["hours_imprisoned"] > 72
    )

    filtered["violation_nv_elderly_over_70"] = (
        (filtered["age"] >= 70) &
        (~filtered["violent_offender"])
    )

    filtered = filtered.fillna(False)

    # ----------------------------
    # Aggregation
    # ----------------------------

    grouped = (
        filtered
        .groupby(["holdroom_id"])
        .agg(

            total_encounters=("stint_ID", "count"),
            total_unique_individuals=("unique_identifier", "count"),

            violations_hold_over_12_hours=("violation_hold_over_12_hours", "sum"),
            violations_hold_over_72_hours=("violation_held_over_72_hours", "sum"),
            violations_hold_nv_elderly_over_70=("violation_nv_elderly_over_70", "sum"),

            nights_without_bed=("nights_without_bed", "sum"),
            children=("child_prisoner", "sum"),
            elderly=("is_elderly", "sum"),

            min_hours=("hours_imprisoned", "min"),
            max_hours=("hours_imprisoned", "max"),
            average_hours=("hours_imprisoned", "mean"),

            min_age=("age", "min"),
            max_age=("age", "max"),
            avg_age=("age","mean"),

            pct_no_charge=("detained_without_charge", "mean"),
            pct_criminal=("criminal", "mean"),
            pct_non_criminal=("non_criminal", "mean"),
            pct_violent_offender=("violent_offender", "mean"),

            min_book_in_date_in_dataset=("book_in_date_time", "min"),
            max_book_out_date_in_dataset=("book_out_date_time", "max"),

            count_convicted_criminals=("criminal", "sum"),
            count_charged=("charges_filed", "sum"),
            count_no_charges=("detained_without_charge", "sum"),

            holdroom_name=("holdroom_name","first"),

            holdroom_address=("holdroom_address","first"),
            holdroom_city=("holdroom_city","first"),
            holdroom_state=("holdroom_state","first"),
            holdroom_zip=("holdroom_zip","first"),

            holdroom_detention_facility_code=("holdroom_detention_facility_code","first"),
            holdroom_detention_facility_name=("holdroom_detention_facility_name","first"),

            office_id=("office_id","first"),
            research_id=("research_id","first")
        )
        .round(2)
        .reset_index()
        .sort_values(["holdroom_state", "holdroom_id"])
    )


    # --------------------------------------------------
    # Column ordering
    # --------------------------------------------------

    stat_fields = [
        "total_encounters",
        "total_unique_individuals",
        "violations_hold_over_12_hours",
        "violations_hold_over_72_hours",
        "violations_hold_nv_elderly_over_70",
        "nights_without_bed",
        "children",
        "elderly",
        "min_hours",
        "max_hours",
        "average_hours",
        "min_age",
        "max_age",
        "avg_age",
        "pct_no_charge",
        "pct_criminal",
        "pct_non_criminal",
        "pct_violent_offender",
        "min_book_in_date_in_dataset",
        "max_book_out_date_in_dataset",
        "count_convicted_criminals",
        "count_charged",
        "count_no_charges"
    ]

    address_fields = [
        "holdroom_address",
        "holdroom_city",
        "holdroom_state",
        "holdroom_zip"
    ]

    id_fields = [
        "holdroom_id",
        "office_id",
        "research_id"
    ]

    final_order = (
        ["holdroom_detention_facility_code", "holdroom_name","holdroom_detention_facility_name"]
        + stat_fields
        + address_fields
        + id_fields
    )

    grouped = grouped[final_order]

    # --------------------------------------------------
    # Write final table
    # --------------------------------------------------

    log("Writing analysis to fact_hold_rooms")

    grouped.to_sql(
        "fact_hold_rooms",
        conn,
        if_exists="replace",
        index=False
    )

    log(f"fact_hold_rooms written ({len(grouped)} rows)")

    conn.execute("DROP TABLE IF EXISTS dim_hold_rooms_base")
    conn.execute("DROP TABLE IF EXISTS bridge_holdroom_office")

# --------------------------------------------------
# STAGE 3 — BUILD FACILITY STATISTICS FACT TABLE
# --------------------------------------------------

def build_facility_statistics(conn):

    banner("STAGE 3 — BUILD FACILITY STATISTICS FACT TABLE")
    log("Loading detention data to memory...")
    df = pd.read_sql_query(
        """
        SELECT * FROM raw_detention_stints st
        LEFT JOIN dim_ncic_offense_codes cd
        ON st.most_serious_conviction_code = cd.Code
        """,
        conn,
        parse_dates=["book_in_date_time","book_out_date_time"]
    )
    log("Filtering duplicates...")
    filtered = df[
        (df.likely_duplicate == 0) &
        (df.book_out_date_time.notna())
    ].copy()
    log("Computing derived fields...")
     # Compute derived fields
    filtered["age"] = filtered["book_out_date_time"].dt.year - filtered.birth_year
    filtered["days_imprisoned"] = (
        filtered.book_out_date_time - filtered.book_in_date_time
    ).dt.total_seconds() / 86400.0
    filtered["temp_hold_over_72"] = filtered["days_imprisoned"] >= 3 
     # add extra demographic/criminal flags
    filtered["child_prisoner"] = filtered["age"] < 18
    filtered["is_elderly"] = filtered["age"] >= 70
     # Extract criminality codes from first character of book_in_criminality
    filtered["criminality_code"] = filtered["book_in_criminality"].astype(str).str[0]
    filtered["criminal"] = filtered["criminality_code"] == "1"  # criminal
    filtered["non_criminal"] = filtered["criminality_code"].isin(["3", "2"]) #no criminal conviction
    filtered["charged"] = filtered["criminality_code"] == "2"  # pending charges
    filtered["no_charges"] = filtered["criminality_code"] == "3"  # no criminal charge
    filtered["violent_offender"] = filtered['Type of Offense Code (V=violent, D=drug-related, Blank = nonviolent or not drug related)'] == 'V'

     # Categorize facility type
    def categorize_facility(row):
        code = row['detention_facility_code']
        name = row['detention_facility']
        if code.endswith('HOLD'):
            return 'hold room'
        elif 'JAIL' in name or 'COR' in name:
            return 'jail'
        elif 'HOS' in name:
            return 'hospital'
        else:
            return 'other'
    filtered['facility_category'] = filtered.apply(categorize_facility, axis=1)
    filtered = filtered.fillna(False)
    log("Aggregating data by facility...")
     # Group by facility and compute aggregate statistics
    grouped = (
        filtered
        .groupby(["state","detention_facility", "detention_facility_code"])
        .agg(
            facility_category=('facility_category', 'first'),
            total_stays_recorded_at_facility=("stay_ID", "count"),
            min_book_in_date_in_dataset=("book_in_date_time", "min"),
            max_book_out_date_in_dataset=("book_out_date_time", "max"),
            min_days=("days_imprisoned", "min"),
            max_days=("days_imprisoned", "max"),
            average_days=("days_imprisoned", "mean"),
            min_age=("age", "min"),
            max_age=("age", "max"),
            children=("child_prisoner", "sum"),
            elderly=("is_elderly", "sum"),
            pct_non_criminal=("non_criminal", "mean"),
            pct_violent_offender=("violent_offender", "mean"),
            avg_bond_posted=("bond_posted_amount", "mean"),
            count_criminal=("criminal", "sum"),
            count_charges_pending=("charged", "sum"),
            count_no_charges=("no_charges", "sum")
     )
        .round(2)
        .reset_index()
        .sort_values("detention_facility_code")
    )

    grouped.rename(columns={"state":"state_code"}, inplace=True)
    log("Writing analysis to fact_facility_statistics")
    grouped.to_sql("fact_facility_statistics", conn, if_exists="replace", index=False)
    log(f"fact_facility_statistics written ({len(grouped)} rows)")



# --------------------------------------------------
# SANITY CHECKS
# --------------------------------------------------

def sanity_checks(conn):

    banner("PIPELINE SANITY CHECKS")

    tables = [
        "raw_arrests",
        "raw_detainers",
        "raw_detention_stays",
        "raw_detention_stints",
        "fact_hold_rooms",
        "fact_facility_statistics",
        "dim_ncic_offense_codes",
        "dim_ice_offices"
    ]

    for t in tables:

        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            log(f"{t}: {count:,} rows")

        except:
            log(f"{t}: table missing")

    log("Integrity checks complete")

#---------------------------------------------------
# FONT FUN
#---------------------------------------------------

init(autoreset=True)

def redhulk(text):
        print(Fore.RED + text + Style.RESET_ALL)
def big_title(text):
        f = Figlet(font="slant")
        print(Fore.CYAN + f.renderText(text) + Style.RESET_ALL)
def stage_title(text):
        f = Figlet(font="alligator2")
        print(Fore.CYAN + f.renderText(text) + Style.RESET_ALL)
        
def title_art():
    for _ in range(4):
        redhulk("EL PUEBLO UNIDO JAMAS SERA VENCIDO")
    big_title("NO")
    big_title("CONCENTRATION CAMPS")
    big_title("IN COLORADO")
    for _ in range(4):
        redhulk("EL PUEBLO UNIDO JAMAS SERA VENCIDO")
        
def slow_print(text, delay=0.01):
    for c in text:
        sys.stdout.write(c)
        sys.stdout.flush()
        time.sleep(delay)
    print()


def divider():
    print(Fore.WHITE + "═" * 80)


def header(text):
    f = Figlet(font="ansi_shadow")
    print(Fore.RED + f.renderText(text))


def box(title, lines):
    width = 78

    print(Fore.WHITE + "╔" + "═" * width + "╗")
    print("║ " + Fore.YELLOW + title.center(width - 2) + Style.RESET_ALL + " ║")
    print("╠" + "═" * width + "╣")

    for line in lines:
        slow_print("║ " + line.ljust(width - 2) + " ║", 0.002)

    print("╚" + "═" * width + "╝")
    print()


def nds_intro():
    divider()
    header("NATIONAL DETENTION")
    header("STANDARDS 2025")
    divider()

    slow_print(
        Fore.CYAN
        + "STANDARD 2.5 — HOLD ROOMS IN DETENTION FACILITIES".center(80),
        0.01,
    )

    divider()
    print()
    time.sleep(0.5)

def nds_art():    

    nds_intro()

    box(
        "HOLD ROOM STRUCTURE",
        [
            "Temporary holding spaces used during intake, processing, or transfer.",
            "These rooms are not intended to function as sleeping quarters.",
            "",
            "Beds or sleeping bunks are prohibited inside hold rooms.",
        ],
    )
    
    box(
        "MAXIMUM LENGTH OF STAY",
        [
            "The expected duration of confinement is short-term.",
            "",
            "The general maximum length of stay in a hold room",
            "should not exceed twelve hours.",
            "",
            "Individuals should be transferred to appropriate housing",
            "or processing areas before this limit is reached.",
        ],
    )
    
    box(
        "SPECIAL POPULATION CONSIDERATIONS",
        [
            "Unnacompanied children.",
            "Elderly (>70)",
            "Specific care for families",
            "",
            "Individuals in these categories who do not present",
            "a history of violence are prohibited from hold room detention.",
        ],
    )
    
    divider()
    
    slow_print(
        Fore.RED
        + "ICE NATIONAL DETENTION STANDARDS — SECTION 2: SECURITY".center(80),
        0.01,
    )
    
    divider()

def green_banner(text, repeats=4, delay=0.5):
    f = Figlet(font="big_money-ne")
    for _ in range(repeats):
        print(Fore.GREEN + f.renderText(text) + Style.RESET_ALL)
        time.sleep(delay)  

def moneyyyyy():
    green_banner("CASHING SOROS CHECK") #this is a joke, nerd


# --------------------------------------------------
# MAIN PIPELINE
# --------------------------------------------------

def run_pipeline():
    title_art()
    log("COLDCOUNTER ETL PIPELINE INITIATED")
    conn = sqlite3.connect(db_path)
    stage_title("S 0")
    load_ncic_dimension(conn)
    stage_title("S 1")
    ingest_datasets(conn)
    stage_title("S 1-B")
    load_holdroom_research(conn)
    stage_title("S 1-C")
    load_holdroom_office_mapping(conn)
    stage_title("S 1-D")
    build_holdroom_dimension(conn)
    stage_title("S 2")    
    build_hold_room_facts(conn)
    stage_title("S 3")
    build_facility_statistics(conn)
    stage_title("S 4")
    sanity_checks(conn)
    conn.close()
    banner("DATABASE BUILD COMPLETE")
    log("coldCounter database successfully updated.")
if __name__ == "__main__":
    run_pipeline()