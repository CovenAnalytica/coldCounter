# -*- coding: utf-8 -*-
"""
build_coldCounter.py

Unified ETL pipeline for the coldCounter database.

Pipeline Stages
----------------
0. Load reference tables from local files (e.g. NCIC offense codes)
1. Ingest public datasets from Deportation Data Project GitHub repositories
    1B. Apply deterministic UUIDs to scraped ice office locations
    1C. Load raw tables from public dataset extract
2. Build fact tables
3. Build dimension tables

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
holdroom_research_csv = workspace_root / "data" / "NOCCC_holdroom_research.csv"
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
    {"url": "https://github.com/deportationdata/ice-offices/raw/refs/heads/main/data/ice-offices.xlsx", "table": "dim_ice_offices"}
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

def office_uuid(name, city, state):
    key = f"{name}_{city}_{state}"
    return str(uuid.uuid5(NAMESPACE_ICE_OFFICES, key))

# --------------------------------------------------
# STAGE 0 — LOAD REFERENCE TABLES
# --------------------------------------------------

def load_ncic_codes(conn):
    banner("STAGE 0 - 1 : LOADING NCIC OFFENSE CODE TABLE")
    if not ncic_excel.exists():
        log(f"ERROR: NCIC offense code file not found: {ncic_excel}")
        return
    log(f"Reading offense code list from {ncic_excel.name}")
    df = pd.read_excel(ncic_excel)
    log(f"Offense codes loaded: {len(df)} rows")
    df.to_sql("ref_ncic_offense_codes", conn, if_exists="replace", index=False)

    log("ref_ncic_offense_codes table refreshed")
    if "Code" in df.columns:
        log(f"Total offense codes loaded: {df['Code'].nunique()}")

def load_noccc_holdroom_research(conn):
    banner("STAGE 0 - 2 : LOADING HOLD ROOM RESEARCH TABLE")
    if not holdroom_research_csv.exists():
        log(f"ERROR: file not found: {holdroom_research_csv}")
        return
    log(f"Reading hold room research from {holdroom_research_csv.name}")
    df = pd.read_csv(holdroom_research_csv)
    def make_uuid(row):
        key = f"{row.get('holdroom_detention_facility_code','')}"
        return str(uuid.uuid5(NAMESPACE_RESEARCH_HOLDROOMS, key))
    df.insert(0, "research_id", df.apply(make_uuid, axis=1))
    log(f"Hold room research loaded: {len(df)} rows")
    df.to_sql("ref_noccc_holdroom_research", conn, if_exists="replace", index=False)
    log("ref_noccc_holdroom_research table refreshed")

def load_holdroom_office_mapping(conn):
    banner("STAGE 0 - 3 : LOADING HOLD ROOM X OFFICE MAPPING TABLE")
    if not holdroom_office_mapping_csv.exists():
        log(f"ERROR: file not found: {holdroom_office_mapping_csv}")
        return
    log(f"Reading hold room office mapping from {holdroom_office_mapping_csv.name}")
    df = pd.read_csv(holdroom_office_mapping_csv)
    log(f"Hold room office mapping loaded: {len(df)} rows")
    df.to_sql("ref_holdroom_office_xwalk", conn, if_exists="replace", index=False)
    log("ref_holdroom_office_xwalk table refreshed")


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
# STAGE 2 - BUILD FACT TABLES
#--------------------------------------------------
def build_stint_fact_table(conn):
    banner("STAGE 2 - 1 : BUILDING STINT FACT TABLE")

    query = """
    select
        st.stint_id,
        st.stay_ID,
        st.unique_identifier,
        st.book_in_date_time,
        st.book_out_date_time,
        st.detention_facility_code,
        st.state,
        st.bond_posted_date,
        st.bond_posted_amount,
        st.detention_release_reason
    from
        raw_detention_stints st
    where not st.likely_duplicate
    """

    log("Collecting evidence for the Hague...")
    df = pd.read_sql_query(
        query,
        conn,
        parse_dates=["book_in_date_time", "book_out_date_time"]
    )

    log(f"Loading: {len(df):,} stints into fact_stints...")
    df.to_sql(
        "fact_stints",
        conn,
        if_exists="replace",
        index=False
    )

#--------------------------------------------------
# STAGE 3 - BUILD DIMENSION TABLES
#--------------------------------------------------
def build_detention_facility_dimension(conn):
    banner("STAGE 3 - 1 BUILDING DETENTION FACILITY DIMENSION")

    query_base = """
        select
            st.detention_facility_code,
            st.detention_facility detention_facility_name,
            st.state detention_facility_state,
            avg(case when st.book_in_criminality like '1%' then 1 else 0 end) pct_criminal,
            avg(case when st.book_in_criminality like '2%' then 1 else 0 end) pct_charged_guilt_not_established,
            avg(case when st.book_in_criminality like '3%' then 1 else 0 end) pct_no_criminal_charges,
            count(distinct st.unique_identifier) total_unique_people,
            count(distinct st.unique_identifier) filter (where CAST(strftime('%Y',st.book_in_date_time) as integer) - st.birth_year < 18) children,
            count(distinct st.unique_identifier) filter (where CAST(strftime('%Y',st.book_in_date_time) as integer) - st.birth_year > 70) elderly_over_70,
            avg(julianday(st.book_out_date_time) - julianday(st.book_in_date_time)) average_days,
            max(julianday(st.book_out_date_time) - julianday(st.book_in_date_time)) max_days,
            min(st.book_in_date_time) earliest_book_in_time_in_data,
            max(st.book_out_date_time) latest_book_out_time_in_data,
            avg(case when st.final_order_yes_no = 'YES' then 1 else 0 end) pct_final_order_given
        from
            raw_detention_stints st
        group by 
            st.detention_facility_code,
            st.detention_facility,
            st.state
        order by
            st.state,
            st.detention_facility_code
        """

    log("Loading detention facility base information...")
    df_base = pd.read_sql_query(
        query_base,
        conn
    )

    query_daily_pop = """
        select
            fp.detention_facility_code,
            avg(fp.n_detained) average_daily_population,
            max(fp.n_detained) max_daily_population
        from
            raw_facility_population fp
        group by fp.detention_facility_code """
    
    log("Loading detention facility daily population information...")
    df_pop = pd.read_sql_query(
        query_daily_pop,
        conn
    )
    log("Merging base detention facility information with population data...")
    df = df_base.merge(df_pop, on="detention_facility_code", how="left")

    log(f"{len(df):,} unique detention facility codes encountered")
    log("Note: some facilities may be local jails or healthcare facilities used for detainee tracking purposes, not official ICE detention centers.")
    log("Inserting dimensional data into dim_detention_facilities...")
    df.to_sql(
        "dim_detention_facilities",
        conn,
        if_exists="replace",
        index=False
    )

def build_hold_room_dimension(conn):
    banner("STAGE 3 - 2 BUILDING HOLD ROOM DIMENSION")

    query = """
    select 
        st.detention_facility_code,
        st.detention_facility detention_facility_name,
        st.state detention_facility_state,
        count(distinct st.stint_id) total_times_used,
        count(distinct st.stint_id) filter (where st.book_in_date_time < '2025-01-01') total_times_used_before_2025,
        count(distinct st.stint_id) filter (where st.book_in_date_time >= '2025-01-01') total_times_used_since_2025,
        count(distinct st.unique_identifier) total_unique_people,
        count(distinct st.unique_identifier) filter (where CAST(strftime('%Y',st.book_in_date_time) as integer) - st.birth_year < 18) children,
        count(distinct st.unique_identifier) filter (where CAST(strftime('%Y',st.book_in_date_time) as integer) - st.birth_year > 70) elderly_over_70,
        count(distinct st.stint_id) filter (where julianday(st.book_out_date_time) - julianday(st.book_in_date_time) >= 0.5 and st.book_out_date_time < '2025-06-24') violations_over_12_hours_before_waiver,
        count(distinct st.stint_id) filter (where julianday(st.book_out_date_time) - julianday(st.book_in_date_time) >= 3) violations_over_72_hours,
        count(distinct st.stint_id) filter (where julianday(st.book_out_date_time) - julianday(st.book_in_date_time) >= 3 and st.book_out_date_time > '2025-06-24') violations_over_72_hours_post_waiver,
        avg((julianday(st.book_out_date_time) - julianday(st.book_in_date_time)) * 24) average_hours_imprisoned,
        count(distinct st.stint_id) filter (where julianday(st.book_out_date_time) - julianday(st.book_in_date_time) >= 7) imprisoned_over_7_days,
        count(distinct st.stint_id) filter (where julianday(st.book_out_date_time) - julianday(st.book_in_date_time) >= 30) imprisoned_over_30_days,
        coalesce(o.address, r.address) holdroom_address,
        coalesce(o.city, r.address_city) holdroom_city,
        coalesce(o.state, r.address_state) holdroom_state,
        coalesce(o.zip, r.address_zip) holdroom_zip,
        case
            when o.office_id is null and r.research_id is not null then 'NOCCC Research'
            when o.office_id is not null then 'DPP ICE office web scrape'
            when o.office_id is null and r.research_id is null then 'Research Incomplete'
        end as source_of_holdroom_location_information,
        MAX(o.office_id) as office_id,
        MAX(r.research_id) as research_id
    from 
        raw_detention_stints st
        left join ref_noccc_holdroom_research r
            on st.detention_facility_code = r.holdroom_detention_facility_code
        left join ref_holdroom_office_xwalk x
            on st.detention_facility_code = x.holdroom_detention_facility_code
        left join dim_ice_offices o
            on x.office_id = o.office_id
    where st.detention_facility_code like '%HOLD'
    group by
    st.detention_facility_code,
    st.detention_facility,
    st.state
    order by
    st.state,
    st.detention_facility_code
    """
    log("Collecting evidence for the Hague...")
    df = pd.read_sql_query(
        query,
        conn
    )
    log(f"Loading: {len(df):,} hold rooms into dim_hold_rooms...")
    df.to_sql(
        "dim_hold_rooms",
        conn,
        if_exists="replace",
        index=False
    )

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
        "raw_facility_population",
        "ref_noccc_holdroom_research",
        "ref_holdroom_office_xwalk",
        "ref_ncic_offense_codes",
        "fact_stints",
        "dim_detention_facilities",
        "dim_ice_offices",
        "dim_hold_rooms"
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
    if os.path.exists(db_path):
        os.remove(db_path)
    conn = sqlite3.connect(db_path)
    stage_title("S 0 REF TABLES")
    load_ncic_codes(conn)
    load_noccc_holdroom_research(conn)
    load_holdroom_office_mapping(conn)
    stage_title("S 1 RAW TABLES")
    ingest_datasets(conn)
    stage_title("S 2 FACT TABLES")
    build_stint_fact_table(conn)
    stage_title("S 3 DIM TABLES")
    build_detention_facility_dimension(conn)
    build_hold_room_dimension(conn)
    sanity_checks(conn)
    conn.close()
    banner("DATABASE BUILD COMPLETE")
    log("coldCounter database successfully updated.")
if __name__ == "__main__":
    run_pipeline()