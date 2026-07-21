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
import tempfile
import uuid
import pandas as pd
import requests
from io import BytesIO
from datetime import datetime
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse
import os
import shutil
import subprocess
from pyfiglet import Figlet
from colorama import Fore, Style, init
import sys
import time
import requests
from bs4 import BeautifulSoup
import re
from geopy.geocoders import ArcGIS, Nominatim
from geopy.extra.rate_limiter import RateLimiter

# --------------------------------------------------
# PATH CONFIGURATION
# --------------------------------------------------

current_dir = Path(__file__).resolve().parent
workspace_root = current_dir.parent.resolve(strict=False)
default_db_path = (workspace_root / "coldCounter.db").resolve(strict=False)
db_path = Path(os.environ.get("COLDCOUNTER_DB_PATH", str(default_db_path))).expanduser().resolve(strict=False)

# --------------------------------------------------
# DDP DATASETS
# --------------------------------------------------

ddp_datasets = [
    {"url": "https://github.com/deportationdata/ice-detention-facilities/raw/refs/heads/main/data/facilities-latest.parquet", "table": "ddp_detention_facilities"},
    {"url": "https://github.com/deportationdata/ice/raw/refs/heads/main/data/arrests-latest.parquet", "table": "ddp_arrests"},
    {"url": "https://github.com/deportationdata/ice/raw/refs/heads/main/data/detainers-latest.parquet", "table": "ddp_detainers"},
    {"url": "https://github.com/deportationdata/ice/raw/refs/heads/main/data/detention-stints-latest.parquet", "table": "ddp_detention_stints"},
    {"url": "https://github.com/deportationdata/ice/raw/refs/heads/main/data/detention-stays-latest.parquet", "table": "ddp_detention_stays"},
    {"url": "https://github.com/deportationdata/ice/raw/refs/heads/main/data/encounters-latest.parquet", "table": "ddp_encounters"},
    {"url": "https://raw.githubusercontent.com/deportationdata/ice/main/data/joined-arrests-detention-stays-latest.parquet", "table": "ddp_joined_arrests_detention_stays"},
    {"url": "https://raw.githubusercontent.com/deportationdata/eoir/main/data/cases.parquet", "table": "ddp_eoir_cases"},
    {"url": "https://raw.githubusercontent.com/deportationdata/ice-detention-facilities/main/data/facilities-attributes-cleaned-with-codes.parquet", "table": "ddp_facilities_attributes"},
    {"url": "https://raw.githubusercontent.com/deportationdata/ice-detention-facilities/main/data/facilities-detention-management.parquet", "table": "ddp_facilities_detention_management"},
    {"url": "https://raw.githubusercontent.com/deportationdata/ice-detention-facilities/main/data/facilities-latest.parquet", "table": "ddp_facilities"},
    {"url": "https://raw.githubusercontent.com/deportationdata/ice-detention-facilities/main/data/noccc-hold-rooms.parquet", "table": "ddp_noccc_hold_rooms"},
    {"url": "https://github.com/deportationdata/ice-offices/raw/refs/heads/main/data/ice-offices.parquet", "table": "ddp_ice_offices"},
    {"url": "https://raw.githubusercontent.com/deportationdata/ice-detention-facilities/main/data/facilities-geocoded-exact.parquet", "table": "ddp_facilities_geocoded"}
]

# --------------------------------------------------
# LOGGING UTILITIES
# --------------------------------------------------

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def banner(msg):
    print("\n" + "="*65)
    print(msg)
    print("="*65)


def normalize_github_url(url):
    parsed = urlparse(url)
    if parsed.netloc in {"github.com", "www.github.com"}:
        parts = parsed.path.lstrip("/").split("/")
        if len(parts) >= 5 and parts[2] == "blob":
            owner, repo, _, ref, *rest = parts
            # Handle URLs like /blob/main/... and /blob/refs/heads/main/...
            if ref == "refs" and len(rest) >= 2 and rest[0] == "heads":
                ref = rest[1]
                rest = rest[2:]
            return f"https://raw.githubusercontent.com/{owner}/{repo}/{ref}/{'/'.join(rest)}"
        if len(parts) >= 6 and parts[2] == "raw" and parts[3] == "refs" and parts[4] == "heads":
            owner, repo = parts[0], parts[1]
            ref = parts[5]
            rest = parts[6:]
            return f"https://raw.githubusercontent.com/{owner}/{repo}/{ref}/{'/'.join(rest)}"
    return url


git_lfs_repo_cache = {}

def is_lfs_pointer(content):
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        return False, None

    if text.startswith("version https://git-lfs.github.com/spec/v1"):
        oid = None
        size = None
        for line in text.splitlines():
            if line.startswith("oid sha256:"):
                oid = line.split(":", 1)[1].strip()
            elif line.startswith("size "):
                try:
                    size = int(line.split(" ", 1)[1].strip())
                except ValueError:
                    size = None
        return True, {"oid": oid, "size": size}

    return False, None


def parse_github_source_url(url):
    parsed = urlparse(url)
    if parsed.netloc == "raw.githubusercontent.com":
        parts = parsed.path.lstrip("/").split("/")
        if len(parts) >= 4:
            owner, repo, ref = parts[:3]
            path = "/".join(parts[3:])
            return owner, repo, ref, path
    if parsed.netloc in {"github.com", "www.github.com"}:
        parts = parsed.path.lstrip("/").split("/")
        if len(parts) >= 5 and parts[2] == "blob":
            owner, repo, _, ref, *rest = parts
            if ref == "refs" and len(rest) >= 2 and rest[0] == "heads":
                ref = rest[1]
                rest = rest[2:]
            return owner, repo, ref, "/".join(rest)
        if len(parts) >= 6 and parts[2] == "raw" and parts[3] == "refs" and parts[4] == "heads":
            owner, repo = parts[0], parts[1]
            ref = parts[5]
            path = "/".join(parts[6:])
            return owner, repo, ref, path
    return None


def download_file_from_git_lfs(owner, repo, ref, path):
    key = (owner, repo, ref)
    if key not in git_lfs_repo_cache:
        repo_dir = Path(tempfile.mkdtemp(prefix="coldcounter_git_lfs_"))
        clone_url = f"https://github.com/{owner}/{repo}.git"
        subprocess.run(
            ["git", "clone", "--depth", "1", "--branch", ref, clone_url, str(repo_dir)],
            check=True,
            capture_output=True,
            text=True,
        )
        git_lfs_repo_cache[key] = repo_dir
    else:
        repo_dir = git_lfs_repo_cache[key]

    subprocess.run(
        ["git", "lfs", "pull", "--include", path],
        cwd=str(repo_dir),
        check=True,
        capture_output=True,
        text=True,
    )

    file_path = repo_dir / path
    if not file_path.exists():
        raise FileNotFoundError(f"LFS file not found after clone: {file_path}")

    return file_path.read_bytes()


def cleanup_git_lfs_cache():
    for repo_dir in git_lfs_repo_cache.values():
        if repo_dir.exists():
            shutil.rmtree(repo_dir, ignore_errors=True)
    git_lfs_repo_cache.clear()
    log("Cleaned up Git LFS temporary repositories")


def initialize_database_file():
    global db_path

    try:
        db_path = db_path.resolve(strict=False)

        if db_path.exists() and db_path.is_dir():
            raise IsADirectoryError(f"Database path points to a directory: {db_path}")

        db_path.parent.mkdir(parents=True, exist_ok=True)

        if db_path.exists():
            db_path.unlink()
            log(f"Replaced existing database at {db_path}")

        conn = sqlite3.connect(str(db_path))
        conn.execute("SELECT 1")
        conn.close()
        log(f"Initialized empty database: {db_path}")
        return db_path
    except (sqlite3.OperationalError, OSError) as exc:
        fallback_path = Path(tempfile.gettempdir()) / "coldcounter.db"
        log(f"Unable to use {db_path}; falling back to {fallback_path}: {exc}")
        if fallback_path.exists():
            fallback_path.unlink()
        conn = sqlite3.connect(str(fallback_path))
        conn.execute("SELECT 1")
        conn.close()
        db_path = fallback_path
        log(f"Initialized empty database: {db_path}")
        return db_path




# --------------------------------------------------
# STAGE 1 — INGEST PUBLIC DATASETS
# --------------------------------------------------

def ingest_datasets(conn):

    banner("STAGE 1 — DOWNLOADING CURRENT PROCESSED DATA FROM DEPORTATION DATA PROJECT DATASETS")
    ingested_tables = []

    try:
        for dataset in ddp_datasets:

            url = dataset["url"]
            table_name = dataset["table"]
            file_type = PurePosixPath(urlparse(url).path).suffix.lower().lstrip(".")

            try:
                normalized_url = normalize_github_url(url)
                if normalized_url != url:
                    log(f"{table_name}: Normalized GitHub URL to raw content")
                log(f"{table_name}: Fetching dataset...")
                r = requests.get(normalized_url)
                r.raise_for_status()
                content = r.content
                lfs_pointer, lfs_info = is_lfs_pointer(content)
                if lfs_pointer and file_type == "parquet":
                    log(f"{table_name}: Detected Git LFS pointer for {normalized_url}")
                    parsed = parse_github_source_url(normalized_url)
                    if parsed is None:
                        raise ValueError("Unable to parse GitHub repository information for LFS download")
                    owner, repo, ref, path = parsed
                    content = download_file_from_git_lfs(owner, repo, ref, path)
                    log(f"{table_name}: Downloaded LFS file ({len(content):,} bytes)")
                log(f"{table_name}: Download successful ({len(content):,} bytes)")
                
                log(f"{table_name}: Loading {file_type} into dataframe...")
                if file_type == "parquet":
                    if not (content[:4] == b"PAR1" or content[-4:] == b"PAR1"):
                        snippet = content[:200].decode("utf-8", errors="replace")
                        raise ValueError(f"Downloaded content is not a Parquet file; first bytes: {snippet!r}")
                    df = pd.read_parquet(BytesIO(content))
                elif file_type == "feather":
                    df = pd.read_feather(BytesIO(content))
                elif file_type == "xlsx":
                    sheet_map = pd.read_excel(BytesIO(content), sheet_name=None)
                    frames = []
                    for sheet_name, frame in sheet_map.items():
                        frames.append(frame)
                    if not frames:
                        raise ValueError("Workbook contained no sheets with data")
                    df = pd.concat(frames, ignore_index=True)
                else:
                    raise ValueError(f"Unsupported file type: {file_type}")
                log(f"{table_name}: Loaded {len(df):,} rows")

                log(f"{table_name}: Writing to coldCounter...")
                df.to_sql(table_name, conn, if_exists="replace", index=False)
                ingested_tables.append(table_name)
                log(f"{table_name}: Table updated")

            except Exception as e:
                log(f"ERROR loading {table_name}: {e}")
    finally:
        cleanup_git_lfs_cache()

    log("Raw ingestion stage completed")
    return ingested_tables

def check_table_integrity(conn, tables):
    log("Performing final checks on tables...")

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
    nds_art()
        
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
    header("STANDARDS 2026")
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

def green_banner(text, repeats=5, delay=0.2):
    f = Figlet(font="big_money-ne")
    for _ in range(repeats):
        print(Fore.GREEN + f.renderText(text) + Style.RESET_ALL)
        time.sleep(delay)  

def moneyyyyy():
    green_banner("CASHING SOROS CHECK") #this is a joke, nerd


def connect_database():
    global db_path

    try:
        db_path = db_path.resolve(strict=False)

        if db_path.exists() and db_path.is_dir():
            raise IsADirectoryError(f"Database path points to a directory: {db_path}")

        db_path.parent.mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(str(db_path))
        conn.execute("SELECT 1")
        log(f"Using database: {db_path}")
        return conn
    except sqlite3.OperationalError as exc:
        fallback_path = Path(tempfile.gettempdir()) / "coldcounter.db"
        log(f"Unable to use {db_path}; falling back to {fallback_path}: {exc}")
        conn = sqlite3.connect(str(fallback_path))
        conn.execute("SELECT 1")
        db_path = fallback_path
        log(f"Using database: {db_path}")
        return conn
    except (sqlite3.OperationalError, OSError) as exc:
        raise RuntimeError(f"Unable to create or open the coldCounter SQLite database at {db_path}: {exc}") from exc


# --------------------------------------------------
# MAIN PIPELINE
# --------------------------------------------------

def run_pipeline():
    title_art()
    log("COLDCOUNTER ETL PIPELINE INITIATED")
    initialize_database_file()
    conn = connect_database()
    banner("INGESTING DATASETS FROM DEPORTATION DATA PROJECT")
    tables=ingest_datasets(conn)
    check_table_integrity(conn, tables)
    moneyyyyy()
    conn.close() 
    banner("DATABASE BUILD COMPLETE")
    log("coldCounter database successfully updated.")
if __name__ == "__main__":
    run_pipeline()