# -*- coding: utf-8 -*-
"""
Created on Wed Mar  4

etl_deportation_project_v0.1.py

Purpose:
   This script acts as an ETL pipeline between the coldCounter SQLite3
   database, and public data published by the Deportation Data Project. 
Scope:
   All public data publisheed by the Deportation Data Project
   deportationdata.org
Developer Notes:
   The Deportation Data Project obtains, posts, and analyzes 
   internal U.S. government immigration enforcement data via 
   rigorous public records litigation. 
   
   Webinar: Inroduction to the ICE data: https://deportationdata.org/docs/ice.html#webinar
   Codebook: https://deportationdata.org/docs/ice/codebook.html
@author: DG ~
"""

import requests
import pandas as pd
import sqlite3
from io import BytesIO
from datetime import datetime
import os

#----------------------
# CONFIG
#----------------------
# Each urlset is a download link from one of the available datasets 
datasets = [
    {"url": "https://github.com/deportationdata/ice/raw/refs/heads/main/data/arrests-latest.xlsx", "table": "arrests"},
    {"url": "https://github.com/deportationdata/ice/raw/refs/heads/main/data/detainers-latest.xlsx", "table": "detainers"},
    {"url": "https://github.com/deportationdata/ice/raw/refs/heads/main/data/detention-stints-latest.xlsx", "table": "detention_stints"},
    {"url": "https://github.com/deportationdata/ice/raw/refs/heads/main/data/detention-stays-latest.xlsx", "table": "detention_stays"},
]
# Locate database at workspace root
workspace_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(workspace_root, "coldCounter.db")

#----------------------
# FUNCTION FOR LOGGING
#----------------------
def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

#----------------------
# MAIN ETL LOOP
#----------------------
conn = sqlite3.connect(db_path)

for dataset in datasets:
    url = dataset["url"]
    table_name = dataset["table"]

    try:
        log(f"Starting ETL for table '{table_name}' from URL: {url}")

        # Download
        log("Downloading dataset...")
        response = requests.get(url)
        response.raise_for_status()
        log(f"Download completed ({len(response.content)} bytes)")

        # Load into pandas
        log("Loading dataset into pandas DataFrame...")
        df = pd.read_excel(BytesIO(response.content))
        log(f"DataFrame loaded: {len(df)} rows, {len(df.columns)} columns")

        # Write to SQLite
        log(f"Writing data to coldCounter table '{table_name}' (if_exists='replace')...")
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        log(f"Table '{table_name}' written successfully")

    except requests.HTTPError as e:
        log(f"HTTP error while downloading {url}: {e}")
    except pd.errors.ExcelFileError as e:
        log(f"Excel parsing error for {url}: {e}")
    except Exception as e:
        log(f"Unexpected error for table '{table_name}': {e}")

log("All ETL processes completed")
conn.close()