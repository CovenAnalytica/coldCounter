import pandas as pd
import sqlite3
from pathlib import Path

# Get the path to the Excel file and database
current_dir = Path(__file__).parent
excel_file = current_dir / "ICOTS_NCIC_OffenseCodesList.xlsx"
db_path = current_dir.parent / "coldCounter.db"

# Read the Excel file
df = pd.read_excel(excel_file)

# Connect to the database and insert the data
conn = sqlite3.connect(db_path)
df.to_sql("dim_ncic_offense_codes", conn, if_exists="replace", index=False)
conn.close()

print("Data inserted successfully into dim_ncic_offense_codes table")