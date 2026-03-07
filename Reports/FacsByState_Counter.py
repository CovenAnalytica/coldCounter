"""
Detention Facility Statistics by State

This script computes detention facility statistics for a specified state and saves
the results to a CSV file, including:
  • facility category (hold room, jail, hospital, other)
  • minimum, maximum, and average length of stay (in days)
  • total number of stints recorded at each facility
  • count of stays longer than 3 days
  • minimum and maximum age of detainees (calculated at book-out date)
  • counts of criminality codes: 1 (criminal), 2 (pending charges), 3 (no criminal charge)

The script filters out records flagged as likely duplicate records in the dataset
 and only includes stints with valid book-out dates. Results are grouped by detention facility and sorted by facility code.

Data Source:
  detention_stints table in coldCounter.db, updated via public data published by
  the Detention Data Project (deportationdata.org)

Data Disclaimer:
  The dataset may have limitations and may not be fully representative of all
  detention facilities or stays. Statistics are based on available data and should
  be interpreted with caution.

Usage:
  python FacsByState_Counter.py CO          # Run for Colorado, choose folder for CSV
  python FacsByState_Counter.py             # Prompt for state code, then choose folder
"""

import sqlite3
import pandas as pd


def load_facility_stats(db_path: str, state: str) -> pd.DataFrame:
    """Load detention facility statistics for the specified state.

    Retrieves detention stay records from coldCounter.db, filters by state,
    and computes aggregate statistics grouped by detention facility.

    Args:
        db_path (str): Path to the SQLite database.
        state (str): 2-digit state code (e.g., "CO", "CA").

    Returns:
        pd.DataFrame: Facility statistics with columns including:
            - detention_facility, detention_facility_code, facility_category
            - min/max book-in/out dates in dataset
            - min/max/average length of stay (days)
            - total stays, stays longer than 3 days
            - age min/max (calculated at book-out date) plus counts/percentages of minors & elderly
            - average bond posted amount
            - count of imprisonment over 3 days at HOLD facilities
            - count of elderly (>70) at HOLD facilities
            - counts of criminality codes: 1 (criminal), 2 (pending charges), 3 (no criminal charge)
    """
    # Load data from database
    conn = sqlite3.connect(db_path)
    stn = pd.read_sql_query(
        "SELECT * FROM detention_stints", conn,
        parse_dates=["book_in_date_time", "book_out_date_time"],
    )
    conn.close()

    # Filter records: state, exclude duplicates, require valid book-out date
    mask = (
        (stn.state == state)
        & (stn.likely_duplicate == 0)
        & stn.stay_book_out_date.notna()
    )
    filtered = stn[mask].copy()

    # Compute derived fields
    filtered["age"] = filtered["book_out_date_time"].dt.year - filtered.birth_year
    filtered["days_imprisoned"] = (
        filtered.book_out_date_time - filtered.book_in_date_time
    ).dt.total_seconds() / 86400.0
    filtered["temp_hold_over_72"] = filtered["days_imprisoned"] >= 3 
    # filtered["under18_unaccompanied"] = False  # placeholder until data available

    # add extra demographic/criminal flags
    filtered["child_prisoner"] = filtered["age"] < 18
    filtered["is_elderly"] = filtered["age"] >= 70

    # Extract criminality codes from first character of book_in_criminality
    filtered["criminality_code"] = filtered["book_in_criminality"].astype(str).str[0]
    filtered["criminal"] = filtered["criminality_code"] == "1"  # criminal
    filtered["charged"] = filtered["criminality_code"] == "2"  # pending charges
    filtered["no_charge"] = filtered["criminality_code"] == "3"  # no criminal charge
    filtered["criminal_and_pending"] = filtered["criminal"] | filtered["charged"]


    # Add hold-specific counters
    filtered["hold_over_3_days"] = (
        (filtered["days_imprisoned"] > 3) & 
        (filtered["detention_facility_code"].str.endswith("HOLD"))
    )
    filtered["hold_elderly_over_70"] = (
        (filtered["age"] > 70) & 
        (filtered["detention_facility_code"].str.endswith("HOLD"))
    )

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

    # Group by facility and compute aggregate statistics
    grouped = (
        filtered
        .groupby(["detention_facility", "detention_facility_code"])
        .agg(
            facility_category=('facility_category', 'first'),
            min_book_in_date_in_dataset=("book_in_date_time", "min"),
            max_book_out_date_in_dataset=("book_out_date_time", "max"),
            min_days=("days_imprisoned", "min"),
            max_days=("days_imprisoned", "max"),
            average_days=("days_imprisoned", "mean"),
            total_stays_recorded_at_facility=("stay_ID", "count"),
            min_age=("age", "min"),
            max_age=("age", "max"),
            children=("child_prisoner", "sum"),
            elderly=("is_elderly", "sum"),
            count_non_criminal=("criminal_and_pending", "sum"),
            pct_non_criminal=("criminal_and_pending", "mean"),
            avg_bond_posted=("bond_posted_amount", "mean"),
            count_criminal=("criminal", "sum"),
            count_charged=("charged", "sum"),
            count_no_charges=("no_charge", "sum"),
        )
        .round(2)
        .reset_index()
        .sort_values("detention_facility_code")
    )

    return grouped


if __name__ == "__main__":
    import argparse
    import os

    # Locate database at workspace root
    workspace_root = os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    db_path = os.path.join(workspace_root,"coldCounter", "coldCounter.db")

    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description="Compute detention facility statistics for a given state"
    )
    parser.add_argument(
        "state",
        nargs="?",
        help="2-digit state code (e.g., CO, CA, TX)",
    )
    args = parser.parse_args()

    # Get state code from argument or prompt user
    state = args.state
    if not state:
        state = input("Enter 2-digit state code (e.g., CO, CA, TX): ").strip().upper()

    # Validate state code format
    if not state or len(state) != 2 or not state.isalpha():
        print("Error: State code must be exactly 2 letters (e.g., CO, CA).")
        exit(1)

    # Load statistics and save to CSV
    df = load_facility_stats(db_path, state)
    if df.empty:
        print(f"No data found for state '{state}'.")
    else:
        filename = f"{state}_facilities_coldCounter.csv"
        filepath = os.path.join(workspace_root,"coldCounter", filename)
        df.to_csv(filepath, index=False)
        print(f"Detention facility statistics saved to: {filepath}")


