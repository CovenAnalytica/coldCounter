"""
Hold Room Statistics by State

This script compiles statistics of hold room usage for a specified state, flagging any identifiable
violations of the ICE National Detention Standards and saves the results to a CSV file, including:
  • 
  • Count of stays over 12 hours
    # ICE national detention standards state: “a detainee may not be held in a hold room for 
    # more than 12 hours.”¹ This requirement appears unchanged in both the 2019 and 2025 
    # National Detention Standards, the time frame that encompasses the scope of the dataset 
    # used in this study.
  • Count of detainees held in a hold room with no prior history of violence, over the age of 70, and held over 12 hours
    # ICE national detention standards prohibit certain vulnerable populations from being 
    # transfered to a hold room, including:
    #   * unaccompanied minors,
    #   * persons over the age of 70
    #   * women with children 
    #   * family groups in hold rooms except under limited circumstances.² 
    # This language likewise appears unchanged between the 2019 and 2025 standards, 
    # which together span the period covered by the dataset analyzed by coldCounter.

The script filters out records flagged as likely duplicate records in the dataset
and only includes stints with valid book-out dates. Results are grouped by detention facility
and sorted by facility code.

Data Sources:
  detention_stints table in coldCounter.db, updated via public data published by
  the Detention Data Project (deportationdata.org)

ICE National Detention Standards:
  2025 edition: https://www.ice.gov/doclib/detention-standards/2025/nds2025.pdf
  2019 edition: https://www.ice.gov/doclib/detention-standards/2019/nds2019.pdf
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
    """Load hold room statistics for the specified state.

    Retrieves detention stay records from coldCounter.db, filters by state,
    and computes aggregate statistics grouped by detention facility.

    Args:
        db_path (str): Path to the SQLite database.
        state (str): 2-digit state code (e.g., "CO", "CA").

    Returns:
        pd.DataFrame: Facility statistics with columns including:
            - detention_facility, detention_facility_code
            - min/max book-in/out dates in dataset
            - min/max/average length of stay (hours)
            - total stays, stays longer than 12 hours
            - age min/max (calculated at book-out date) plus counts/percentages of minors & elderly
            - non-criminal stay count/percentage (potential red flags)
            - average bond posted amount
            - count of imprisonment over 12 hours at HOLD facilities
            - count of elderly (>=70) at HOLD facilities with no prior history of violence, held over 12 hours
            - count of violent offenders
            - counts of criminality codes: 1 (criminal), 2 (pending charges), 3 (no criminal charge)
    """
    # Load data from database
    conn = sqlite3.connect(db_path)
    stn = pd.read_sql_query(
        "SELECT st.*, cd.* FROM detention_stints st LEFT JOIN dim_ncic_offense_codes cd ON st.most_serious_conviction_code = cd.Code", conn,
        parse_dates=["book_in_date_time", "book_out_date_time"],
    )
    conn.close()

    # Filter records: state, exclude duplicates, require valid book-out date
    mask = (
        (stn.state == state)
        & (stn.likely_duplicate == 0)
        & stn.book_out_date_time.notna()
        & stn.detention_facility_code.str.endswith("HOLD")  # focus on hold rooms
    )
    filtered = stn[mask].copy()

    # Compute derived fields
    filtered["age"] = filtered["book_out_date_time"].dt.year - filtered.birth_year
    filtered["hours_imprisoned"] = (
        filtered.book_out_date_time - filtered.book_in_date_time
    ).dt.total_seconds() / 3600.0
    filtered["temp_hold_over_12"] = filtered["hours_imprisoned"] >= 12
    # filtered["under18_unaccompanied"] = False  # placeholder until data available

    # add extra demographic/criminal flags
    filtered["child_prisoner"] = filtered["age"] < 18
    filtered["is_elderly"] = filtered["age"] >= 70

    # Extract criminality codes from first character of book_in_criminality
    filtered["criminality_code"] = filtered["book_in_criminality"].astype(str).str[0]

    # Classify violent offenders based on offense code type
    filtered["violent_offender"] = filtered['Type of Offense Code (V=violent, D=drug-related, Blank = nonviolent or not drug related)'] == 'V'
    filtered["criminal"] = filtered["criminality_code"] == "1"  # criminal
    filtered["charged"] = filtered["criminality_code"] == "2"  # pending charges
    filtered["no_charge"] = filtered["criminality_code"] == "3"  # no criminal charge
    filtered["under18_unaccompanied"] = False  # placeholder until data available


    # Add hold-specific counters
    filtered["hold_over_12_hours"] = (
        (filtered["hours_imprisoned"] > 12) & 
        (filtered["detention_facility_code"].str.endswith("HOLD"))
    )
    filtered["hold_elderly_over_70"] = (
        (filtered["age"] >= 70) & 
        (filtered["hours_imprisoned"] > 12) &
        (filtered["detention_facility_code"].str.endswith("HOLD")) &
        (~filtered["violent_offender"])
    )

    # Group by facility and compute aggregate statistics
    grouped = (
        filtered
        .groupby(["detention_facility", "detention_facility_code"])
        .agg(
            min_book_in_date_in_dataset=("book_in_date_time", "min"),
            max_book_out_date_in_dataset=("book_out_date_time", "max"),
            min_hours=("hours_imprisoned", "min"),
            max_hours=("hours_imprisoned", "max"),
            average_hours=("hours_imprisoned", "mean"),
            total_stays_recorded_at_facility=("stay_ID", "count"),
            min_age=("age", "min"),
            max_age=("age", "max"),
            children=("child_prisoner", "sum"),
            elderly=("is_elderly", "sum"),
            count_non_criminal=("no_charge", "sum"),
            pct_non_criminal=("no_charge", "mean"),
            avg_bond_posted=("bond_posted_amount", "mean"),
            count_under18_unaccompanied=("under18_unaccompanied", "sum"),
            violations_hold_over_12_hours=("hold_over_12_hours", "sum"),
            violations_hold_nv_elderly_over_70=("hold_elderly_over_70", "sum"),
            count_violent_offenders=("violent_offender", "sum"),
            count_convicted_criminals=("criminal", "sum"),
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
        os.path.dirname(os.path.abspath(__file__))
    )
    db_path = os.path.join(workspace_root, "coldCounter.db")

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
        filename = f"{state}_hold_rooms_coldCounter.csv"
        filepath = os.path.join(workspace_root, filename)
        df.to_csv(filepath, index=False)
        print(f"Detention facility statistics saved to: {filepath}")


