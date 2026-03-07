"""
build_dim_final_charge.py
Statute-verified ICE final charge classifier

This script builds a dimension table (dim_final_charge) in the coldCounter database
that classifies all unique final charges in the detention_stints table into legal categories.
It extracts INA (Immigration and Nationality Act) sections from charge text, verifies them against
Cornell Law Institute's Legal Information Institute API, and applies rule-based
classification to categorize charges as criminal/non-criminal, by severity, and by
specific offense types (violent crime, drug offenses, etc.).

The classification aids in analyzing the data set for patterns in criminality,
severity of charges, and compliance with legal standards. The purpose is to support 
investigative journalism on immigration enforcement and detention practices.

No Concentration Camps in Colorado - 03-06-2026
"""

import sqlite3
import pandas as pd
import regex as re
import requests
from tenacity import retry, stop_after_attempt, wait_fixed
import os


# Locate database at workspace root
workspace_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(workspace_root, "coldCounter.db")


# ----------------------------
# INA statute extraction
# ----------------------------
# This section defines functions to extract and verify INA (Immigration and Nationality Act) sections from charge text.
# INA sections are legal references like "212(a)(2)" that specify the grounds for inadmissibility or deportability.

INA_REGEX = r"(?:INA\s*)?(\d{3})\(([a-z0-9]+)\)"

def extract_ina(text):
    """
    Extracts INA section from charge text using regex.
    Returns the section in format "XXX(YY)" or None if not found.
    """
    m = re.search(INA_REGEX, text, flags=re.IGNORECASE)

    if m:
        return f"{m.group(1)}({m.group(2)})"

    return None


# ----------------------------
# Legal API verification
# ----------------------------
# This section verifies extracted INA sections against Cornell Law Institute's Legal Information Institute API.
# It constructs URLs to check if the statute exists and returns the reference URL if valid.

@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
def verify_ina_section(section):

    """
    Uses Cornell LII structured statute pages to verify INA sections.
    Retries up to 3 times with 1-second wait on failure.
    Returns the URL if the section is found, None otherwise.
    """

    if not section:
        return None

    base = "https://www.law.cornell.edu/uscode/text/8"

    try:
        url = f"{base}/{section.split('(')[0]}"
        r = requests.get(url, timeout=10)

        if r.status_code == 200:
            return url
    except:
        pass

    return None


# ----------------------------
# Legal classification rules
# ----------------------------
# This section defines regex patterns for different categories of charges.
# These patterns are used to classify charges into legal categories such as aggravated felonies, violent crimes, etc.
# Each list contains regex patterns that match specific keywords or INA sections related to that category.

AGGRAVATED_FELONY = [
    r"AGGRAVATED FELONY",
    r"101\(A\)\(43\)"
]

VIOLENT_CRIME = [
    r"MURDER",
    r"RAPE",
    r"SEXUAL ABUSE",
    r"CRIME OF VIOLENCE"
]

DRUG_CRIME = [
    r"CONTROLLED SUBSTANCE",
    r"DRUG"
]

FIREARMS = [
    r"FIREARMS",
    r"EXPLOSIVE"
]

DOMESTIC = [
    r"DOMESTIC VIOLENCE",
    r"PROTECTION ORDER"
]

CIMT = [
    r"MORAL TURPITUDE"
]

FRAUD = [
    r"FRAUD",
    r"MISREPRESENTATION",
    r"FALSE CLAIM"
]

REENTRY = [
    r"REENTRY",
    r"RE-ENTRY",
    r"AFTER REMOVAL"
]

STATUS = [
    r"OVERSTAY",
    r"OUT OF STATUS",
    r"ENTRY WITHOUT INSPECTION",
    r"WITHOUT ADMISSION",
    r"WITHOUT VISA",
    r"NOT ADMITTED",
    r"NONIMMIGRANT"
]


def match(patterns, text):
    """
    Helper function to check if any regex pattern in the list matches the text.
    Returns True if at least one pattern matches, False otherwise.
    """
    return any(re.search(p, text) for p in patterns)


# ----------------------------
# Classifier
# ----------------------------
# This section contains the main classification function that applies the rules to a charge text.
# It sets flags for criminality, severity, category, and other attributes based on pattern matching.

def classify_charge(charge):
    """
    Classifies a final charge into legal categories using regex pattern matching.
    Returns a dictionary with classification flags, category, severity, and other metadata.
    """
    text = charge.upper()

    criminal = 0
    felony = 0
    misdemeanor = 0
    violent = 0
    drug = 0
    firearms = 0
    domestic = 0
    fraud = 0
    reentry = 0
    category = "unknown"
    severity = 0
    rule = "fallback"

    if match(AGGRAVATED_FELONY, text):

        criminal = 1
        felony = 1
        category = "criminal_aggravated_felony"
        severity = 5
        rule = "aggravated_felony"

    elif match(VIOLENT_CRIME, text):

        criminal = 1
        violent = 1
        category = "violent_crime"
        severity = 4
        rule = "violent_crime"

    elif match(DRUG_CRIME, text):

        criminal = 1
        drug = 1
        category = "drug_offense"
        severity = 3
        rule = "drug_offense"

    elif match(FIREARMS, text):

        criminal = 1
        firearms = 1
        category = "firearms_offense"
        severity = 3
        rule = "firearms"

    elif match(DOMESTIC, text):

        criminal = 1
        domestic = 1
        category = "domestic_violence"
        severity = 3
        rule = "domestic_violence"

    elif match(CIMT, text):

        criminal = 1
        category = "crime_moral_turpitude"
        severity = 3
        rule = "cimt"

    elif match(REENTRY, text):

        criminal = 1
        reentry = 1
        category = "illegal_reentry"
        severity = 2
        rule = "reentry"

    elif match(FRAUD, text):

        fraud = 1
        category = "immigration_fraud"
        severity = 1
        rule = "fraud"

    elif match(STATUS, text):

        category = "civil_status_violation"
        severity = 0
        rule = "status_violation"

    ina = extract_ina(text)
    statute_url = verify_ina_section(ina)

    return {
        "criminal_flag": criminal,
        "felony_flag": felony,
        "misdemeanor_flag": misdemeanor,
        "violent_crime_flag": violent,
        "drug_offense_flag": drug,
        "firearms_flag": firearms,
        "domestic_violence_flag": domestic,
        "immigration_fraud_flag": fraud,
        "reentry_flag": reentry,
        "charge_category": category,
        "ina_section": ina,
        "ina_reference_url": statute_url,
        "severity_score": severity,
        "classification_rule": rule
    }


# ----------------------------
# Build dimension
# ----------------------------
# This section contains the main function to build the dim_final_charge table.
# It loads distinct final charges from the detention_stints table, classifies each one,
# creates a DataFrame with the classifications, and writes it to the database.

def build_dimension():
    """
    Builds the dim_final_charge dimension table by classifying all unique final charges.
    Prints progress updates to the console during execution.
    """
    print("Starting to build dim_final_charge dimension table...")

    print("Connecting to database...")
    conn = sqlite3.connect(DB_PATH)

    print("Loading distinct final charges from detention_stints table...")
    charges = pd.read_sql(
        """
        SELECT DISTINCT final_charge
        FROM detention_stints
        WHERE final_charge IS NOT NULL
        """,
        conn
    )
    print(f"Loaded {len(charges)} unique final charges.")

    rows = []
    total_charges = len(charges)

    print("Classifying charges...")
    for i, charge in enumerate(charges.final_charge, start=1):
        c = classify_charge(charge)

        row = {
            "final_charge_id": i,
            "final_charge": charge
        }

        row.update(c)
        rows.append(row)

        # Print progress every 100 charges or at the end
        if i % 100 == 0 or i == total_charges:
            print(f"Processed {i}/{total_charges} charges...")

    print("Creating DataFrame from classified charges...")
    dim = pd.DataFrame(rows)

    print("Creating dim_final_charge table in database...")
    conn.execute("""
    CREATE TABLE IF NOT EXISTS dim_final_charge (
        final_charge_id INTEGER PRIMARY KEY,
        final_charge TEXT UNIQUE,
        criminal_flag INTEGER,
        felony_flag INTEGER,
        misdemeanor_flag INTEGER,
        violent_crime_flag INTEGER,
        drug_offense_flag INTEGER,
        firearms_flag INTEGER,
        domestic_violence_flag INTEGER,
        immigration_fraud_flag INTEGER,
        reentry_flag INTEGER,
        charge_category TEXT,
        ina_section TEXT,
        ina_reference_url TEXT,
        severity_score INTEGER,
        classification_rule TEXT
    )
    """)

    print("Writing classified data to dim_final_charge table...")
    dim.to_sql("dim_final_charge", conn, if_exists="replace", index=False)

    print("Committing changes and closing database connection...")
    conn.commit()
    conn.close()

    print("dim_final_charge successfully built and populated!")


if __name__ == "__main__":
    build_dimension()