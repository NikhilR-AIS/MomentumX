import pandas as pd
from sqlalchemy import create_engine
import re
from getpass import getpass

# === Fill in your details ===
HOST = "a.oregon-postgres.render.com"
DB = "event_db_nikhil"
USER = "event_db_nikhil_user"
PASSWORD = getpass("Enter your Render DB password: ")  # secure prompt
PORT = 5432
SSL_MODE = "require"

# === File path ===
FILE_PATH = "/Users/admin/Desktop/AI_Squared_MomentumX_Global_Polygon_Gamescom_Event_Lead_List_August_2025_with_color.xlsx"

# === Canonical column sets (values remain strings/untouched) ===
RSVP_CANON = [
    "name","first_name","last_name","email",
    "approval_status","job_title","company","type_of_company",
    "inviter_name","linkedin","telegram","color_label"
]

DATA_CANON = [
    "type_of_company","total_count","pct_increase","color_label",
    "total_rsvp_approved","total_pending","total_sign_ups","notable_companies"
]

# === Known header variants → canonical ===
COMMON_VARIANTS = {
    # shared-style normalizations (apply after basic cleaning)
    "type_of_company": "type_of_company",
    "total_count": "total_count",
    "total_rsvp_approved": "total_rsvp_approved",
    "total_pending": "total_pending",
    "total_sign_ups": "total_sign_ups",
    "notable_companies": "notable_companies",
    "color_label": "color_label",
    "color": "color_label",               # map Color → color_label
    "pct_increase": "pct_increase",
    "percent_increase": "pct_increase",
    "percentage_increase": "pct_increase",
    "unnamed_2": "pct_increase",          # Excel placeholder column
    "job_title": "job_title",
    "company": "company",
    "approval_status": "approval_status",
    "inviter_name": "inviter_name",
    "linkedin": "linkedin",
    "telegram": "telegram",
    "name": "name",
    "first_name": "first_name",
    "last_name": "last_name",
    "email": "email",
}

# Extra fuzzy variants often seen verbatim in sheets
FUZZY_VARIANTS = {
    "what_s_your_linkedin": "linkedin",
    "what_s_your_linked_in": "linkedin",
    "kindly_provide_the_name_of_the_person_who_invited_you_to_this_e": "inviter_name",
    "total_rsvp_approved_": "total_rsvp_approved",
    "total_pending_": "total_pending",
    "total_sign_ups_": "total_sign_ups",
}

def basic_clean(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)          # non-alnum -> _
    s = re.sub(r"_+", "_", s).strip("_")       # collapse & trim _
    return s

def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    new_cols = []
    for c in df.columns:
        k = basic_clean(str(c))
        # first use fuzzy, then common
        if k in FUZZY_VARIANTS:
            k = FUZZY_VARIANTS[k]
        if k in COMMON_VARIANTS:
            k = COMMON_VARIANTS[k]
        new_cols.append(k)
    df = df.copy()
    df.columns = new_cols
    return df

def detect_family(sheet_name: str, df_cols: set) -> str:
    s = sheet_name.lower()
    if "rsvp" in s:
        return "rsvp"
    if "data" in s:
        return "data"
    # Heuristics by columns
    if {"job_title", "approval_status", "company"} & df_cols:
        return "rsvp"
    if {"total_count", "notable_companies", "pct_increase"} & df_cols:
        return "data"
    # default to RSVP (safer for person-level rows)
    return "rsvp"

def ensure_missing_columns(df: pd.DataFrame, family: str) -> pd.DataFrame:
    canon = RSVP_CANON if family == "rsvp" else DATA_CANON
    df = df.copy()
    for col in canon:
        if col not in df.columns:
            df[col] = pd.NA  # stays NULL in Postgres
    # Keep only existing + canonical columns (no value transforms)
    # but do NOT drop extra columns—LLM may still want them. We keep all.
    return df

# === Create DB connection ===
engine = create_engine(
    f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}?sslmode={SSL_MODE}"
)

# === Read workbook ===
xls = pd.ExcelFile(FILE_PATH)
print(f"Found sheets: {xls.sheet_names}")

for sheet_name in xls.sheet_names:
    print(f"\nUploading sheet: {sheet_name}...")
    df = pd.read_excel(FILE_PATH, sheet_name=sheet_name, dtype=object)  # keep raw strings
    df = normalize_headers(df)

    family = detect_family(sheet_name, set(df.columns))
    df = ensure_missing_columns(df, family)

    # Clean sheet name for table (preserve your one-table-per-sheet pattern)
    table_name = re.sub(r"[^a-zA-Z0-9_]", "_", sheet_name.lower())

    # Upload to Postgres (replace table for this sheet)
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"✅ Uploaded '{sheet_name}' -> table '{table_name}' ({len(df)} rows, family={family})")

print("\n🎉 All sheets uploaded successfully (headers normalized; values untouched)!")
