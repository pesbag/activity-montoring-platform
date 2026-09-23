from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
file_path = BASE_DIR / "Data" / "activity_readings.csv"


def validate_eventId(df):
    df = df.dropna(subset=["event_id"])
    mask = df["event_id"].astype(str).str.strip() != ""
    return df[mask]


def validate_sourceId(df):
    df = df.dropna(subset=["source_id"])
    mask = df["source_id"].astype(str).str.strip() != ""
    return df[mask]


def validate_timestamp(df):
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="mixed", errors="coerce")
    df = df.dropna(subset=["timestamp"])
    df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return df


def validate_value(df):
    df = df.copy()
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df.dropna(subset=["value"])


def clean_script(df):
    df = validate_eventId(df)
    df = validate_sourceId(df)
    df = validate_timestamp(df)
    df = validate_value(df)
    return df