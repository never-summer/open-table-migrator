import pandas as pd
import os

DATA_PATH = "data/events.parquet"


def load_events(path: str = DATA_PATH) -> pd.DataFrame:
    return pd.read_parquet(path)


def save_events(df: pd.DataFrame, path: str = DATA_PATH) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)


def filter_active(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["status"] == "active"]
