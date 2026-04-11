import pandas as pd
import pytest
from src.etl import save_events, load_events, filter_active


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "id": [1, 2, 3],
        "status": ["active", "inactive", "active"],
        "value": [10.0, 20.0, 30.0],
    })


def test_save_and_load_roundtrip(tmp_path, sample_df):
    path = str(tmp_path / "events.parquet")
    save_events(sample_df, path)
    loaded = load_events(path)
    assert list(loaded.columns) == list(sample_df.columns)
    assert len(loaded) == 3


def test_filter_active(sample_df):
    result = filter_active(sample_df)
    assert len(result) == 2
    assert all(result["status"] == "active")
