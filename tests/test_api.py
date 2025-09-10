import pytest
import os
from datetime import datetime, timedelta
import pandas as pd
from ritis_inrix_api import RITIS_Downloader, INRIX_Downloader

SAMPLE_SEGMENTS = [1236893704,1236860943]

def test_ritis_download():
    """
    Test RITIS single download.
    """
    # Clean up old test files
    if os.path.exists("tests/ritis_unit_test.parquet"):
        os.remove("tests/ritis_unit_test.parquet")

    ritis_key = os.environ.get("RITIS_API_KEY")
    if not ritis_key:
        pytest.skip("RITIS_API_KEY not set")

    downloader = RITIS_Downloader(
        api_key=ritis_key,
        segments=SAMPLE_SEGMENTS,
        download_path="tests",
        start_time='06:00:00',
        end_time='06:02:00',
        bin_size=1,
        verbose=2,
        verify=False
    )
    
    # Use yesterday's date for testing
    start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    end_date = (datetime.now()).strftime('%Y-%m-%d')

    downloader.single_download(start_date, end_date, "ritis_unit_test")

    # Check that the file was created
    assert os.path.exists("tests/ritis_unit_test.parquet")
    
    # Check that the file is not empty
    df = pd.read_parquet("tests/ritis_unit_test.parquet")
    assert not df.empty

def test_inrix_download():
    """
    Test INRIX speed data download.
    """
    inrix_id = os.environ.get("INRIX_APP_ID")
    inrix_token = os.environ.get("INRIX_HASH_TOKEN")

    if not inrix_id or not inrix_token:
        pytest.skip("INRIX_APP_ID or INRIX_HASH_TOKEN not set")

    downloader = INRIX_Downloader(
        app_id=inrix_id,
        hash_token=inrix_token,
        segments=SAMPLE_SEGMENTS,
        verbose=2
    )

    speed_data = downloader.get_speed_data()

    assert isinstance(speed_data, pd.DataFrame)
    assert not speed_data.empty
