import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# We import the logic, but usually you'd refactor the logic 
# into a helper file for cleaner imports.
def check_data_quality(df):
    # Null Check Logic (5% threshold)
    null_count = df['close'].isnull().sum()
    if null_count > (len(df) * 0.05):
        raise ValueError(f"Too many NULLs: {null_count}")
    
    # Freshness Check Logic (1 hour)
    last_ts = pd.to_datetime(df['timestamp']).max()
    if (datetime.utcnow() - last_ts.replace(tzinfo=None)).total_seconds() > 3600:
        raise ValueError("Data is stale")
    return True

# --- THE TESTS ---

def test_validation_fails_on_high_nulls():
    # Create data where 10% of 'close' prices are NaN
    data = {
        'timestamp': [datetime.utcnow()] * 100,
        'close': [1.0] * 90 + [np.nan] * 10 
    }
    df = pd.DataFrame(data)
    
    with pytest.raises(ValueError, match="Too many NULLs"):
        check_data_quality(df)

def test_validation_fails_on_stale_data():
    # Create data that is 2 hours old
    stale_time = datetime.utcnow() - timedelta(hours=2)
    data = {
        'timestamp': [stale_time] * 10,
        'close': [50000.0] * 10
    }
    df = pd.DataFrame(data)
    
    with pytest.raises(ValueError, match="Data is stale"):
        check_data_quality(df)

def test_validation_passes_on_good_data():
    data = {
        'timestamp': [datetime.utcnow()] * 10,
        'close': [50000.0] * 10
    }
    df = pd.DataFrame(data)
    assert check_data_quality(df) is True
