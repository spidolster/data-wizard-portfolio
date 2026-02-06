import pytest
import pandas as pd
import sys
import os

# Ensure src is in path
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

from pipeline import CoalLogisticsPipeline

@pytest.fixture
def mock_pipeline():
    config = {'user': 'test', 'password': 'pwd', 'host': 'localhost', 'port': '5432', 'dbname': 'testdb'}
    return CoalLogisticsPipeline(config)

def test_transformation_logic(mock_pipeline):
    """Test that transit time calculation and risk flagging works correctly."""
    data = {
        'shipment_id': ['SHP-001', 'SHP-002'],
        'status': ['In Transit', 'Delayed'],
        'departure_time': ['2026-02-01 10:00:00', '2026-02-01 10:00:00'],
        'expected_arrival': ['2026-02-02 10:00:00', '2026-02-04 10:00:00'] # 24h vs 72h
    }
    df = pd.DataFrame(data)
    
    # Run transform
    clean_df = mock_pipeline.transform(df)
    
    # Assertions
    assert clean_df.loc[0, 'transit_hours'] == 24.0
    assert clean_df.loc[0, 'risk_flag'] == 'LOW'
    
    assert clean_df.loc[1, 'transit_hours'] == 72.0
    assert clean_df.loc[1, 'risk_flag'] == 'HIGH' # > 48h
    assert clean_df.loc[1, 'status'] == 'Delayed' # Should also trigger high risk if logic holds

def test_validation_schema(mock_pipeline):
    """Ensure required columns exist."""
    df = pd.DataFrame({'wrong_col': [1]})
    try:
        mock_pipeline.transform(df)
    except KeyError:
        assert True
    except Exception:
        pytest.fail("Should have raised KeyError for missing columns")
