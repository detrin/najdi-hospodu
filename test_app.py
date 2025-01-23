import pytest
from datetime import datetime, timedelta

# Import the function to be tested
from app import get_total_minutes

def test_get_total_minutes():
    """
    Test the get_total_minutes function with actual API call.
    Uses "Anděl" and "Pankrác" as stops and datetime set to 24 hours from now.
    """
    from_stop = "Anděl"
    to_stop = "Pankrác"
    dt = datetime.now() + timedelta(hours=24)

    try:
        total_minutes = get_total_minutes(from_stop, to_stop, dt)
    except Exception as e:
        pytest.fail(f"get_total_minutes raised an exception: {e}")

    assert isinstance(total_minutes, int), "The result should be an integer."
    assert total_minutes >= 0, "Total minutes should be non-negative."