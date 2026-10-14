import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


@pytest.fixture(scope="session")
def spark():
    from bdi.spark import get_spark

    s = get_spark("tests", master="local[2]")
    yield s
    s.stop()
