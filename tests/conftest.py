import os
import sys
from pathlib import Path

import pytest

if os.name == "nt":
    if not os.environ.get("HADOOP_HOME") and os.path.exists(r"C:\hadoop"):
        os.environ["HADOOP_HOME"] = r"C:\hadoop"
    if os.path.exists(r"C:\hadoop\bin") and r"C:\hadoop\bin" not in os.environ.get("PATH", ""):
        os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


@pytest.fixture(scope="session")
def spark():
    from bdi.spark import get_spark

    s = get_spark("tests", master="local[2]")
    yield s
    s.stop()
