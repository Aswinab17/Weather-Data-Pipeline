import os
import sys

def setup_pyspark_env():
    """Ensure PySpark uses the correct Python interpreter."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
