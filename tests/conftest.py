import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("DataQualityTest") \
        .master("local[*]") \
        .getOrCreate()

