import pytest
from pyspark.sql import SparkSession

from tests.fixtures import *  # noqa


@pytest.fixture
def spark():
    return (
        SparkSession.builder
        .appName("Pytest Job")
        .getOrCreate()
    )
