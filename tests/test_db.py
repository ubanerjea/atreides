# Run with: pytest tests/test_db.py -v

import pytest
from src.util.db import DBConnection


@pytest.fixture(scope="module")
def db():
    return DBConnection()


def test_onek_count(db):
    result = db.query("select count(*) from onek")
    assert result[0]["count"] == 1000


def test_tenk1_count(db):
    result = db.query("select count(*) from tenk1")
    assert result[0]["count"] == 10000


def test_tenk2_count(db):
    result = db.query("select count(*) from tenk2")
    assert result[0]["count"] == 10000

def test_onek_unique1(db):
    result = db.query("select unique1 from onek order by unique1")
    unique1 = set()
    for row in result:
        assert row["unique1"] <= 1000 and row["unique1"] >= 0
        unique1.add(row["unique1"])
    assert len(unique1) == 1000