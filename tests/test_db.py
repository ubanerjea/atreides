# Run with: pytest tests/test_db.py -v

import pytest
from src.db import DBConnection


@pytest.fixture(scope="module")
def db():
    return DBConnection()


def test_onek_count(db):
    result = db.query("select count(*) from onek")
    assert result[0][0] == 1000


def test_tenk1_count(db):
    result = db.query("select count(*) from tenk1")
    assert result[0][0] == 10000


def test_tenk2_count(db):
    result = db.query("select count(*) from tenk2")
    assert result[0][0] == 10000
