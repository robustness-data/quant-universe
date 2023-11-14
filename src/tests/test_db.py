import os, sys, logging
logger = logging.getLogger('db_manager')
import pandas as pd
from pathlib import Path
import src.config as cfg
from src.utils.pandas_utils import df_filter, set_cols_numeric
from src.data.database.db_manager import DBManager, TradingViewDB

from sqlalchemy import Column
from sqlalchemy import Integer, String, Float, DateTime
from sqlalchemy import MetaData
from sqlalchemy import PrimaryKeyConstraint
from sqlalchemy import Table
from sqlalchemy import create_engine
from sqlalchemy import delete
from sqlalchemy import exc
from sqlalchemy import insert
from sqlalchemy import inspect
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy import update
from sqlalchemy.orm import sessionmaker, declarative_base


test_tv = TradingViewDB()

logger.info("Here")
logger.info(test_tv.query_data_into_df('universe', ['*']).columns)


def test_create_db():
    db_manager = DBManager('test_db')
    assert db_manager.engine != None
    assert db_manager.connection != None
    assert db_manager.metadata != None
    assert db_manager.session != None
    assert db_manager.base != None
    assert db_manager.tables != None
    assert db_manager.table_names != None
    assert db_manager.inspector != None


def test_create_table():
    db_manager = DBManager("test_db")
    db_manager.create_table("test_table", [
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("age", Integer)
    ])
    assert "test_table" in db_manager.table_names


def test_create_table_from_df():
    db_manager = DBManager('test_db')
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Tom", "Jerry", "Spike"],
        "age": [10, 20, 30]
    })
    db_manager.create_table_from_df("test_table", df)
    logger.info(db_manager.table_names)
    logger.info(db_manager.metadata.tables.keys())
    assert "test_table" in db_manager.table_names

    logger.info(db_manager.query_data("test_table", ['id', 'age']))


def test_insert_data():
    db_manager = DBManager("test_db")
    db_manager.create_table("test_table", [
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("age", Integer)
    ])
    db_manager.insert_data("test_table", [
        {"id": 1, "name": "Tom", "age": 10},
        {"id": 2, "name": "Jerry", "age": 20},
        {"id": 3, "name": "Spike", "age": 30}
    ])
    result = db_manager.query_data("test_table", ["*"])
    assert len(result) == 3
    logger.info(result)


def test_insert_data_from_df():
    db_manager = DBManager()
    db_manager.create_db("test_db")
    db_manager.create_table("test_table", [
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("age", Integer)
    ])
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Tom", "Jerry", "Spike"],
        "age": [10, 20, 30]
    })
    db_manager.insert_data_from_df("test_table", df)
    result = db_manager.query_data("test_table", ["*"])
    assert len(result) == 3


def test_query_data():
    db_manager = DBManager()
    db_manager.create_db("test_db")
    db_manager.create_table("test_table", [
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("age", Integer)
    ])
    db_manager.insert_data("test_table", [
        {"id": 1, "name": "Tom", "age": 10},
        {"id": 2, "name": "Jerry", "age": 20},
        {"id": 3, "name": "Spike", "age": 30}
    ])
    result = db_manager.query_data("test_table", ["*"])
    assert len(result) == 3
    logger.info(pd.DataFrame(result, columns=["id", "name", "age"]))


def test_query_data_into_df():
    db_manager = DBManager()
    db_manager.create_db("test_db")
    db_manager.create_table("test_table", [
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("age", Integer)
    ])
    db_manager.insert_data("test_table", [
        {"id": 1, "name": "Tom", "age": 10},
        {"id": 2, "name": "Jerry", "age": 20},
        {"id": 3, "name": "Spike", "age": 30}
    ])
    result = db_manager.query_data_into_df("test_table", ["*"])
    assert len(result) == 3
    logger.info(result)


def test_update_data():
    db_manager = DBManager()
    db_manager.create_db("test_db")
    db_manager.create_table("test_table", [
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("age", Integer)
    ])
    db_manager.insert_data("test_table", [
        {"id": 1, "name": "Tom", "age": 10},
        {"id": 2, "name": "Jerry", "age": 20},
        {"id": 3, "name": "Spike", "age": 30}
    ])
    db_manager.update_data("test_table", "name='Tommy'", "id=1")
    result = db_manager.query_data("test_table", ["*"], "id=1")
    assert result[0][1] == "Tommy"


def test_delete_data():
    db_manager = DBManager()
    db_manager.create_db("test_db")
    db_manager.create_table("test_table", [
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("age", Integer)
    ])
    db_manager.insert_data("test_table", [
        {"id": 1, "name": "Tom", "age": 10},
        {"id": 2, "name": "Jerry", "age": 20},
        {"id": 3, "name": "Spike", "age": 30}
    ])
    db_manager.delete_data("test_table", "id=1")
    result = db_manager.query_data("test_table", ["*"])
    assert len(result) == 2


def test_drop_table():
    db_manager = DBManager()
    db_manager.create_db("test_db")
    db_manager.create_table("test_table", [
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("age", Integer)
    ])
    db_manager.drop_table("test_table")
    assert "test_table" not in db_manager.table_names


def test_close_connection():
    db_manager = DBManager()
    db_manager.create_db("test_db")
    db_manager.close_connection()
    assert db_manager.connection.closed == True


def test_drop_db():
    db_manager = DBManager()
    db_manager.create_db("test_db")
    db_manager.drop_db("test_db")
    assert os.path.exists("test_db.db") == False


def test_create_db_from_csv():
    db_manager = DBManager()
    db_manager.create_db_from_csv("test_db", "test_table", "test.csv", [
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("age", Integer)
    ])
    assert "test_table" in db_manager.table_names


def test_create_table_from_csv():
    db_manager = DBManager()
    db_manager.create_db("test_db")
    db_manager.create_table_from_csv("test_table", "test.csv", [
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("age", Integer)
    ])
    assert "test_table" in db_manager.table_names


def test_update_table_from_csv():
    db_manager = DBManager()
    db_manager.create_db("test_db")
    db_manager.create_table("test_table", [
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("age", Integer)
    ])
    db_manager.update_table_from_csv("test_table", "test.csv", [
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("age", Integer)
    ])
    result = db_manager.query_data("test_table", ["*"])
    assert len(result) == 3

# run all test cases
# test_drop_db()
# test_create_db(); logger.info("test_create_db passed")
# test_create_table(); logger.info("test_create_table passed")
# test_create_table_from_df(); logger.info("test_create_table_from_df passed")
# test_insert_data(); logger.info("test_insert_data passed")
# test_insert_data_from_df(); logger.info("test_insert_data_from_df passed")
# test_query_data(); logger.info("test_query_data passed")
# test_query_data_into_df()

# test_update_data(); logger.info("test_update_data passed")
# test_delete_data(); logger.info("test_delete_data passed")
# test_drop_table(); logger.info("test_drop_table passed")
# test_close_connection(); logger.info("test_close_connection passed")
# test_drop_db(); logger.info("test_drop_db passed")
# test_create_db_from_csv(); logger.info("test_create_db_from_csv passed")
# test_create_table_from_csv(); logger.info("test_create_table_from_csv passed")
# test_update_table_from_csv(); logger.info("test_update_table_from_csv passed")
# logger.info("All test cases passed")

# test_drop_db()
