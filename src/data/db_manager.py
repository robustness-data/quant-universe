# please create a database manager class here with sqlalchemy, which has the following functions:
# 1. create a database, including the generic method and a method to fetch info from a csv file
# 2. insert data into the database, also, generic method and a method to fetch info from a csv file
# 3. query data from the database, generic method
# 4. update data in the database, generic method
# 5. delete data in the database, generic method
# 6. drop the database, generic method
# 7. close the database connection, generic method
# 8. create a table, generic method and one from a dataframe
# 9. updated a table, generic method and one from a dataframe

# Path: src/data/db_manager.py

import os, sys, logging
from pathlib import Path


import pandas as pd
from sqlalchemy import Column
from sqlalchemy import Integer, String, Float, DateTime
from sqlalchemy import MetaData
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


class DBManager:

    def __init__(self, db_name: str = 'test_db') -> None:
        self.db_name = db_name
        self.engine = None
        self.connection = None
        self.metadata = None
        self.session = None
        self.base = None
        self.tables = None
        self.table_names = None
        self.inspector = None
        self.dtype_map = {
            'int64': Integer,
            'float64': Float,
            'object': String,
            'datetime64[ns]': DateTime,
        }
        self.create_db(db_name)

    def create_db(self, db_name: str) -> None:
        """
        Create a database
        """
        try:
            self.engine = create_engine(f"sqlite:///{db_name}.db", echo=True)
            self.connection = self.engine.connect()
            self.cursor = self.engine.raw_connection().cursor()
            self.metadata = MetaData()
            self.session = sessionmaker(bind=self.engine)()
            self.base = declarative_base()
            self.tables = self.metadata.tables
            self.table_names = self.metadata.tables.keys()
            self.inspector = inspect(self.engine)
            print(f"Database {db_name} created successfully")
        except exc.SQLAlchemyError as e:
            print(f"Error: {e}")
            sys.exit(1)

    def refresh_tables(self) -> None:
        """
        Refresh the tables
        """
        self.tables = self.metadata.tables
        self.table_names = self.metadata.tables.keys()

    def create_table(self, table_name: str, columns: list) -> None:
        """
        Create a table
        """
        try:
            table = Table(table_name, self.metadata, *columns)
            table.create(self.engine, checkfirst=True)
            self.refresh_tables()
            print(f"Table {table_name} created successfully")
        except exc.SQLAlchemyError as e:
            print(f"Error: {e}")
            sys.exit(1)

    def create_table_from_df(self, table_name: str, df: pd.DataFrame) -> None:
        """
        Create a table from a dataframe
        """
        try:
            # Generate a list of columns based on the dataframe's dtypes
            columns = [Column(name, self.dtype_map[str(dtype)], index=is_index)
                       for name, dtype, is_index in zip(df.columns, df.dtypes, df.columns.isin(df.index.names))]

            # Create the table
            table = Table(table_name, self.metadata, *columns)
            table.create(self.engine, checkfirst=True)
            self.refresh_tables()
            print(f"Table {table_name} created successfully from df.")
        except exc.SQLAlchemyError as e:
            print(f"Error: {e}")
            sys.exit(1)

    def insert_data(self, table_name: str, data: list) -> None:
        """
        Insert data into a table
        """
        try:
            table = self.tables[table_name]
            query = insert(table)
            self.connection.execute(query, data)
            print(f"Data inserted into table {table_name} successfully")
        except exc.SQLAlchemyError as e:
            print(f"Error: {e}")
            sys.exit(1)

    def insert_data_from_df(self, table_name: str, df: pd.DataFrame) -> None:
        """
        Insert data into a table from a dataframe
        """
        try:
            # Create the table
            if table_name in self.table_names:
                table = self.tables[table_name]
            else:
                # Generate a list of columns based on the dataframe's dtypes
                columns = [Column(name, self.dtype_map[str(dtype)], index=is_index)
                           for name, dtype, is_index in zip(df.columns, df.dtypes, df.columns.isin(df.index.names))]
                print(f"Table {table_name} does not exist. Creating table {table_name}...")
                table = Table(table_name, self.metadata, *columns)
                table.create(self.engine, checkfirst=True)
                self.refresh_tables()

            # Insert the data
            query = insert(table)
            self.connection.execute(query, df.to_dict(orient="records"))
            print(f"Data inserted into table {table_name} successfully")

        except exc.SQLAlchemyError as e:
            print(f"Error: {e}")
            sys.exit(1)

    def query_data(self, table_name: str, columns: list, where: str = None, order_by: str = None) -> list:
        """
        Query data from a table
        """
        try:
            table = self.tables[table_name]
            query = select(text(",".join(columns))).select_from(table)
            if where:
                query = query.where(text(where))
            if order_by:
                query = query.order_by(text(order_by))
            result = self.connection.execute(query).fetchall()
            return result
        except exc.SQLAlchemyError as e:
            print(f"Error: {e}")
            sys.exit(1)

    def query_data_into_df(self, table_name: str, columns: list, where: str = None, order_by: str = None) -> pd.DataFrame:
        """
        Query data from a table into a dataframe
        """
        try:
            result = self.query_data(table_name, columns, where, order_by)
            df = pd.DataFrame(result)
            return df
        except exc.SQLAlchemyError as e:
            print(f"Error: {e}")
            sys.exit(1)

    def update_data(self, table_name: str, set: str, where: str = None) -> None:
        """
        Update data in a table
        """
        try:
            table = self.tables[table_name]
            query = update(table).values(text(set))
            if where:
                query = query.where(text(where))
            self.connection.execute(query)
            print(f"Data updated in table {table_name} successfully")
        except exc.SQLAlchemyError as e:
            print(f"Error: {e}")
            sys.exit(1)

    def delete_data(self, table_name: str, where: str = None) -> None:

        """
        Delete data in a table
        """
        try:
            table = self.tables[table_name]
            query = delete(table)
            if where:
                query = query.where(text(where))
            self.connection.execute(query)
            print(f"Data deleted in table {table_name} successfully")
        except exc.SQLAlchemyError as e:
            print(f"Error: {e}")
            sys.exit(1)

    def drop_table(self, table_name: str) -> None:
        """
        Drop a table
        """
        try:
            table = self.tables[table_name]
            table.drop(self.engine)
            print(f"Table {table_name} dropped successfully")
        except exc.SQLAlchemyError as e:
            print(f"Error: {e}")
            sys.exit(1)

    def close_connection(self) -> None:

        """
        Close the database connection
        """
        try:
            self.connection.close()
            print("Connection closed successfully")
        except exc.SQLAlchemyError as e:
            print(f"Error: {e}")
            sys.exit(1)

    def drop_db(self, db_name: str) -> None:

        """
        Drop the database
        """
        try:
            os.remove(f"{db_name}.db")
            print(f"Database {db_name} dropped successfully")
        except exc.SQLAlchemyError as e:
            print(f"Error: {e}")
            sys.exit(1)

    def create_db_from_csv(self, db_name: str, table_name: str, csv_path: str, columns: list) -> None:
        """
        Create a database from a csv file
        """
        try:
            self.create_db(db_name)
            self.create_table(table_name, columns)
            df = pd.read_csv(csv_path)
            self.insert_data_from_df(table_name, df)
        except exc.SQLAlchemyError as e:
            print(f"Error: {e}")
            sys.exit(1)

    def create_table_from_csv(self, table_name: str, csv_path: str, columns: list) -> None:

        """
        Create a table from a csv file
        """
        try:
            self.create_table(table_name, columns)
            df = pd.read_csv(csv_path)
            self.insert_data_from_df(table_name, df)
        except exc.SQLAlchemyError as e:
            print(f"Error: {e}")
            sys.exit(1)

    def update_table_from_csv(self, table_name: str, csv_path: str, columns: list) -> None:

        """
        Update a table from a csv file
        """
        try:
            df = pd.read_csv(csv_path)
            self.insert_data_from_df(table_name, df)
        except exc.SQLAlchemyError as e:
            print(f"Error: {e}")
            sys.exit(1)


class TradingViewDB(DBManager):

    def __init__(self, db_name: str = 'tradingview') -> None:
        super().__init__(db_name)
        self.create_universe_table()

    def create_universe_table(self, EQ_CACHE_DIR):
        universe_setup_df = pd.read_csv(EQ_CACHE_DIR/'3_fundamental'/'raw'/'_db_setup_universe.csv')
        self.create_table_from_df('universe', universe_setup_df)


if __name__ == "__main__":

    test_tv = TradingViewDB()

    print("Here")
    #print(test_tv.query_data_into_df('universe', ['*']))

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
        print(db_manager.table_names)
        print(db_manager.metadata.tables.keys())
        assert "test_table" in db_manager.table_names

        print(db_manager.query_data("test_table", ['id','age']))


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
        print(result)


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
        print(pd.DataFrame(result, columns=["id", "name", "age"]))


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
        print(result)


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
    test_drop_db()
    test_create_db(); print("test_create_db passed")
    #test_create_table(); print("test_create_table passed")
    #test_create_table_from_df(); print("test_create_table_from_df passed")
    #test_insert_data(); print("test_insert_data passed")
    #test_insert_data_from_df(); print("test_insert_data_from_df passed")
    #test_query_data(); print("test_query_data passed")
    #test_query_data_into_df()

    # test_update_data(); print("test_update_data passed")
    # test_delete_data(); print("test_delete_data passed")
    # test_drop_table(); print("test_drop_table passed")
    # test_close_connection(); print("test_close_connection passed")
    # test_drop_db(); print("test_drop_db passed")
    # test_create_db_from_csv(); print("test_create_db_from_csv passed")
    # test_create_table_from_csv(); print("test_create_table_from_csv passed")
    # test_update_table_from_csv(); print("test_update_table_from_csv passed")
    # print("All test cases passed")

    test_drop_db()
