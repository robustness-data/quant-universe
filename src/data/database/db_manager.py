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
import pandas as pd
from pathlib import Path
import src.config as cfg
from src.utils.pandas_utils import df_filter, set_cols_numeric

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


class DBManager:

    def __init__(self, db_name: str = 'test_db', echo=False) -> None:
        self.db_name = db_name
        self.echo = echo
        self.engine = None
        self.connection = None
        self.metadata = None
        self.session = None
        self.base = None
        self.tables = None
        self.table_names = None
        self.inspector = None
        self.dtype_map = {
            'int': Integer,
            'int64': Integer,
            'float': Float,
            'float64': Float,
            'float32': Float,
            'str': String,
            'object': String,
            'datetime64[ns]': DateTime,
            'datetime64': DateTime,
            'datetime': DateTime,
            'datetime64[ns, America/New_York]': DateTime
        }
        self.create_db(str(cfg.DB_DIR/db_name))

    def create_db(self, db_name: str) -> None:
        """
        Create a database
        """
        try:
            self.engine = create_engine(f"sqlite:///{db_name}.db", echo=self.echo)
            self.metadata = MetaData()
            self.session = sessionmaker(bind=self.engine)()
            self.base = declarative_base()
            self.tables = self.metadata.tables
            self.table_names = self.metadata.tables.keys()
            self.inspector = inspect(self.engine)
            logging.info(f"Database {db_name} created successfully")
        except exc.SQLAlchemyError as e:
            logging.info(f"Error: {e}")
            sys.exit(1)

    def refresh_tables(self) -> None:
        """
        Refresh the tables
        """
        self.tables = self.metadata.tables
        self.table_names = self.metadata.tables.keys()

    def create_columns(self, columns: list) -> list:
        """
        Create a list of column objects from a list of tuples
        :param columns: a list of tuples, each tuple contains the column name, dtype, and whether it is an index
        :return: a list of column objects
        """

        columns = [Column(name, self.dtype_map[str(dtype)], index=is_index) for name, dtype, is_index in columns]
        return columns

    def create_table(self, table_name: str, columns: list) -> None:
        """
        Create a table
        """
        try:
            if table_name in self.table_names:
                logging.warning(f"Table {table_name} already exists.")
                return
            table = Table(table_name, self.metadata, *columns)
            with self.engine.begin() as connection:
                table.create(connection, checkfirst=True)
            self.refresh_tables()
            logging.info(f"Table {table_name} created successfully")
        except exc.SQLAlchemyError as e:
            logging.info(f"Error: {e}")
            sys.exit(1)

    def create_table_from_df(self, table_name: str, df: pd.DataFrame, primary_keys: list = None) -> None:
        """
        Create a table from a dataframe
        :parameter
        :param table_name: the name of the table
        :param df: the dataframe
        :param primary_keys: a list of primary keys
        """
        try:
            if table_name in self.table_names:
                logging.warning(f"Table {table_name} already exists.")
                return

            # Generate a list of columns based on the dataframe's dtypes
            columns = [Column(name, self.dtype_map[str(dtype)]) for name, dtype in zip(df.columns, df.dtypes)]

            # Create the table
            if primary_keys:
                table = Table(table_name, self.metadata, *columns, PrimaryKeyConstraint(*primary_keys))
            else:
                table = Table(table_name, self.metadata, *columns)

            with self.engine.begin() as connection:
                table.create(connection, checkfirst=True)

            self.refresh_tables()
            logging.info(f"Table {table_name} created successfully from df.")
        except exc.SQLAlchemyError as e:
            logging.info(f"Error: {e}")
            sys.exit(1)

    def insert_data(self, table_name: str, data: list) -> None:
        """
        Insert data into a table
        """
        try:
            table = self.tables[table_name]
            query = insert(table)
            with self.engine.begin() as connection:
                connection.execute(query, data)
            logging.info(f"Data inserted into table {table_name} successfully")
        except exc.SQLAlchemyError as e:
            logging.info(f"Error: {e}")
            sys.exit(1)

    def insert_data_from_df(self, table_name: str, df: pd.DataFrame) -> None:
        """
        Insert data into a table from a dataframe
        """
        try:
            # Create the table
            if table_name not in self.table_names:
                self.create_table_from_df(table_name, df)
            table = self.tables[table_name]
            query = insert(table)
            with self.engine.begin() as connection:
                connection.execute(query, df.to_dict(orient="records"))
            logging.info(f"Data inserted into table {table_name} successfully")

        except exc.SQLAlchemyError as e:
            logging.info(f"Error: {e}")
            sys.exit(1)

    def query_data(self, table_name: str, columns: list, where: str = None, order_by: str = None) -> list:
        """
        Query data from a table
        """
        try:
            if table_name not in self.tables:
                raise Warning(f"Table {table_name} does not exist!")
                sys.exit(1)
            table = self.tables[table_name]

            # prepare query
            query = select(text(",".join(columns))).select_from(table)
            if where:
                query = query.where(text(where))
            if order_by:
                query = query.order_by(text(order_by))

            # run query and get results
            with self.engine.begin() as connection:
                result = connection.execute(query).fetchall()

            return result

        except exc.SQLAlchemyError as e:
            logging.info(f"Error: {e}")
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
            logging.info(f"Error: {e}")
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
            with self.engine.begin() as connection:
                connection.execute(query)
            logging.info(f"Data updated in table {table_name} successfully")
        except exc.SQLAlchemyError as e:
            logging.info(f"Error: {e}")
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
            with self.engine.begin() as connection:
                connection.execute(query)
            logging.info(f"Data deleted in table {table_name} successfully")
        except exc.SQLAlchemyError as e:
            logging.info(f"Error: {e}")
            sys.exit(1)

    def drop_table(self, table_name: str) -> None:
        """
        Drop a table
        """
        try:
            table = self.tables[table_name]
            table.drop(self.engine)
            with self.engine.begin() as connection:
                table.drop(connection)
            logging.info(f"Table {table_name} dropped successfully")
        except exc.SQLAlchemyError as e:
            logging.info(f"Error: {e}")
            sys.exit(1)

    @staticmethod
    def drop_db(db_name: str) -> None:

        """
        Drop the database
        """
        try:
            os.remove(f"{str(cfg.DB_DIR/db_name)}.db")
            logging.info(f"Database {db_name} dropped successfully")
        except exc.SQLAlchemyError as e:
            logging.info(f"Error: {e}")
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
            logging.info(f"Error: {e}")
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
            logging.info(f"Error: {e}")
            sys.exit(1)


class TradingViewDB(DBManager):

    def __init__(self, db_name: str = 'tradingview') -> None:
        super().__init__(db_name)
        self.create_universe_table()

    def create_universe_table(self):
        universe_setup_df = pd.read_csv(cfg.TV_CACHE_DIR/'raw'/'_db_setup_universe.csv')
        self.create_table_from_df('universe', universe_setup_df)

    def populate_universe_table(self, universe_df: pd.DataFrame, filter_spec=None):
        self.insert_data_from_df(table_name='universe', df=df_filter(df=universe_df, filter_dict=filter_spec))