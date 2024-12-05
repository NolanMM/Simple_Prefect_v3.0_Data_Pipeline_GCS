from sqlalchemy import create_engine, text, inspect
from google.cloud.sql.connector import Connector
import polars as pl

class DatabaseManager:
    def __init__(self, instance_connection_name, db_user, db_pass, db_name):
        """
        Initialize the DatabaseManager with connection details.
        """
        self.instance_connection_name = instance_connection_name
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_name = db_name
        self.connector = Connector()
        self.pool = self.create_connection_pool()

    def create_connection_pool(self):
        """
        Create and return a SQLAlchemy connection pool using the Connector object.
        """
        return create_engine(
            "mysql+pymysql://",
            creator=lambda: self.connector.connect(
                self.instance_connection_name,
                "pymysql",
                user=self.db_user,
                password=self.db_pass,
                db=self.db_name
            )
        )

    def execute_query(self, query):
        """
        Execute a given SQL query using the connection pool.
        """
        with self.pool.connect() as conn:
            conn.execute(text(query))

    def table_exists(self, table_name):
        """
        Check if a table exists in the database.
        """
        with self.pool.connect() as conn:
            inspector = inspect(conn)
            return table_name in inspector.get_table_names()

    def insert_polars_df(self, table_name, df):
        """
        Insert a Polars DataFrame into the specified table.
        """
        if not isinstance(df, pl.DataFrame):
            raise ValueError("Input must be a Polars DataFrame.")
        pandas_df = df.to_pandas()
        with self.pool.connect() as conn:
            pandas_df.to_sql(table_name, conn, if_exists="append", index=False)
            
def check_and_create_table(db_manager:DatabaseManager, table_name):
    """
    Create a table if it does not exist and populate it with data from a Polars DataFrame.
    
    Args:
        db_manager (DatabaseManager): An instance of the DatabaseManager class.
        table_name (str): Name of the table to create or populate.
        data (dict): Data to populate the table, provided as a dictionary to be converted to Polars DataFrame.
    
    Returns:
        None
    """
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        `Date` DATE NOT NULL,
        `DJI_Open` DECIMAL(10, 2) NULL,
        `DJI_High` DECIMAL(10, 2) NULL,
        `DJI_Low` DECIMAL(10, 2) NULL,
        `DJI_Close` DECIMAL(10, 2) NULL,
        `DJI_Adj_Close` DECIMAL(10, 2) NULL,
        `DJI_Volume` BIGINT NULL,
        PRIMARY KEY (`Date`)
    );
    """
    if not db_manager.table_exists(table_name):
        db_manager.execute_query(create_table_query)
        print(f"Table `{table_name}` created.")
    else:
        print(f"Table `{table_name}` already exists.")


def append_data_to_table(db_manager:DatabaseManager, table_name, data:pl.DataFrame):
    try:
        db_manager.insert_polars_df(table_name, data)
        print("Data inserted successfully.")
        return True
    except Exception as e:
        print(f"Error inserting data: {e}")
        return False
            
