# Import the necessary modules
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    # Set the user interface color
    ui_color = '#80BD9E'

    # Define SQL templates for inserting and truncating data
    sql_insert = """
        INSERT INTO {}
        {};
    """
    sql_truncate = """
        TRUNCATE TABLE {};
    """

    # Define the operator's constructor with input parameters
    @apply_defaults
    def __init__(self, redshift_conn_id="", table="", sql="", *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Store input parameters as instance variables
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    # Define the operator's execute method
    def execute(self, context):
        # Log the start of the execution
        self.log.info("Connecting to Redshift")

        # Create a PostgresHook to interact with Redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Truncate the dimension table
        self.log.info(f"Truncating dimension table: {self.table}")
        redshift_hook.run(LoadDimensionOperator.sql_truncate.format(self.table))

        # Load data into the dimension table
        load_sql = LoadDimensionOperator.sql_insert.format(self.table, self.sql)
        self.log.info(f"Inserting data into table {self.table}")
        redshift_hook.run(load_sql)

        # Log the completion of the execution
        self.log.info(f"Data loaded into table {self.table}")
