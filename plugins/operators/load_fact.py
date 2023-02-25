# Import the necessary modules
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    # Set the user interface color
    ui_color = '#F98866'

    # Define the SQL insert statement as a class variable
    sql_insert = """
        INSERT INTO {}
        {};
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
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Format the SQL insert statement with the table and SQL statement
        formatted_sql = LoadFactOperator.sql_insert.format(self.table, self.sql)

        # Execute the formatted SQL statement using the Redshift hook
        redshift.run(formatted_sql)

        # Log the completion of the execution
        self.log.info(f"Data inserted into table {self.table}")
