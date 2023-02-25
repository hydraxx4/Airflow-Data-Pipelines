# Import necessary modules
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Define a custom operator to stage data from S3 to Redshift
class S3ToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    # Define the SQL template to copy data from S3 to Redshift
    copy_sql_template = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        FORMAT AS JSON '{}'      
    """

    # Define the operator's constructor to accept required parameters
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_conn_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 *args, **kwargs):
        super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)

        # Set instance variables to store operator parameters
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    # Define the operator's execute method
    def execute(self, context):
        # Log that the operator is starting
        self.log.info('S3ToRedshiftOperator is starting...')

        # Retrieve AWS and Redshift credentials using hooks
        aws_hook = AwsHook(aws_conn_id=self.aws_conn_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Clear data from destination Redshift table
        self.log.info(f"Clearing data from destination Redshift table {self.table}")
        redshift_hook.run(f"DELETE FROM {self.table}")

        # Copy data from S3 to Redshift
        self.log.info(f"Copying data from S3 to Redshift table {self.table}")
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key.format(**context)}"
        copy_sql = self.copy_sql_template.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_format
        )
        redshift_hook.run(copy_sql)

        # Log that the operator is done
        self.log.info(f"S3ToRedshiftOperator finished copying data from S3 to Redshift table {self.table}")
