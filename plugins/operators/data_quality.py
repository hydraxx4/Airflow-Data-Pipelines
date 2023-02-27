from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define  operators params (with defaults) 
                 redshift_conn_id = '', # Connection ID for the Redshift cluster
                 check_data_quality = '',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.check_data_quality = check_data_quality


    def execute(self, context):
        self.log.info('Starting the S3ToRedshiftOperator...')  # Log that the task is starting

        redshift_hook = PostgresHook(self.redshift_conn_id)  # Create a hook to connect to the Redshift cluster

        def check_greater_than_zero(*args, **kwargs):  # Define a helper function for the data quality check
            table = kwargs["params"]["table"]
            # Get the number of records in the table from the Redshift cluster
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"No results were returned by {table}. Data quality check failed.")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"{table} contains 0 rows. Data quality check failed.")
            logging.info(f"Data quality check on {table} passed with {records[0][0]} records.")

        # Call the helper function for each table to be checked
        check_songplays_task = check_greater_than_zero(params={'table': 'songplays'})
        check_songs_task = check_greater_than_zero(params={'table': 'songs'})
        check_artists_task = check_greater_than_zero(params={'table': 'artists'})

        for check in self.check_data_quality:
            check_sql = check.get('data_check')
            exepected_result =  check.get('expected_value')
            redshift_result = redshift.get_records(check_sql)[0]
            if redshift_result != exepected_result:
                self.log.info('Fail Data Quality Check')

        



        