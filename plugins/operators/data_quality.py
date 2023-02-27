from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define  operators params (with defaults) 
                 redshift_conn_id = '', # Connection ID for the Redshift cluster
                 data_quality_checks = '',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.data_quality_checks = data_quality_checks


    def execute(self, context):
        self.log.info('Starting the S3ToRedshiftOperator...')  # Log that the task is starting

        redshift_hook = PostgresHook(self.redshift_conn_id)  # Create a hook to connect to the Redshift cluster

        

        for check in self.check_data_quality:
            check_sql = check.get('data_check')
            exepected_result =  check.get('expected_value')
            redshift_result = redshift.get_records(check_sql)[0]
            if redshift_result != exepected_result:
                self.log.info('Fail Data Quality Check')

        



        