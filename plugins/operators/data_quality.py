from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define  operators params (with defaults) 
                 redshift_conn_id = '',
                 data_quality_check = '',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.data_quality_check = data_quality_check

    def execute(self, context):
        # Log that the operator is starting
        self.log.info('S3ToRedshiftOperator is starting...')

        redshift_hook = PostgresHook(self.redshift_conn_id)

        def check_greater_than_zero(*args, **kwargs):
            table = kwargs["params"]["table"]
            redshift_hook = PostgresHook("redshift")
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            logging.info(f"Data quality on table {table} check passed with {records[0][0]} records")
        
        check_songplays_task = check_greater_than_zero(
            params={
                'table':'songplays'
                }
        ) 

        check_songs_task = check_greater_than_zero(
            params={
                'table':'songs'
                }
        )  

        check_artists_task = check_greater_than_zero(
            params={
                'table':'artists'
                }
        )  


        