from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 dq_checks=[],
                 *args, **kwargs):

        super().__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.dq_checks = dq_checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        error_count = 0
        failing_tests = []

        self.log.info("Starting Data Quality checks")
        for check in self.dq_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')

            try:
                self.log.info(f"Running check: {sql}")
                records = redshift.get_records(sql)[0]

                if exp_result != records[0]:
                    error_count += 1
                    failing_tests.append(sql)

                self.log.info(f"Passed check: {sql}")

            except Exception as e:
                self.log.info(f"Query failed with error: {e}")
                raise e

        if error_count > 0:
            self.log.info(f"Tests failed: {failing_tests}")
            raise ValueError('Data quality check failed')

        self.log.info("All Data Quality checks passed")