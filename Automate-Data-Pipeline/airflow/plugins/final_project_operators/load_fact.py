from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 *args, **kwargs):
        super().__init__(*args, **kwargs)  # Modern Python 3 syntax
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        # Connect to Redshift using the Hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Loading fact table {self.table}")

        # Build the INSERT statement dynamically using the provided SQL query
        formatted_sql = f"INSERT INTO {self.table} {self.sql_query}"

        # Execute the transformation
        redshift.run(formatted_sql)

        self.log.info(f"Successfully loaded fact table {self.table}")