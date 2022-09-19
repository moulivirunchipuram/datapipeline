from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id='',
                 tables=[],
                 tests=[],
                 results=[],
                 table='',
                 test='',
                 result=0,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.tables = tables
        self.tests = tests
        self.results = results

    def execute(self, context):
        postgres_hook = PostgresHook(self.postgres_conn_id)
        count = 0
        for self.test in self.tests:
            self.log.info(f"running test sql {self.test}")
            records = postgres_hook.get_records(self.test)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. Returned no results for {self.test}")
            num_records = records[0][0]
            if num_records < self.results[count]:
                raise ValueError(f"Data quality check failed. Got {num_records}, but expected {self.results[count]}")
            self.log.info(
                f"Data quality check passed with {num_records} records with expected records >=  {self.results[count]}")
            count = count + 1