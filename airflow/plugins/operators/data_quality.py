from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 tests={},
                 test_results={},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id=conn_id
        self.tests=tests
        self.test_results=test_results

    def execute(self, context):
        """The function to execute sql tests passed via
        tests dictionary and compares against the test
        results values, if tests fail then exception is 
        raised and task fails"""
        
        conn_hook=PostgresHook(self.conn_id)
        for name, sql in self.tests.items():
            result=conn_hook.get_records(sql)
            if result[0][0]==self.test_results[name]:
                self.log.info(f"Test {name} was successful")
            else:
                raise ValueError(f"Test {name} failed as {result[0][0]}\
                                   did not match the expected result\
                                   of {test_results[name]}")