from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    insert_sql="""INSERT INTO {target_table}
                  ({sql})"""

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 sql="",
                 target_table="",
                 truncate_table=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id=conn_id
        self.sql=sql
        self.target_table=target_table
        self.truncate_table=truncate_table

    def execute(self, context):
        """Loads dimension tables to a postgres database
        based on the table and sql logic provided to create 
        the dimension table and will check if table needs to 
        be truncated or not. If table is not trunacated correctly, 
        the task will fail"""
        
        conn_hook=PostgresHook(self.conn_id)
        rendered_sql=endered_sql=LoadDimensionOperator\
                     .insert_sql\
                     .format(target_table=self.target_table,
                             sql=self.sql)
        if self.truncate_table:
            self.log.info(f"Truncating table {self.target_table}")
            conn_hook.run(f"TRUNCATE {self.target_table}")
            num_rows = conn_hook.get_records(f"SELECT COUNT(*) \
                                          FROM {self.target_table}")
            
            if num_rows[0][0] == 0:
                self.log.info(f"Table {self.target_table} successfully \
                                truncated")
            else:
                raise ValueError(f"{self.target_table} is not empty")
        self.log.info(f"Loading data into {self.target_table}")
        conn_hook.run(rendered_sql)
        self.log.info(f"Data successfully loaded in dimension table \
                         {self.target_table}")
        
