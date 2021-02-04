from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    insert_sql="""INSERT INTO {target_table}
                  ({sql})"""
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 sql="",
                 target_table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id=conn_id
        self.sql=sql
        self.target_table=target_table

    def execute(self, context):
        """Loads facts tables into postgres database based on
        the table and sql logic provided to create the fact
        table"""
        
        conn_hook=PostgresHook(self.conn_id)
        rendered_sql=LoadFactOperator.insert_sql.format(target_table=self.target_table,\
                                                        sql=self.sql)
        conn_hook.run(rendered_sql)
        self.log.info(f"Data successfully loaded in fact table \
                     {self.target_table}")
