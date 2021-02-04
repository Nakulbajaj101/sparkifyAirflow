from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    template_fields=("s3_key",)
    ui_color = '#358140'
    stage_sql = """COPY {} FROM '{}' 
                      CREDENTIALS 'aws_iam_role={}' 
                      region '{}'
                      {}
                """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_name="",
                 s3_bucket="",
                 s3_key="",
                 s3_region="",
                 redshift_iam_arn="",
                 delimiter=",",
                 ignore_header=1,
                 file_type="json",
                 json_paths="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id=redshift_conn_id
        self.table_name=table_name
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.s3_region=s3_region
        self.redshift_iam_arn=redshift_iam_arn
        self.delimiter=delimiter
        self.ignore_header=ignore_header
        self.file_type=file_type
        self.json_paths=json_paths
        
        
    def get_load_file_copy_config(self):
        """
        Creates json and csv s3 to redshift
        basic configs which are used to specify
        incoming data file configuration setup
        """
        
        if self.file_type=="json":
            if self.json_paths=="":
                return "json 'auto ignorecase'"
            else:
                return f"json 's3://{self.s3_bucket}/{self.json_paths}'"
            
        elif self.file_type=="csv":
             return f"csv DELIMITER '{self.delimiter}' \
                      IGNOREHEADER {self.ignore_header}"
                
    def execute(self, context):
        """
        The overwritten execute function to load data 
        from s3 to redshift
        """
        
        redshift_hook=PostgresHook(self.redshift_conn_id)
        rendered_key=self.s3_key.format(**context)
        s3_path=f"s3://{self.s3_bucket}/{rendered_key}"
        rendered_stage_sql=StageToRedshiftOperator.stage_sql\
                           .format(self.table_name,
                                   s3_path,
                                   self.redshift_iam_arn,
                                   self.s3_region,
                                   self.get_load_file_copy_config())
        redshift_hook.run(rendered_stage_sql)
        self.log.info(f"Successfully loaded data into {self.table_name}\
                        from {s3_path}")





