from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    
    """
    Operator to Copy Stage data from S3 to Redshift Staging tables
    
    :param table (str): The name of the table
    :param s3_bucket (str): The name of the s3 bucket having staging data
    :param s3_key (str): The name of the s3 key having staging data
    :param aws_credentials_id (str): Credentials to connect to AWS
    :param redshift_conn_id (str): Connection ID used to connect to Redshift Cluster
    :param json_log_path (str): Log path for input json files
    
    """
    
    ui_color = '#358140'
    
    template_fields = ["s3_key"]
    
    copy_command = """
        COPY {} 
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}';
    """

    @apply_defaults
    def __init__(self,
                 # Define operators params (with defaults)
                 table="",
                 s3_bucket="",
                 s3_key="",
                 aws_credentials_id="",
                 redshift_conn_id="", 
                 json_log_path="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.json_log_path = json_log_path

    def execute(self, context):
        
        self.log.info('Executing StageToRedshiftOperator')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        
        copy_command_sql = StageToRedshiftOperator.copy_command.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_log_path
        )
        
        self.log.info('Executing Copy command to load data from S3 to Redshift Stage')
        redshift.run(copy_command_sql)
        self.log.info('Copy command executed succesfully')
        