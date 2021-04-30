from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    
    """
    Operator transforms data from Redshift Staging tables and loads data into Redshift Fact table
    
    :param table (str): The name of the dimension table
    :param table_columns (str): The names of columns in the table
    :param insert_table_sql (str): Transform logic to generate data for dimension tables
    :param redshift_conn_id (str): Connection ID used to connect to Redshift Cluster
    
    """
    
    ui_color = '#80BD9E'
    
    insert_table_command = """
        INSERT INTO {} ({}) {} ;
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Operator params (with defaults)
                 table="",
                 table_columns="",
                 insert_table_sql="",
                 redshift_conn_id="", 
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Mapping params
        self.table = table
        self.table_columns = table_columns
        self.insert_table_sql = insert_table_sql
        self.redshift_conn_id=redshift_conn_id

        
    def execute(self, context):
        self.log.info('Executing LoadFactOperator for table {}'.format(self.table))
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
       
        self.log.info('Loading data into fact table {}'.format(self.table))
        insert_query = LoadFactOperator.insert_table_command.format(
                            self.table,
                            self.table_columns,
                            self.insert_table_sql
                        )

        redshift.run(insert_query)
        
        self.log.info('Loaded data into fact table {}'.format(self.table))
