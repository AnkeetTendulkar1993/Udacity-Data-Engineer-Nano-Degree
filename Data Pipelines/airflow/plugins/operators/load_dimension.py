from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    
    """
    Operator transforms data from Redshift Staging tables and loads data into Redshift Dimension tables
    
    :param table (str): The name of the dimension table
    :param table_columns (str): The names of columns in the table
    :param truncate_table (bool): If True then truncate table before executing the insert command
    :param insert_table_sql (str): Transform logic to generate and load data into dimension tables
    :param redshift_conn_id (str): Connection ID used to connect to Redshift Cluster
    
    """
    
    ui_color = '#80BD9E'
    
    truncate_table_command = """
        TRUNCATE TABLE {};
    """
    
    insert_table_command = """
        INSERT INTO {} ({}) {} ;
    """

    @apply_defaults
    def __init__(self,
                 # Operators params (with defaults)
                 table="",
                 table_columns="",
                 truncate_table=False,
                 insert_table_sql="",
                 redshift_conn_id="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params
        self.table = table
        self.table_columns = table_columns
        self.truncate_table = truncate_table
        self.insert_table_sql = insert_table_sql
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        
        self.log.info('Executing LoadDimensionOperator for table {}'.format(self.table))
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_table:
            self.log.info('Truncating table {}'.format(self.table))
            truncate_query = LoadDimensionOperator.truncate_table_command.format(
                                self.table
                            )
            redshift.run(truncate_query)
            
        self.log.info('Loading data into dimension table {}'.format(self.table))
        insert_query = LoadDimensionOperator.insert_table_command.format(
                            self.table,
                            self.table_columns,
                            self.insert_table_sql
                        )

        redshift.run(insert_query)
        
        self.log.info('Loaded data into dimension table {}'.format(self.table))
        