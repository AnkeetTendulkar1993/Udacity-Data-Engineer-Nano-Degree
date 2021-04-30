from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    
    """
    Operator performs data quality checks based on the input queries provided
    
    :param validation_queries (list): List of validation queries
    :param expected_results (list): List of expected results for the validation queries
    :param redshift_conn_id (str): Connection ID used to connect to Redshift Cluster
    
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Operator params (with defaults)
                 validation_queries=[],
                 expected_results=[],
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params
        self.validation_queries = validation_queries
        self.expected_results = expected_results
        self.redshift_conn_id = redshift_conn_id


    def execute(self, context):
        self.log.info('Executing DataQualityOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if len(self.validation_queries) != len(self.expected_results):
            raise ValueError("The number of expected results provided don't match the number of validation queries provided")
        
        for index in range(0, len(self.validation_queries)):
            query = self.validation_queries[index]
            expected_result = self.expected_results[index]
            
            self.log.info("Executing Query {}".format(query))
            
            records = redshift.get_records(query)

            if len(records) < 1 or len(records[0]) < 1:
                self.log.info("No results returned for the given query")
                raise ValueError("No results returned for the given query")
            
            record_count = int(records[0][0])
            
            if record_count != expected_result:
                self.log.info("Validation Query failed. Record count {} did not match expected result count {}".format(record_count, expected_result))
                raise ValueError("Validation Query failed. Record count {} did not match expected result count {}".format(record_count, expected_result))
            
        
        self.log.info('Data Validation Completed Successfully')

                
                