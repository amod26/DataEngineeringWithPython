from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import *
import logging
class DataQualityOperator(BaseOperator):
    """
   This quality will check if the tests have completed successfuly
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id='',
                 queries_with_exp_result={},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id=conn_id
        self.queries_with_exp_result=queries_with_exp_result
    
    def execute(self, context):
        conn=PostgresHook(self.conn_id).get_conn()
        cur=conn.cursor()
        for query,exp_result in self.queries_with_exp_result.items:
            result = cur.execute(query)
            if (exp_result==result[0]):
                print(f'Tested {query} and Sucessfully completed')
            else:
                raise AirflowRescheduleException
                self.log.info(f'DataQualityOperator not for {query} was not successfull')
        conn.commit()