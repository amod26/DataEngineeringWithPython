from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Executes the load dimension operator
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id='postgres_default',
                 query='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.conn_id=conn_id
        self.query=query
        
    def execute(self, context):
        conn=PostgresHook(self.conn_id).get_conn()
        cur=conn.cursor()
        cur.execute(self.query)
        conn.commit()
        self.log.info('LoadDimensionOperator Ran Sucessfully')