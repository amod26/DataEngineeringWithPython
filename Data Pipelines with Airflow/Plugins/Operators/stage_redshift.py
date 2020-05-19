from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Executes the stage to Redshift operator and checks if the operator has ran successfully
    """
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 s3_src_path='',
                 table='',
                 iam_role='',
                 json_type='',
                 region='',
                 extra_params='',                     
                 conn_id='postgres_default',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_src_path=s3_src_path
        self.conn_id=conn_id
        self.table=table
        self.iam_role=iam_role
        self.json_type=json_type
        self.region=region
        self.extra_params=extra_params
        
    def execute(self, context):
        copy_query=f"""
        copy {self.table}
        from {self.s3_src_path}
        iam_role {self.iam_role}
        format as JSON '{self.json_type}'
        region '{self.region}'
        {self.extra_params};
        """
        conn=PostgresHook(self.conn_id).get_conn()
        cur=conn.cursor()
        cur.execute(copy_query)
        conn.commit()
        self.log.info('StageToRedshiftOperator Ran Sucessfully')