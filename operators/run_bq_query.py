from airflow.models import BaseOperator
from google.cloud.bigquery import Client


class RunBQOperator(BaseOperator):

    def __init__(self, client: Client, query: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.client = client
        self.query = query

    def execute(self, context):
        self.client.query(self.query).result()
