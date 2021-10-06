import pandas as pd
from airflow.models.baseoperator import BaseOperator
from google.cloud import storage

from api import get_data


class ApiToGCSOperator(BaseOperator):
    def __init__(self, client, bucket_name: str, last_match_file: str, match_file: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.client = client
        self.bucket_name = bucket_name
        self.last_match_file = last_match_file
        self.match_file = match_file

    def execute(self, context):
        bucket = self.client.bucket(bucket_name=self.bucket_name)
        lastmatch_file = storage.Blob(bucket=bucket, name=self.last_match_file)
        if lastmatch_file.exists(client=self.client):
            last_match = int(lastmatch_file.download_as_text())
        else:
            last_match = 0
        print("last_match_id: " + str(last_match))

        list_matches = get_data.get_matches(last_match)
        list_df = list()
        for match in list_matches:
            try:
                list_df.append(get_data.get_match_info(match))
            except:
                continue
        df = pd.concat(list_df, ignore_index=True)

        bucket.blob(self.match_file).upload_from_string(df.to_csv(index=False), 'text/csv')
