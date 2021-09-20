import io

import pandas as pd
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryCreateExternalTableOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from operators import api_to_gcs, run_bq_query
from google.cloud import storage, bigquery
from api import get_data
from sql import queries
import os
import tenacity
from datetime import timedelta

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "dags/credentials/credentials.json"
bucket_name = "dota2-dwh"

lastmatch_file_path = "last_match.txt"
match_file = "matches.csv"
player_file = "player.csv"
team_file = "team.csv"
hero_file = "hero.csv"
item_file = "item.csv"

dataset_name = "dota2_dwh"
dataset_staging = "dota2_dwh_staging"

team_table = "d_teams"
player_table = "d_players"
record_table = "f_records"
hero_table = "d_heroes"
item_table = "d_items"

args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}
client = storage.Client()
bq_client = bigquery.Client()
dag = DAG(dag_id="read-db-dag", default_args=args,
          schedule_interval=timedelta(hours=6))


def get_dataframe_from_gcs(bucket: str, filename: str) -> pd.DataFrame:
    data = client.bucket(bucket_name=bucket).get_blob(filename).download_as_string()
    return pd.read_csv(io.BytesIO(data))


def save_dataframe_to_gcs(bucket, df: pd.DataFrame, filename: str):
    bucket = client.bucket(bucket_name=bucket)
    bucket.blob(filename).upload_from_string(df.to_csv(index=False, encoding='utf-8'), 'text/csv')


def gcs_to_bigquery_operator(task_id, bucket, source_object, dataset, table_name, schema_fields):
    return BigQueryCreateExternalTableOperator(
        task_id=task_id,
        bucket=bucket,
        source_objects=[source_object],
        destination_project_dataset_table=f'{dataset}.{table_name}',
        bigquery_conn_id="google_cloud_connection",
        google_cloud_storage_conn_id="google_cloud_connection",
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        schema_fields=schema_fields
    )


def get_new_players(**kwargs):
    records = get_dataframe_from_gcs(bucket_name, match_file)
    kwargs['ti'].xcom_push(key="last_match", value=int(records["match_id"][0]))
    players = list(records["player_id"].unique())
    print(players)
    player_info = get_data.get_players_by_ids(players)
    save_dataframe_to_gcs(bucket_name, player_info, player_file)


def get_new_teams():
    records = get_dataframe_from_gcs(bucket_name, match_file)
    teams = list(records["team_id"].unique())
    print(teams)
    teams_info = get_data.get_team_by_ids(teams)
    save_dataframe_to_gcs(bucket_name, teams_info, team_file)


def get_items():
    items = get_data.get_items()
    save_dataframe_to_gcs(bucket_name, items, item_file)


def get_heroes():
    heroes = get_data.get_heroes()
    save_dataframe_to_gcs(bucket_name, heroes, hero_file)


def get_new_last_match(**kwargs):
    def push_last_match_to_gcs():
        last_match = kwargs['ti'].xcom_pull(key='last_match')
        print(last_match)
        bucket = client.bucket(bucket_name=bucket_name)
        bucket.blob(lastmatch_file_path).upload_from_string(str(last_match))

    r = tenacity.Retrying()
    r.call(push_last_match_to_gcs)




with dag:
    write_match_to_gcs = api_to_gcs.ApiToGCSOperator(bucket_name=bucket_name,
                                                     match_file=match_file,
                                                     last_match_file=lastmatch_file_path,
                                                     client=client,
                                                     task_id="get_new_records")

    get_players = PythonOperator(task_id="get_new_players", python_callable=get_new_players)

    get_teams = PythonOperator(task_id="get_new_teams", python_callable=get_new_teams)

    get_all_items = PythonOperator(task_id="get_items", python_callable=get_items)

    get_all_heroes = PythonOperator(task_id="get_heroes", python_callable=get_heroes)

    create_staging_teams_table = gcs_to_bigquery_operator("create_staging_teams",
                                                          bucket_name,
                                                          team_file,
                                                          dataset_staging,
                                                          team_table,
                                                          schema_fields=[
                                                              {"name": "team_id", "type": "NUMERIC"},
                                                              {"name": "team_name", "type": "STRING"},
                                                          ])

    create_staging_players_table = gcs_to_bigquery_operator("create_staging_players",
                                                            bucket_name,
                                                            player_file,
                                                            dataset_staging,
                                                            player_table,
                                                            schema_fields=[
                                                                {"name": "player_id", "type": "NUMERIC"},
                                                                {"name": "player_name", "type": "STRING"},
                                                            ])

    create_staging_heroes_table = gcs_to_bigquery_operator("create_staging_heroes",
                                                           bucket_name,
                                                           hero_file,
                                                           dataset_staging,
                                                           hero_table,
                                                           schema_fields=[
                                                               {"name": "hero_id", "type": "NUMERIC"},
                                                               {"name": "hero_name", "type": "STRING"},
                                                               {"name": "primary_attr", "type": "STRING"},
                                                               {"name": "attack_type", "type": "STRING"},
                                                               {"name": "localized_name", "type": "STRING"},
                                                           ])
    create_staging_items_table = gcs_to_bigquery_operator("create_staging_items",
                                                          bucket_name,
                                                          item_file,
                                                          dataset_staging,
                                                          item_table,
                                                          schema_fields=[
                                                              {"name": "item_id", "type": "NUMERIC"},
                                                              {"name": "item_name", "type": "STRING"},
                                                              {"name": "localized_name", "type": "STRING"},
                                                          ])

    create_staging_records = gcs_to_bigquery_operator("create_staging_records",
                                                      bucket_name,
                                                      match_file,
                                                      dataset_staging,
                                                      record_table,
                                                      schema_fields=[
                                                          {"name": "match_id", "type": "NUMERIC"},
                                                          {"name": "player_id", "type": "NUMERIC"},
                                                          {"name": "team_id", "type": "NUMERIC"},
                                                          {"name": "won", "type": "NUMERIC"},
                                                          {"name": "kills", "type": "NUMERIC"},
                                                          {"name": "deaths", "type": "NUMERIC"},
                                                          {"name": "assists", "type": "NUMERIC"},
                                                          {"name": "item1", "type": "NUMERIC"},
                                                          {"name": "item2", "type": "NUMERIC"},
                                                          {"name": "item3", "type": "NUMERIC"},
                                                          {"name": "item4", "type": "NUMERIC"},
                                                          {"name": "item5", "type": "NUMERIC"},
                                                          {"name": "item6", "type": "NUMERIC"},
                                                          {"name": "hero_id", "type": "NUMERIC"},
                                                          {"name": "date", "type": "DATETIME"},
                                                      ])

    bash = BashOperator(task_id="dummy", bash_command="echo 1")

    load_to_dw = run_bq_query.RunBQOperator(task_id="load_new_data", client=bq_client, query=queries.update_queries)

    new_last_match = PythonOperator(task_id="get_new_last_match", python_callable=get_new_last_match)

    write_match_to_gcs >> [get_players, get_teams, get_all_heroes, get_all_items] >> bash >> [
        create_staging_teams_table,
        create_staging_heroes_table,
        create_staging_players_table,
        create_staging_items_table] >> create_staging_records >> load_to_dw >> new_last_match
