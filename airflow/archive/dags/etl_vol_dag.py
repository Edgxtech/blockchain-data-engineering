from datetime import timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.decorators import task
import pandas as pd
import psycopg2
import psycopg2.extras as extras

pg_hook = PostgresHook(postgres_conn_id='bde_default')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 3
}

def upsert_values(conn, df, table, pri_keys):
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    prikeys = ','.join(pri_keys)
    query = "INSERT INTO %s(%s) VALUES %%s ON CONFLICT (%s) DO UPDATE SET value_adj = excluded.value_adj" % (table, cols, prikeys)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()

with DAG(
    'etl-vol',
    start_date=days_ago(2),
    default_args=default_args,
    max_active_tasks=3,
    max_active_runs=1,
    schedule_interval=timedelta(minutes=1),
    tags=['cardano','vol'],
    catchup=False
) as dag:

    extract_vols = SQLExecuteQueryOperator(
        task_id="extract_vols",
        conn_id="bde_default",
        sql="SELECT * FROM vol where value_adj != 0;",
        dag=dag
    )

    @task
    def transform_vols_by_block(**context):
        vols_data = context["ti"].xcom_pull(task_ids="extract_vols", key="return_value")
        vols_df = pd.DataFrame(vols_data, columns=['hash', 'height', 'slot', 'unit', 'value_adj'])
        by_block_df = (vols_df
                        .groupby(['unit', 'height'])['value_adj']
                        .sum()
                        .apply(lambda x: x / 1000000)
                        .reset_index())
        return by_block_df

    @task
    def load_vols_by_block(**context):
        pg_hook = PostgresHook(postgres_conn_id='bde_default', schema='bde')
        conn = pg_hook.get_conn()
        by_block_df = context['ti'].xcom_pull(task_ids='transform_vols_by_block', key='return_value')
        upsert_values(conn, by_block_df, 'vol_by_block', ['height', 'unit'])

    @task
    def transform_vols_all_time(**context):
        vols_data = context["ti"].xcom_pull(task_ids="transform_vols_by_block", key="return_value")
        vols_df = pd.DataFrame(vols_data, columns=['unit', 'height', 'value_adj'])
        totals_df = (vols_df
                      .groupby(['unit'])['value_adj']
                      .sum()
                      .reset_index()
                      .sort_values('value_adj', ascending=False))
        return totals_df

    @task
    def load_vols_all_time(**context):
        conn = pg_hook.get_conn()
        all_time_df = context['ti'].xcom_pull(task_ids='transform_vols_all_time', key='return_value')
        upsert_values(conn, all_time_df, 'vol_all_time', ['unit'])

    transform_vols_by_block_task = transform_vols_by_block()

    extract_vols >> transform_vols_by_block_task >> load_vols_by_block()

    transform_vols_by_block_task >> transform_vols_all_time() >> load_vols_all_time()