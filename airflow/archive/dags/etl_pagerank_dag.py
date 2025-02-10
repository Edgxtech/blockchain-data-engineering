from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.decorators import task
import pandas as pd
import psycopg2
import psycopg2.extras as extras

pg_hook = PostgresHook(postgres_conn_id='bde_default')
neo4j_hook = Neo4jHook(conn_id='bde_graph_default')

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
    query = "INSERT INTO %s(%s) VALUES %%s ON CONFLICT (%s) DO UPDATE SET score = excluded.score" % (table, cols, prikeys)
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

def extract_pageranks():
    results = neo4j_hook.run("CALL gds.pageRank.stream('txGraph')\
         YIELD nodeId, score\
         RETURN gds.util.asNode(nodeId).address as address, score\
         ORDER BY score DESC")
    return pd.DataFrame(results)

with DAG(
    'etl-pagerank',
    start_date=days_ago(2),
    default_args=default_args,
    max_active_tasks=3,
    max_active_runs=1,
    schedule_interval=timedelta(minutes=1),
    tags=['cardano','pagerank'],
    catchup=False
) as dag:

    extract_pageranks_task=PythonOperator(
        task_id="extract_pageranks",
        python_callable=extract_pageranks
    )

    @task
    def load_pageranks(**context):
        conn = pg_hook.get_conn()
        pageranks_data = context['ti'].xcom_pull(task_ids='extract_pageranks', key='return_value')
        pageranks_df = pd.DataFrame(pageranks_data, columns=['address', 'score']) \
                            .drop_duplicates(keep='last', inplace=False)
        upsert_values(conn, pageranks_df, 'address_pagerank', ['address'])

extract_pageranks_task >> load_pageranks()