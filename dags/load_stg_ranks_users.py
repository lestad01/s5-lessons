from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import datetime
import logging

def load_ranks():
    try:
        logging.info("Начало загрузки таблицы ranks...")    
        src = PostgresHook(postgres_conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION')
        trg = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
        
        data_src = src.get_records("""
            select id, name, bonus_percent, min_payment_threshold
            from ranks
        """)
        
        trg.run("truncate table stg.bonussystem_ranks;")

        trg.insert_rows(
            table="stg.bonussystem_ranks",
            rows=data_src,      
            target_fields=["id", "name", "bonus_percent", "min_payment_threshold"]
        )

        count = trg.get_first("select COUNT(*) from stg.bonussystem_ranks;")[0]
        logging.info(f"Загружено {count} записей в bonussystem_ranks")
    except Exception as a:
        logging.error(f"Ошибка {str(a)}")
        raise a

def load_users():
    try:
        logging.info("Начало загрузки таблицы bonussystem_users...")      
        src = PostgresHook(postgres_conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION')
        trg = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
        
        data_src = src.get_records("""
            select id, order_user_id
            from public.users
        """)
        
        trg.run("truncate table stg.bonussystem_users")

        trg.insert_rows(
            table="stg.bonussystem_users",
            rows=data_src,      
            target_fields=["id", "order_user_id"]
        )
        count = trg.get_first(f"select COUNT(*) from stg.bonussystem_users;")[0]
        logging.info(f"Загружено {count} записей в stg.bonussystem_users")
    except Exception as e:
        logging.error(f"Ошибка {str(e)}")
        raise e


default_args = {
    'start_date': datetime.datetime(2025, 1, 1),
    'catchup': False,
    'retries': 1 # попытка перезапуска
}

dag = DAG(
    dag_id='load_stg_ranks_users_dag',
    schedule_interval='0/5 * * * *',  
    default_args=default_args
)

task_load_ranks = PythonOperator(
    task_id='load_ranks',
    python_callable=load_ranks,
    dag=dag
)
task_load_users = PythonOperator(
    task_id='load_users',
    python_callable=load_users,
    dag=dag
)

task_load_ranks >> task_load_users
##[task_load_ranks, task_load_users]

