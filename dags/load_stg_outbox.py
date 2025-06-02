import datetime
import logging
import json

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'student',
    'start_date': datetime.datetime(2023, 3, 1),
    'catchup': False,
}

dag = DAG(
    dag_id='load_to_stg_outbox',
    default_args=default_args,
    schedule_interval='0/15 * * * *',  
    description='Загружает ranks, users, outbox из БД источника в stg.*',
)

def load_ranks_func():
    src = PostgresHook(postgres_conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION')
    dest = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
    
    records = src.get_records("SELECT id, name, bonus_percent, min_payment_threshold FROM ranks;")
    logging.info(f"Получено {len(records)} строк из ranks")
    
    dest.run("TRUNCATE TABLE stg.bonussystem_ranks;")
    
    dest.insert_rows(
        table="stg.bonussystem_ranks",
        rows=records,
        target_fields=["id", "name", "bonus_percent", "min_payment_threshold"]
    )
    logging.info(f"Вставлено {len(records)} строк в stg.bonussystem_ranks")


t_load_ranks = PythonOperator(
    task_id='load_ranks',
    python_callable=load_ranks_func,
    dag=dag
)


def load_users_func():
    src = PostgresHook(postgres_conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION')
    dest = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
    
    records = src.get_records("SELECT id, order_user_id FROM users;")
    logging.info(f"Получено {len(records)} строк из users")
    
    dest.run("TRUNCATE TABLE stg.bonussystem_users;")
    
    dest.insert_rows(
        table="stg.bonussystem_users",
        rows=records,
        target_fields=["id", "order_user_id"]
    )
    logging.info(f"Вставлено {len(records)} строк в stg.bonussystem_users")


t_load_users = PythonOperator(
    task_id='load_users',
    python_callable=load_users_func,
    dag=dag
)


def load_outbox_events_func():
    src = PostgresHook(postgres_conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION')
    dest = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
    workflow_key = 'outbox_events_load'
    
    with dest.get_conn() as conn:  
        with conn.cursor() as cur:
            cur.execute("""
                SELECT workflow_settings
                  FROM stg.srv_wf_settings
                 WHERE workflow_key = %s
            """, (workflow_key,))
            row = cur.fetchone()
            
            if not row:
                last_loaded_id = 0
            else:
                settings_json = row[0]  
                last_loaded_id = settings_json.get('last_loaded_id', 0)
            
    logging.info(f"Текущий курсор для outbox: {last_loaded_id}")
    
    outbox_sql = """
        SELECT id, event_ts, event_type, event_value
          FROM outbox
         WHERE id > %s
         ORDER BY id
    """
    new_events = src.get_records(outbox_sql, parameters=(last_loaded_id,))
    logging.info(f"Найдено {len(new_events)} новых записей в outbox")
    
    if not new_events:
        return  
    
    with dest.get_conn() as conn:
        with conn.cursor() as cur:
            insert_stmt = """
                INSERT INTO stg.bonussystem_events (id, event_ts, event_type, event_value)
                VALUES (%s, %s, %s, %s)
            """
            max_id = last_loaded_id
            for row in new_events:
                (oid, event_ts, event_type, event_value) = row
                cur.execute(insert_stmt, (oid, event_ts, event_type, event_value))
                if oid > max_id:
                    max_id = oid
            
            new_settings = {
                "last_loaded_id": max_id
            }
            new_settings_json = json.dumps(new_settings)
            
            cur.execute("""
                INSERT INTO stg.srv_wf_settings(workflow_key, workflow_settings)
                VALUES (%s, %s)
                ON CONFLICT (workflow_key) DO UPDATE
                  SET workflow_settings = EXCLUDED.workflow_settings
            """, (workflow_key, new_settings_json))
            
        conn.commit()
    
    logging.info(f"Загружено {len(new_events)} записей, новый курсор = {max_id}")


t_load_events = PythonOperator(
    task_id='events_load',
    python_callable=load_outbox_events_func,
    dag=dag
)

t_load_ranks >> t_load_users >> t_load_events