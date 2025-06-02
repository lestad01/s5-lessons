import logging
import pendulum
import json
from dateutil import parser
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',  
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['dds', 'example'],
    is_paused_upon_creation=False
)
def dds_load():
    """
    DAG, который переносит:
      1) stg.ordersystem_users -> dds.dm_users  (таск load_dm_users)
      2) stg.ordersystem_restaurants -> dds.dm_restaurants (таск load_dm_restaurants)
      3) stg.ordersystem_orders -> dds.dm_timestamps (таск load_dm_timestamps)
      4) stg.ordersystem_orders -> dds.dm_products (таск load_dm_products)
      5) stg.ordersystem_orders -> dds.dm_orders (таск load_dm_orders)
      6) stg.ordersystem_orders + stg.bonussystem_events -> dds.fct_product_sales (таск load_fct_product_sales)
    """

    @task()
    def load_dm_users():
        pg_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT object_value FROM stg.ordersystem_users")
                rows = cur.fetchall()

        if not rows:
            log.info("Нет данных в stg.ordersystem_users.")
            return

        data_to_upsert = []
        for (object_value,) in rows:
            doc = json.loads(object_value)
            doc_user_id = doc.get("_id")
            doc_user_name = doc.get("name")
            doc_user_login = doc.get("login")
            
            if not doc_user_id:
                continue  
            data_to_upsert.append((doc_user_id, doc_user_name, doc_user_login))

        if not data_to_upsert:
            log.info("Нечего загружать в dm_users.")
            return

        upsert_sql = """
            INSERT INTO dds.dm_users (user_id, user_name, user_login)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE
              SET user_name  = EXCLUDED.user_name,
                  user_login = EXCLUDED.user_login
        """
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(upsert_sql, data_to_upsert)
            conn.commit()

        log.info(f"Загрузили или обновили {len(data_to_upsert)} пользователей в dds.dm_users.")

    @task()
    def load_dm_restaurants():
        pg_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT object_value FROM stg.ordersystem_restaurants")
                rows = cur.fetchall()

        if not rows:
            log.info("Нет данных в stg.ordersystem_restaurants.")
            return

        data_to_upsert = []
        for (obj_val,) in rows:
            doc = json.loads(obj_val)
            r_id = doc.get("_id")
            r_name = doc.get("name")
            r_ts = doc.get("update_ts")

            if not r_id:
                continue

            r_ts_parsed = parser.parse(r_ts) if isinstance(r_ts, str) else r_ts
            data_to_upsert.append((r_id, r_name, r_ts_parsed))

        if not data_to_upsert:
            log.info("Нечего загружать в dm_restaurants.")
            return

        upsert_sql = """
            INSERT INTO dds.dm_restaurants (restaurant_id, restaurant_name, active_from, active_to)
            VALUES (%s, %s, %s, '2099-12-31')
            ON CONFLICT (restaurant_id) DO UPDATE
              SET restaurant_name = EXCLUDED.restaurant_name,
                  active_from     = EXCLUDED.active_from,
                  active_to       = EXCLUDED.active_to
        """
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(upsert_sql, data_to_upsert)
            conn.commit()

        log.info(f"Загрузили/обновили {len(data_to_upsert)} ресторанов в dds.dm_restaurants.")

    @task()
    def load_dm_timestamps():
        """
        Заполняем dds.dm_timestamps из stg.ordersystem_orders.
        Предполагаем в object_value JSON ключ "date" c форматом 'YYYY-MM-DD HH:MM:SS',
        и поле "status" = CLOSED или CANCELLED.
        """
        pg_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT object_value FROM stg.ordersystem_orders;")
                rows = cur.fetchall()

        if not rows:
            log.info("Нет данных в stg.ordersystem_orders.")
            return

        data_to_insert = []

        for (obj_val,) in rows:
            doc = json.loads(obj_val)

            if doc.get("final_status") not in ("CLOSED", "CANCELLED"):
                continue

            date_str = doc.get("date")
            if not date_str:
                continue

            dt_parsed = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')

            ts = dt_parsed
            year = ts.year
            month = ts.month
            day = ts.day
            date_only = ts.date()    
            time_only = ts.time()    

            data_to_insert.append((ts, year, month, day, date_only, time_only))

        if not data_to_insert:
            log.info("Все записи не содержат 'date' или нужный статус, ничего не загружаем.")
            return

        insert_sql = """
            INSERT INTO dds.dm_timestamps (ts, year, month, day, date, time)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (ts) DO NOTHING
        """

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(insert_sql, data_to_insert)
            conn.commit()

        log.info(f"Добавили {len(data_to_insert)} строк в dds.dm_timestamps.")

    @task()
    def load_dm_products():
        """
        Заполняем dds.dm_products из stg.ordersystem_orders:
        - restaurant_id из dds.dm_restaurants (сопоставляем doc["restaurant"]["id"])
        - товары из doc["order_items"] (массив)
        - product_id = item["id"]
        - product_name = item["name"]
        - product_price = item["price"]
        - active_from = doc["update_ts"]
        - active_to = '2099-12-31'
        - id (int): генерируем при вставке, если такая пара (product_id, restaurant_id) не найдена
        """
        pg_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT object_value FROM stg.ordersystem_orders")
                orders_rows = cur.fetchall()

                if not orders_rows:
                    log.info("Нет заказов.")
                    return

                new_count = 0
                upd_count = 0

                for (obj_val,) in orders_rows:
                    doc = json.loads(obj_val)
                    rest_src_id = doc.get("restaurant", {}).get("id")
                    if not rest_src_id:
                        continue

                    cur.execute("SELECT id FROM dds.dm_restaurants WHERE restaurant_id=%s", (rest_src_id,))
                    rest_row = cur.fetchone()
                    if not rest_row:
                        continue
                    dwh_rest_id = rest_row[0]

                    r_ts_str = doc.get("update_ts")
                    if not r_ts_str:
                        continue
                    active_from = datetime.strptime(r_ts_str, "%Y-%m-%d %H:%M:%S")

                    items = doc.get("order_items", [])
                    for item in items:
                        p_id = item.get("id")
                        p_name = item.get("name")
                        p_price = item.get("price", 0)
                        if not p_id:
                            continue

                        cur.execute(
                            "SELECT id FROM dds.dm_products WHERE product_id=%s AND restaurant_id=%s",
                            (p_id, dwh_rest_id)
                        )
                        prod_row = cur.fetchone()

                        if prod_row:
                            existing_id = prod_row[0]
                            cur.execute(
                                """
                                UPDATE dds.dm_products
                                SET product_name = %s,
                                    product_price = %s,
                                    active_from = %s,
                                    active_to = '2099-12-31'
                                WHERE id = %s
                                """,
                                (p_name, p_price, active_from, existing_id)
                            )
                            upd_count += 1
                        else:
                            cur.execute("SELECT COALESCE(MAX(id), 0) FROM dds.dm_products")
                            max_id = cur.fetchone()[0]
                            new_id = max_id + 1

                            cur.execute(
                                """
                                INSERT INTO dds.dm_products (
                                    id, restaurant_id, product_id, product_name, product_price, active_from, active_to
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, '2099-12-31')
                                """,
                                (new_id, dwh_rest_id, p_id, p_name, p_price, active_from)
                            )
                            new_count += 1

                log.info(f"Новых товаров вставлено: {new_count}, обновлено: {upd_count}")

    @task()
    def load_dm_orders():
        """
        Заполняем dds.dm_orders из stg.ordersystem_orders.
        Поля:
        - id (int)           (генерируем row-by-row)
        - order_key (varchar)     = doc["_id"] (например)
        - order_status (varchar)  = doc["final_status"] или doc["status"]
        - restaurant_id (int)     = PK из dds.dm_restaurants, ищем по doc["restaurant"]["id"]
        - timestamp_id (int)      = PK из dds.dm_timestamps, ищем по ts=doc["date"]
        - user_id (int)           = PK из dds.dm_users, ищем по doc["user"]["id"]
        """

        pg_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT object_value FROM stg.ordersystem_orders;")
                rows = cur.fetchall()
                if not rows:
                    print("Нет данных в stg.ordersystem_orders.")
                    return

                new_count = 0
                upd_count = 0

                for (obj_val,) in rows:
                    doc = json.loads(obj_val)

                    order_key = doc.get("_id")
                    if not order_key:
                        continue

                    order_status = doc.get("final_status") or doc.get("status") or "UNKNOWN"

                    user_src_id = doc.get("user", {}).get("id")
                    if not user_src_id:
                        continue
                    cur.execute("""
                        SELECT id 
                        FROM dds.dm_users
                        WHERE user_id=%s
                    """, (user_src_id,))
                    user_row = cur.fetchone()
                    if not user_row:
                        continue
                    user_dwh_id = user_row[0]

                    rest_src_id = doc.get("restaurant", {}).get("id")
                    if not rest_src_id:
                        continue
                    cur.execute("""
                        SELECT id
                        FROM dds.dm_restaurants
                        WHERE restaurant_id=%s
                    """, (rest_src_id,))
                    rest_row = cur.fetchone()
                    if not rest_row:
                        continue
                    rest_dwh_id = rest_row[0]

                    date_str = doc.get("date")
                    if not date_str:
                        continue
                    dt_parsed = parser.parse(date_str)
                    
                    cur.execute("""
                        SELECT id 
                        FROM dds.dm_timestamps
                        WHERE ts=%s
                    """, (dt_parsed,))
                    ts_row = cur.fetchone()
                    if not ts_row:
                        continue
                    ts_dwh_id = ts_row[0]

                    cur.execute("""
                        SELECT id
                        FROM dds.dm_orders
                        WHERE order_key=%s
                    """, (order_key,))
                    existing = cur.fetchone()
                    if existing:
                        existing_id = existing[0]
                        cur.execute("""
                            UPDATE dds.dm_orders
                            SET order_status=%s,
                                restaurant_id=%s,
                                timestamp_id=%s,
                                user_id=%s
                            WHERE id=%s
                        """, (order_status, rest_dwh_id, ts_dwh_id, user_dwh_id, existing_id))
                        upd_count += 1
                    else:
                        cur.execute("SELECT COALESCE(MAX(id), 0) FROM dds.dm_orders;")
                        max_id = cur.fetchone()[0]
                        new_id = max_id + 1

                        cur.execute("""
                            INSERT INTO dds.dm_orders (
                                id, order_key, order_status,
                                restaurant_id, timestamp_id, user_id
                            )
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (new_id, order_key, order_status, rest_dwh_id, ts_dwh_id, user_dwh_id))
                        new_count += 1

                print(f"Новых заказов вставлено: {new_count}, обновлено: {upd_count}")

    @task()
    def load_fct_product_sales():
        """
        Заполняем dds.fct_product_sales, беря:
        - doc из stg.ordersystem_orders => item["quantity"], item["price"]
        - bonus из stg.bonussystem_events => product_payments[].bonus_payment / bonus_grant
        - ссылки:
            * dds.dm_orders: order_id (по order_key=doc["_id"])
            * dds.dm_products: product_id (по (product_id=item["id"], restaurant_id))
        """
        pg_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT object_value
                    FROM stg.ordersystem_orders
                    """
                )
                orders_rows = cur.fetchall()
                if not orders_rows:
                    log.info("Нет данных в stg.ordersystem_orders за 9–10 апреля 2025.")
                    return

                bonus_dict = {}
                cur.execute("SELECT event_value FROM stg.bonussystem_events WHERE event_type = 'bonus_transaction'")
                events_rows = cur.fetchall()
                for (ev_val,) in events_rows:
                    ev_doc = json.loads(ev_val)
                    ev_order_id = ev_doc.get("order_id")
                    if not ev_order_id:
                        continue

                    product_payments = ev_doc.get("product_payments", [])
                    sub_dict = {}
                    for pp in product_payments:
                        pid = pp.get("product_id")
                        bpay = pp.get("bonus_payment", 0.0)
                        bgrn = pp.get("bonus_grant", 0.0)
                        sub_dict[pid] = (bpay, bgrn)
                    bonus_dict[ev_order_id] = sub_dict
                    log.debug(f"Bonus dict for order {ev_order_id}: {sub_dict}")

                new_count = 0
                upd_count = 0

                for (obj_val,) in orders_rows:
                    doc = json.loads(obj_val)
                    order_key = doc.get("_id")
                    if not order_key:
                        continue

                    cur.execute("SELECT id FROM dds.dm_orders WHERE order_key=%s", (order_key,))
                    order_row = cur.fetchone()
                    if not order_row:
                        log.debug(f"Order {order_key} not found in dm_orders.")
                        continue
                    dwh_order_id = order_row[0]

                    rest_src_id = doc.get("restaurant", {}).get("id")
                    if not rest_src_id:
                        continue
                    cur.execute("SELECT id FROM dds.dm_restaurants WHERE restaurant_id=%s", (rest_src_id,))
                    rest_row = cur.fetchone()
                    if not rest_row:
                        continue
                    dwh_rest_id = rest_row[0]

                    order_bonus_map = bonus_dict.get(order_key, {})
                    log.debug(f"Order {order_key}: bonus map = {order_bonus_map}")

                    items = doc.get("order_items", [])
                    for item in items:
                        product_src_id = item.get("id")
                        if not product_src_id:
                            continue
                        quantity = item.get("quantity", 0)
                        price = float(item.get("price", 0.0))
                        if quantity <= 0 or price <= 0:
                            continue
                        total_sum = price * quantity

                        bpay, bgrn = order_bonus_map.get(product_src_id, (0.0, 0.0))
                        log.debug(f"Product {product_src_id}: bonus_payment={bpay}, bonus_grant={bgrn}")

                        cur.execute(
                            "SELECT id FROM dds.dm_products WHERE product_id=%s AND restaurant_id=%s",
                            (product_src_id, dwh_rest_id)
                        )
                        prod_row = cur.fetchone()
                        if not prod_row:
                            log.debug(f"Product {product_src_id} for restaurant {dwh_rest_id} not found in dm_products.")
                            continue
                        dwh_product_id = prod_row[0]

                        cur.execute(
                            "SELECT id FROM dds.fct_product_sales WHERE order_id=%s AND product_id=%s",
                            (dwh_order_id, dwh_product_id)
                        )
                        existing = cur.fetchone()
                        if existing:
                            ex_id = existing[0]
                            cur.execute(
                                """
                                UPDATE dds.fct_product_sales
                                SET count=%s,
                                    price=%s,
                                    total_sum=%s,
                                    bonus_payment=%s,
                                    bonus_grant=%s
                                WHERE id=%s
                                """,
                                (quantity, price, total_sum, bpay, bgrn, ex_id)
                            )
                            upd_count += 1
                        else:
                            cur.execute("SELECT COALESCE(MAX(id), 0) FROM dds.fct_product_sales")
                            max_id = cur.fetchone()[0]
                            new_id = max_id + 1

                            cur.execute(
                                """
                                INSERT INTO dds.fct_product_sales (
                                    id, product_id, order_id, count, price,
                                    total_sum, bonus_payment, bonus_grant
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                """,
                                (new_id, dwh_product_id, dwh_order_id, quantity, price, total_sum, bpay, bgrn)
                            )
                            new_count += 1

                log.info(f"INSERTED={new_count}, UPDATED={upd_count}")

    load_users_task = load_dm_users()
    load_restaurants_task = load_dm_restaurants()
    load_timestamps_task = load_dm_timestamps()
    load_products_task = load_dm_products()
    load_orders_task = load_dm_orders()
    load_product_sales = load_fct_product_sales()

    load_users_task >> load_restaurants_task >> load_timestamps_task >> load_products_task >> load_orders_task >> load_product_sales

load_ddsload = dds_load()