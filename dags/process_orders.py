import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.operators.python import PythonOperator
from airflow.sensors.bash import BashSensor
from airflow.operators.dummy_operator import DummyOperator

from shared import normalize_csv, load_csv_to_postgres
import process_orders_sqls as sqls

default_args = {"owner": "airflow"}
connection_id = 'dwh'
default_end_time = '2999-12-31 23:59:59'

with DAG(
        dag_id="process_orders",
        start_date=datetime.datetime(2020, 1, 1),
        schedule_interval="@once",
        default_args=default_args,
        catchup=False,
) as dag:
    dwd_start = DummyOperator(task_id='dwd_start')

    check_stg_products_csv_readiness = BashSensor(
        task_id="check_stg_products_csv_readiness",
        bash_command="""
            ls /data/raw/products_{{ ds }}.csv
        """,
    )
    create_stg_products_table = PostgresOperator(
        task_id="create_stg_products_table",
        postgres_conn_id=connection_id,
        sql=sqls.create_stg_products_sql,
    )

    normalize_products_csv = PythonOperator(
        task_id='normalize_products_csv',
        python_callable=normalize_csv,
        op_kwargs={
            'source': "/data/raw/products_{{ ds }}.csv",
            'target': "/data/stg/products_{{ ds }}.csv"
        }
    )

    load_products_to_stg_products_table = PythonOperator(
        task_id='load_products_to_stg_products_table',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_filepath': "/data/stg/products_{{ ds }}.csv",
            'table_name': 'stg_products',
            'connection_id': connection_id
        },
    )

    dwd_start >> check_stg_products_csv_readiness >> normalize_products_csv >> create_stg_products_table >> load_products_to_stg_products_table

    create_dim_products_table = PostgresOperator(
        task_id="create_dim_products_table",
        postgres_conn_id=connection_id,
        sql=sqls.create_dim_products_sql,
    )

    transform_dim_products_table = PostgresOperator(
        task_id="load_dim_products_table",
        postgres_conn_id=connection_id,
        sql=sqls.transform_dim_products_sql,
    )

    load_products_to_stg_products_table >> create_dim_products_table >> transform_dim_products_table

    # orders
    check_stg_orders_csv_readiness = BashSensor(
        task_id="check_stg_orders_csv_readiness",
        bash_command="""
            ls /data/raw/orders_{{ ds }}.csv
        """,
    )

    normalize_orders_csv = PythonOperator(
        task_id='normalize_orders_csv',
        python_callable=normalize_csv,
        op_kwargs={
            'source': "/data/raw/orders_{{ ds }}.csv",
            'target': "/data/stg/orders_{{ ds }}.csv"
        }
    )

    create_stg_orders_table = PostgresOperator(
        task_id="create_stg_orders_table",
        postgres_conn_id=connection_id,
        sql=sqls.create_stg_orders_sql,
    )

    load_orders_to_stg_orders_table = PythonOperator(
        task_id='load_orders_to_stg_orders_table',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_filepath': "/data/stg/orders_{{ ds }}.csv",
            'table_name': 'stg_orders',
            'connection_id': connection_id
        },
    )

    create_dim_orders_table = PostgresOperator(
        task_id="create_dim_orders_table",
        postgres_conn_id=connection_id,
        sql=sqls.create_dim_orders_sql,
    )

    create_fact_orders_created_table = PostgresOperator(
        task_id="create_fact_orders_created_table",
        postgres_conn_id=connection_id,
        sql=sqls.create_orders_sql,
    )

    dwd_start >> check_stg_orders_csv_readiness >> normalize_orders_csv >> create_stg_orders_table >> load_orders_to_stg_orders_table >> [
        create_dim_orders_table, create_fact_orders_created_table]

    transform_dim_orders_table = PostgresOperator(
        task_id="transform_dim_orders_table",
        postgres_conn_id=connection_id,
        sql=sqls.transform_dim_orders_sql,
    )

    create_dim_orders_table >> transform_dim_orders_table

    transform_fact_orders_created_table = PostgresOperator(
        task_id="transform_fact_orders_created_table",
        postgres_conn_id=connection_id,
        sql=sqls.transform_fact_orders_created_sql,
    )

    create_fact_orders_created_table >> transform_fact_orders_created_table

    dwd_end = DummyOperator(task_id='dwd_finished')

    transform_dim_products_table >> dwd_end
    transform_dim_orders_table >> dwd_end
    transform_fact_orders_created_table >> dwd_end

    check_stg_inventory_csv_readiness = BashSensor(
        task_id="check_stg_inventory_csv_readiness",
        bash_command="""
            ls /data/raw/inventory_{{ ds }}.csv
        """,
    )
    create_stg_inventory_table = PostgresOperator(
        task_id="create_stg_inventory_table",
        postgres_conn_id=connection_id,
        sql=sqls.create_stg_inventory_table,
    )

    normalize_inventory_csv = PythonOperator(
        task_id='normalize_inventory_csv',
        python_callable=normalize_csv,
        op_kwargs={
            'source': "/data/raw/inventory_{{ ds }}.csv",
            'target': "/data/stg/inventory_{{ ds }}.csv"
        }
    )

    load_inventory_to_stg_inventory_table = PythonOperator(
        task_id='load_inventory_to_stg_inventory_table',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_filepath': "/data/stg/inventory_{{ ds }}.csv",
            'table_name': 'stg_inventory',
            'connection_id': connection_id
        },
    )

    create_dim_inventory_table = PostgresOperator(
        task_id="create_dim_inventory_table",
        postgres_conn_id=connection_id,
        sql=sqls.create_dim_inventory_table,
    )

    transform_dim_inventory_table = PostgresOperator(
        task_id="transform_dim_inventory_table",
        postgres_conn_id=connection_id,
        sql=sqls.transform_dim_inventory_table,
    )

    dwd_start >> check_stg_inventory_csv_readiness >> create_stg_inventory_table >> normalize_inventory_csv >> \
    load_inventory_to_stg_inventory_table >> create_dim_inventory_table >> transform_dim_inventory_table >> dwd_end
