from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from controllers import *


# Definindo a frequência de execução do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

dag = DAG(
    'check_and_update_orders', # Nome do DAG
    default_args= default_args,
    description = 'Checa e atualiza os pedidos do extrator no banco de dados',
    schedule_interval = timedelta(minutes=1)
)

# Instanciando as classes
app = FinalReport()

# Definindo as tasks
task_1_check_and_update_orders = PythonOperator(
    task_id = '1_check_and_update_orders',
    python_callable = app.check_and_update_orders,
    dag = dag
)
task_2_rename_format_files = PythonOperator(
    task_id = '2_rename_format_files',
    python_callable = app.rename_format_files,
    dag = dag
)

task_3_move_file_to_client_folder = PythonOperator(
    task_id = '3_move_file_to_client_folder',
    python_callable = app.move_file_to_client_folder,
    dag = dag
)

# Definindo a ordem de execução das tasks
task_1_check_and_update_orders >> task_2_rename_format_files >> task_3_move_file_to_client_folder



