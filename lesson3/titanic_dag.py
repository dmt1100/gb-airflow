from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from util.settings import default_settings
from util.helpers import make_directory
from util.tasks import (
    pivot_dataset,
    mean_fare_per_class,
    download_titanic_dataset
)

# В контексте DAG'а зададим набор task'ок
# Объект-инстанс Operator'а - это и есть task
with DAG(**default_settings()) as dag:
    # BashOperator, выполняющий указанную bash-команду
    first_task = BashOperator(
        task_id='first_task',
        bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"',
        dag=dag,
    )
    #
    create_titanic_folder = PythonOperator(
        task_id='make_directory',
        python_callable=make_directory,
        dag=dag,
    )
    # Загрузка датасета
    create_titanic_dataset = PythonOperator(
        task_id='download_titanic_dataset',
        python_callable=download_titanic_dataset,
        dag=dag,
    )
    # Чтение, преобразование и запись датасета
    pivot_titanic_dataset = PythonOperator(
        task_id='pivot_dataset',
        python_callable=pivot_dataset,
        dag=dag,
    )
    # Чтение и расчет средней арифметической цены билета
    mean_fares_titanic_dataset = PythonOperator(
        task_id='mean_fare_per_class',
        python_callable=mean_fare_per_class,
        dag=dag,
    )
    # Выводится строка с сообщением
    last_task = BashOperator(
        task_id='last_task',
        bash_command='echo "Pipeline finished! Execution date is {{ ds }}!"',
        dag=dag,
    )

    # Порядок выполнения тасок
    first_task >> create_titanic_folder >> create_titanic_dataset >> (pivot_titanic_dataset, mean_fares_titanic_dataset) >> last_task
