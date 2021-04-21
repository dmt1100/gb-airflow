from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
import pandas as pd


def download_titanic_dataset(**context):
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    return df.to_json()


def pivot_dataset(**context):
    ti = context['ti']
    res = ti.xcom_pull(task_ids='download_titanic_dataset')
    titanic_df = pd.read_json(res)

    df = titanic_df.pivot_table(index=['Sex'],
                                columns=['Pclass'],
                                values='Name',
                                aggfunc='count').reset_index()
    # создаем кастом хук, коннектшн берем из предварительно созданного в UI:
    pg_hook = BaseHook.get_hook('postgres_db')

    # имя таблицы в локальной БД предварительно задано в UI в Variables. Извлекаем:
    pg_table_name = Variable.get('pivot_table_name')

    # перемалываем датафрейм в список кортежей, приводим типы к стандартным (str и int):
    pg_rows = list(df.to_records(index=False))
    pg_rows_conv = [(t[0], int(t[1]), int(t[2]), int(t[3])) for t in pg_rows]

    # извлекаем названия полей(колонок) датафрейма и приводим их типы к строковому:
    pg_columns = list(df.columns)
    pg_columns_conv = [pg_columns[0],
                       '"' + str(pg_columns[1]) + '"',
                       '"' + str(pg_columns[2]) + '"',
                       '"' + str(pg_columns[3]) + '"']

    # отправляем данные в локальную БД:
    pg_hook.insert_rows(table=pg_table_name,
                        rows=pg_rows_conv,
                        target_fields=pg_columns_conv,
                        commit_every=0,
                        replace=False)


def mean_fare_per_class(**context):
    ti = context['ti']
    res = ti.xcom_pull(task_ids='download_titanic_dataset')
    titanic_df = pd.read_json(res)

    df = titanic_df.groupby(['Pclass']).agg({'Fare': 'mean'}).reset_index()
    # создаем кастом хук, коннектшн берем из предварительно созданного в UI:
    pg_hook = BaseHook.get_hook('postgres_db')

    # имя тааблицы в локальной БД предварительно задано в UI в Variables. Извлекаем:
    pg_table_name = Variable.get('mean_fares_table_name')

    # перемалываем датафрейм в список кортежей, приводим типы к стандартным (int и float):
    pg_rows = list(df.to_records(index=False))
    pg_rows_conv = [(int(t[0]), float(t[1])) for t in pg_rows]

    # извлекаем названия полей(колонок) датафрейма:
    pg_columns = list(df.columns)

    # отправляем данные в локальную БД:
    pg_hook.insert_rows(table=pg_table_name,
                        rows=pg_rows_conv,
                        target_fields=pg_columns,
                        commit_every=0,
                        replace=False)
