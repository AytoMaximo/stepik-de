from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, when, mean, median, round, count, first, floor
from pyspark.sql.types import FloatType, IntegerType
from pyspark.sql.window import Window

import requests
import matplotlib.pyplot as plt
from datetime import datetime
import os
import clickhouse_connect
from clickhouse_driver import Client

# Параметры DAG
default_args = {
    'owner': 'airflow',
    'retries': 0
}

dag = DAG(
    'russian_houses_analysis',
    default_args=default_args,
    description='Анализ данных о российских домах с использованием PySpark',
    schedule_interval=None,
    start_date=datetime(2025, 1, 26),
    catchup=False,
)

# Функция для записи DataFrame в Parquet
def write_parquet(df, path):
    df.write.mode("overwrite").parquet(path)
    print(f"DataFrame сохранён в: {path}")


# Функция для чтения Parquet
def read_parquet(path):
    spark = SparkSession.builder \
        .appName("Load CSV") \
        .master("local") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
        .getOrCreate()
    
    if not os.path.exists(path):
        raise FileNotFoundError(f"Файл Parquet не найден: {path}")
    
    df = spark.read.parquet(path)
    print(f"DataFrame загружен из: {path}")
    return df


# Функция для скачивания файла
def download_csv():
    url = "https://aytomaximo.ru/share/russian_houses.csv"
    local_path = "/tmp/russian_houses.csv"
    response = requests.get(url)
    with open(local_path, 'wb') as file:
        file.write(response.content)
    print(f"Файл скачан в: {local_path}")


# Функция для загрузки данных в PySpark и подсчета строк
def load_csv_to_spark():
    spark = SparkSession.builder \
        .appName("Load CSV") \
        .master("local") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
        .getOrCreate()
    
    file_path = "/tmp/russian_houses.csv"
    if not os.path.exists(file_path):
        raise FileNotFoundError("Файл CSV не найден по указанному пути!")
    
    df = spark.read.format("csv") \
         .option("header", "true") \
         .option("inferSchema", "true") \
         .option("encoding", "UTF-16") \
         .option('multiline', 'true') \
         .load(file_path)
    
    print(f"Количество строк в DataFrame: {df.count()}")
    df.show(5)
    
    # Сохранение DataFrame в Parquet
    write_parquet(df, "/tmp/russian_houses.parquet")


# Функция для проверки корректности данных
def check_data_quality(**kwargs):

    # Загружаем DataFrame через общую функцию
    df = read_parquet("/tmp/russian_houses.parquet")
    
    # Проверка схемы данных
    df.printSchema()
    
    # Проверка на пустые строки
    empty_rows = df.filter(col(df.columns[0]).isNull())
    print(f"Количество пустых строк: {empty_rows.count()}")
    
    # Проверка на дубликаты
    duplicates = df.groupBy(df.columns).count().filter("count > 1")
    print(f"Количество дубликатов: {duplicates.count()}")


# Функция для преобразования типов данных
def transform_data_types(**kwargs):

    # Загружаем DataFrame через общую функцию
    df = read_parquet("/tmp/russian_houses.parquet")
    
    # Преобразование maintenance_year в integer
    df = df.withColumn("maintenance_year", when(col("maintenance_year").isNull(), None)
                                          .otherwise(col("maintenance_year").cast(IntegerType())))
    
    # Преобразование square в double
    df = df.withColumn("square", regexp_replace(col("square"), "—", "NULL"))
    df = df.withColumn("square", regexp_replace(col("square"), " ", ""))
    df = df.withColumn("square", when(col("square") == "NULL", None)
                                   .otherwise(col("square").cast(FloatType())))
    
    # Преобразование population в integer
    df = df.withColumn("population", regexp_replace(col("population"), "—", "NULL"))
    df = df.withColumn("population", when(col("population") == "NULL", None)
                                   .otherwise(col("population").cast(IntegerType())))
    
    # Преобразование communal_service_id в integer
    df = df.withColumn("communal_service_id", when(col("communal_service_id") == "", None)
                                          .otherwise(col("communal_service_id").cast(IntegerType())))
    
    df.show(5)
    df.printSchema()

    # Сохранение преобразованного DataFrame в Parquet
    write_parquet(df, "/tmp/russian_houses_transformed.parquet")


# Функция для вычисления среднего и медианного года постройки
def calculate_mean_median_year(**kwargs):

    # Загружаем DataFrame через общую функцию
    df = read_parquet("/tmp/russian_houses_transformed.parquet")
    
    # Для дальнейшей аналитики нереалистичные года постройки домов сильно мешают
    # Погуглив, пришла к выводу, что самое старое здание вряд ли будет старше XI века
    # В ClickHouse загружу первоначальный вариант без вмешательств
    current_year = datetime.now().year
    df = df.filter((col("maintenance_year") < current_year) & (col("maintenance_year") > 1000))
    
    mean_maintenance_year = df.select(round(mean("maintenance_year"), 0)).collect()[0][0]
    median_maintenance_year = df.select(round(median("maintenance_year"), 0)).collect()[0][0]
    
    print(f"Средний год постройки: {mean_maintenance_year}")
    print(f"Медианный год постройки: {median_maintenance_year}")

    # Сохранение преобразованного DataFrame в Parquet
    write_parquet(df, "/tmp/russian_houses_transformed_fixed.parquet")


# Функция для определения топ-10 областей и городов
def top_regions_and_cities(**kwargs):
    
    # Загружаем DataFrame через общую функцию
    df = read_parquet("/tmp/russian_houses_transformed_fixed.parquet")
    
    # Топ-10 областей
    top_regions = df.groupBy("region").count().orderBy("count", ascending=False)
    top_regions.show(10)

    # Преобразуем PySpark DataFrame в pandas DataFrame
    top_regions_pd = top_regions.limit(10).toPandas()

    # Данные для графика
    regions = top_regions_pd['region']
    counts = top_regions_pd['count']

    # Построение линейчатого графика
    plt.figure(figsize=(10, 6))
    plt.barh(regions, counts, color='skyblue')  # Горизонтальные линии

    # Добавляем подписи и заголовок
    plt.xlabel('Количество объектов')
    plt.ylabel('Регион')
    plt.title('Топ-10 регионов с наибольшим количеством объектов')

    # Инвертируем ось Y, чтобы регионы с наибольшим количеством были сверху
    plt.gca().invert_yaxis()

    # Показываем график
    plt.tight_layout()
    plt.show()
    
    # Топ-10 городов
    top_locations = df.groupBy("locality_name").count().orderBy("count", ascending=False)
    top_locations.show(10)

    # Заменяем NULL значения в locality_name на "Неизвестно"
    top_locations_cleaned = top_locations.withColumn(
        "locality_name", when(col("locality_name").isNull(), "Неизвестно").otherwise(col("locality_name"))
    )

    # Преобразуем PySpark DataFrame в pandas DataFrame
    top_locations_pd = top_locations_cleaned.limit(10).toPandas()

    # Данные для графика
    localities = top_locations_pd['locality_name']
    counts = top_locations_pd['count']

    # Построение линейчатого графика
    plt.figure(figsize=(10, 6))
    plt.barh(localities, counts, color='skyblue')  # Горизонтальные линии

    # Добавляем подписи и заголовок
    plt.xlabel('Количество объектов')
    plt.ylabel('Локация')
    plt.title('Топ-10 локаций с наибольшим количеством объектов')

    # Инвертируем ось Y, чтобы локации с наибольшим количеством были сверху
    plt.gca().invert_yaxis()

    # Показываем график
    plt.tight_layout()
    plt.show()


# Функция для поиска зданий с максимальной и минимальной площадью
def find_min_max_square(**kwargs):

    # Загружаем DataFrame через общую функцию
    df = read_parquet("/tmp/russian_houses_transformed_fixed.parquet")
    
    filtered_df = df.filter(col("square").isNotNull())
    
    window_spec_min = Window.partitionBy("region").orderBy("square")
    window_spec_max = Window.partitionBy("region").orderBy(col("square").desc())
    
    min_df = (
        filtered_df
        .withColumn("min_address", first("address").over(window_spec_min))
        .withColumn("min_square", first("square").over(window_spec_min))
        .groupBy("region")
        .agg(
            first("min_address").alias("min_address"),
            first("min_square").alias("min_square")
        )
    )
    
    max_df = (
        filtered_df
        .withColumn("max_address", first("address").over(window_spec_max))
        .withColumn("max_square", first("square").over(window_spec_max))
        .groupBy("region")
        .agg(
            first("max_address").alias("max_address"),
            first("max_square").alias("max_square")
        )
    )
    
    result_df = min_df.join(max_df, on="region")
    result_df.show()

    # Преобразуем PySpark DataFrame в pandas DataFrame
    df_pd = result_df.toPandas()

    # Добавляем колонку с разницей между max_square и min_square
    df_pd['square_difference'] = df_pd['max_square'] - df_pd['min_square']

    # Сортируем по разнице и берём топ-20
    top_regions = df_pd.sort_values(by='square_difference', ascending=False).head(20)

    # Данные для графика
    regions = top_regions['region']
    differences = top_regions['square_difference']

    # Построение графика
    plt.figure(figsize=(10, 6))
    plt.barh(regions, differences, color='skyblue')

    # Настройка графика
    plt.xlabel('Разница между max_square и min_square')
    plt.ylabel('Регион')
    plt.title('Топ-20 регионов по разнице площадей')
    plt.gca().invert_yaxis()
    plt.tight_layout()

    # Показываем график
    plt.show()


# Функция для подсчета зданий по десятилетиям
def count_buildings_by_decade(**kwargs):

    # Загружаем DataFrame через общую функцию
    df = read_parquet("/tmp/russian_houses_transformed_fixed.parquet")
    
    filtered_df = df.filter(col("maintenance_year").isNotNull())
    
    df_with_decade = filtered_df.withColumn(
        "decade",
        (floor(col("maintenance_year") / 10) * 10).cast("int")
    )
    
    buildings_by_decade = (
        df_with_decade.groupBy("decade")
        .agg(count("*").alias("building_count"))
        .orderBy("decade", ascending=False)
    )
    
    buildings_by_decade.show()

    # Преобразуем PySpark DataFrame в pandas DataFrame
    decades_pd = buildings_by_decade.toPandas()

    # Фильтруем данные: оставляем только десятилетия >= 1900
    filtered_decades = decades_pd[decades_pd['decade'] >= 1900]

    # Сортируем данные по десятилетиям
    filtered_decades = filtered_decades.sort_values(by='decade')

    # Построение графика
    plt.figure(figsize=(10, 6))
    plt.bar(filtered_decades['decade'], filtered_decades['building_count'], color='skyblue', width=8)

    # Настройка графика
    plt.xlabel('Десятилетие', fontsize=12)
    plt.ylabel('Количество зданий', fontsize=12)
    plt.title('Количество зданий по десятилетиям (с 1900 года)', fontsize=14)
    plt.xticks(filtered_decades['decade'], rotation=45)  # Поворачиваем подписи
    plt.grid(axis='y', linestyle='--', alpha=0.7)

    # Показываем график
    plt.tight_layout()
    plt.show()


def create_table_if_not_exists():

    # Получение данных подключения из Airflow Connections
    connection = BaseHook.get_connection('clickhouse_default')

    # Подключение к ClickHouse
    client = clickhouse_connect.get_client(
        host=connection.host,
        port=connection.port,
        database=connection.schema  
    )

    # SQL-запрос для создания таблицы
    create_table_query = """
    CREATE TABLE IF NOT EXISTS houses (
        house_id UInt32,
        latitude Nullable(Float64),
        longitude Nullable(Float64),
        maintenance_year Nullable(Int32),
        square Nullable(Float32),
        population Nullable(UInt32),
        region Nullable(String),
        locality_name Nullable(String),
        address Nullable(String),
        full_address Nullable(String),
        communal_service_id Nullable(UInt32),
        description Nullable(String)
    ) 
    ENGINE = MergeTree
    ORDER BY house_id;
    """

    # Выполнение запроса
    client.command(create_table_query)


def load_data_to_clickhouse():

    # Путь к файлу Parquet
    file_path = '/tmp/russian_houses_transformed.parquet'

    # Чтение данных из Parquet с помощью Spark
    spark_df = read_parquet(file_path)
    spark_df.printSchema()

    # Подключение к ClickHouse через Airflow Connections
    connection = BaseHook.get_connection('clickhouse_TCP')
    client = Client(
        host=connection.host,
        port=connection.port,
        database=connection.schema
    )

    # Получение существующих house_id из ClickHouse
    existing_ids = client.execute("SELECT house_id FROM houses")
    existing_ids_set = {row[0] for row in existing_ids}

    # Фильтрация уникальных записей
    unique_df = spark_df.filter(~col("house_id").isin(existing_ids_set))

    # Проверка на наличие данных для загрузки
    if unique_df.count() > 0:
        # Преобразование в список строк
        rows_to_insert = [tuple(row) for row in unique_df.collect()]
        columns = ", ".join(unique_df.columns)

        # Загрузка данных в ClickHouse
        insert_query = f"INSERT INTO houses ({columns}) VALUES"
        client.execute(insert_query, rows_to_insert, types_check=True)
        print(f"Загружено {len(rows_to_insert)} записей в ClickHouse.")
    else:
        print("Нет новых записей для загрузки.")


def fetch_top_houses():

    # Подключение к ClickHouse через Airflow Connections
    connection = BaseHook.get_connection('clickhouse_default')
    client = clickhouse_connect.get_client(
        host=connection.host,
        port=connection.port,
        database=connection.schema
    )

    # SQL-запрос для получения топ-25 домов с площадью > 60 кв.м
    query = """
    SELECT house_id, latitude, longitude, square, population, region, locality_name
    FROM houses
    WHERE square > 60
    ORDER BY square DESC
    LIMIT 25
    """

    # Выполнение запроса
    result = client.query(query)

    # Преобразование результата в список словарей
    rows = result.result_rows
    columns = result.column_names
    data = [dict(zip(columns, row)) for row in rows]

    # Вывод результатов
    for row in data:
        print(row)


# Операторы
start = EmptyOperator(
    task_id='start',
    dag=dag
)

set_connections = BashOperator(
    task_id='set_connections',
    bash_command='airflow connections import /opt/airflow/dags/connections.json',
    dag=dag
)

download_task = PythonOperator(
    task_id='download_csv',
    python_callable=download_csv,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_csv_to_spark',
    python_callable=load_csv_to_spark,
    dag=dag
)

check_data_task = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    provide_context=True,
    dag=dag
)

transform_data_task = PythonOperator(
    task_id='transform_data_types',
    python_callable=transform_data_types,
    provide_context=True,
    dag=dag
)

mean_median_task = PythonOperator(
    task_id='calculate_mean_median_year',
    python_callable=calculate_mean_median_year,
    provide_context=True,
    dag=dag
)

top_regions_task = PythonOperator(
    task_id='top_regions_and_cities',
    python_callable=top_regions_and_cities,
    provide_context=True,
    dag=dag
)

min_max_square_task = PythonOperator(
    task_id='find_min_max_square',
    python_callable=find_min_max_square,
    provide_context=True,
    dag=dag
)

count_decade_task = PythonOperator(
    task_id='count_buildings_by_decade',
    python_callable=count_buildings_by_decade,
    provide_context=True,
    dag=dag
)

create_table_task = PythonOperator(
    task_id='create_clickhouse_table',
    python_callable=create_table_if_not_exists,
    dag=dag
)

load_data_task = PythonOperator(
    task_id='load_data_to_clickhouse_with_spark',
    python_callable=load_data_to_clickhouse,
    dag=dag
)

top_square_houses_task = PythonOperator(
    task_id='get_top_square_houses_from_clickhouse',
    python_callable=fetch_top_houses,
    dag=dag
)

end = EmptyOperator(
    task_id='end',
    dag=dag
)

# Задачи
start >> set_connections >> download_task >> load_task >> check_data_task >> transform_data_task >> mean_median_task >> [top_regions_task, min_max_square_task, count_decade_task] >> create_table_task >> load_data_task >> top_square_houses_task >> end