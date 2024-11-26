from pyspark.sql import SparkSession

# Создаем SparkSession (точка входа в Spark)
spark = SparkSession.builder.appName("PySpark PostgreSQL Connection") \
    .master("local[*]") \
    .config("spark.jars", "postgresql-42.2.23.jar") \
    .getOrCreate()

# Проверка, что все работает
print(spark)

url = "jdbc:postgresql://localhost:6432/postgres"
properties = {
    "user": "admin",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

df = spark.read.jdbc(url=url, table="employees", properties=properties)

df.show()

df.createOrReplaceTempView("my_table_view")
spark.sql("SELECT * FROM my_table_view WHERE salary >= 65000").show()

data = [
    ("Alice", "Engineer", 75000, "2021-06-15"),
    ("Bob", "Manager", 90000, "2020-05-01"),
    ("Charlie", "HR", 60000, "2019-04-12"),
    ("Diana", "Sales", 50000, "2018-01-25")
]
columns = ["name", "position", "salary", "hire_date"]

df = spark.createDataFrame(data, columns)

filtered_df = df.filter(df.salary >= 60000)
filtered_df.show()

filtered_df.write.jdbc(
    url=url,
    table="high_salary_employees",
    mode="overwrite",  # "overwrite" - если таблица уже существует, она будет перезаписана
    properties=properties
)

print("Данные успешно записаны в таблицу high_salary_employees")

spark.stop()