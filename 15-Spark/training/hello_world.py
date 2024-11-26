from pyspark.sql import SparkSession

# Создаем SparkSession (точка входа в Spark)
spark = SparkSession.builder.appName("Lesson1") \
    .master("local[*]") \
    .getOrCreate()

# Проверка, что все работает
print(spark)

# 1. Создание DataFrame из списка
data = [
    ("Alice", 25, "Engineer"),
    ("Bob", 30, "Data Scientist"),
    ("Charlie", 28, "Analyst"),
    ("David", 35, "Engineer"),
    ("Eve", 22, "Analyst")
]
columns = ["Name", "Age", "Profession"]
df = spark.createDataFrame(data, columns)
df.show()

# 1. Фильтрация (filter)
engineers = df.filter(df["Profession"] == "Engineer")
engineers.show()

#2. Выборка столбцов (select)
names_and_ages = df.select("Name", "Age")
names_and_ages.show()

#3. Сортировка (orderBy)
sorted_by_age = df.orderBy("Age", ascending=False) # По убыванию
sorted_by_age.show()

#  Не забываем остановить SparkSession после завершения работы
spark.stop()