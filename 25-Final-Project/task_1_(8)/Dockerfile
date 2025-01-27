FROM apache/airflow:2.9.2

USER root

# Установка wget
RUN apt-get update && apt-get install -y wget && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y file

# Установка OpenJDK 23
RUN wget https://download.oracle.com/java/23/latest/jdk-23_linux-x64_bin.tar.gz && \
    mkdir -p /opt/java && \
    tar -xvf jdk-23_linux-x64_bin.tar.gz -C /opt/java && \
    rm jdk-23_linux-x64_bin.tar.gz

# Установка Apache Spark 3.5.4
RUN wget https://downloads.apache.org/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz && \
    mkdir -p /opt/spark && \
    tar -xvf spark-3.5.4-bin-hadoop3.tgz -C /opt/spark && \
    rm spark-3.5.4-bin-hadoop3.tgz

# Настройка переменных окружения
ENV JAVA_HOME=/opt/java/jdk-23.0.2
ENV SPARK_HOME=/opt/spark/spark-3.5.4-bin-hadoop3
ENV PATH=$PATH:$JAVA_HOME/bin:$SPARK_HOME/bin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip

# Копирование файла с зависимостями
COPY requirements.txt /requirements.txt
RUN chmod 777 /requirements.txt

USER airflow

# Установка Python-зависимостей
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt