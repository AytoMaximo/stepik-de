import csv
from faker import Faker
import random

fake = Faker()

num_records = 100000

http_methods = ['GET', 'POST', 'PUT', 'DELETE']
response_codes = [200, 301, 404, 500]

file_path = "web_server_logs.csv"

with open(file_path, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['ip', 'timestamp', 'method', 'url', 'response_code', 'response_size'])

    for _ in range(num_records):
        ip = fake.ipv4()
        timestamp = fake.date_time_this_year().isoformat()
        method = random.choice(http_methods)
        url = fake.uri_path()
        response_code = random.choice(response_codes)
        response_size = random.randint(100, 10000)

        writer.writerow([ip, timestamp, method, url, response_code, response_size])

print(f"Сгенерировано {num_records} записей и сохранено в {file_path}")