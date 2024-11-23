#!/bin/bash

# Лог-файл
LOG_FILE="access.log"

# Проверяем, существует ли файл
if [ ! -f "$LOG_FILE" ]; then
    echo "Файл $LOG_FILE не найден!"
    exit 1
fi

# 1. Подсчитать общее количество запросов
total_requests=0
while read -r line; do
    total_requests=$((total_requests + 1))
done < "$LOG_FILE"

# 2. Подсчитать количество уникальных IP-адресов
unique_ips_list=""
while read -r line; do
    ip=$(echo "$line" | awk '{print $1}')
    if [[ ! "$unique_ips_list" =~ "$ip" ]]; then
        unique_ips_list="$unique_ips_list $ip"
    fi
done < "$LOG_FILE"
unique_ips=$(echo "$unique_ips_list" | wc -w)

# 3. Подсчитать количество запросов по методам
get_count=0
post_count=0
while read -r line; do
    method=$(echo "$line" | awk '{print $6}' | tr -d '"')
    if [ "$method" = "GET" ]; then
        get_count=$((get_count + 1))
    elif [ "$method" = "POST" ]; then
        post_count=$((post_count + 1))
    fi
done < "$LOG_FILE"

# 4. Найти самый популярный URL
declare -A url_counts
while read -r line; do
    url=$(echo "$line" | awk '{print $7}')
    if [ -n "$url" ]; then
        url_counts["$url"]=$((url_counts["$url"] + 1))
    fi
done < "$LOG_FILE"

most_popular_url=""
most_popular_url_count=0
for url in "${!url_counts[@]}"; do
    if [ "${url_counts[$url]}" -gt "$most_popular_url_count" ]; then
        most_popular_url="$url"
        most_popular_url_count="${url_counts[$url]}"
    fi
done

# 5. Создать отчет
{
    echo "Отчет о логе веб-сервера"
    echo "========================="
    echo "Общее количество запросов: $total_requests"
    echo "Количество уникальных IP-адресов: $unique_ips"
    echo ""
    echo "Количество запросов по методам:"
    echo "$get_count GET"
    echo "$post_count POST"
    echo ""
    echo "Самый популярный URL: $most_popular_url_count $most_popular_url"
} > report.txt

# Вывод результата
echo "Отчет сохранен в файл report.txt"
