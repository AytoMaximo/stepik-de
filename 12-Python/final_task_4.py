purchases = [
    {"item": "apple", "category": "fruit", "price": 1.2, "quantity": 10},
    {"item": "banana", "category": "fruit", "price": 0.5, "quantity": 5},
    {"item": "milk", "category": "dairy", "price": 1.5, "quantity": 2},
    {"item": "bread", "category": "bakery", "price": 2.0, "quantity": 3},
]

def total_revenue(purchases):
    return sum(item["price"] * item["quantity"] for item in purchases)

def items_by_category(purchases):
    categories = {}
    for item in purchases:
        category = item["category"]
        if category not in categories:
            categories[category] = set()
        categories[category].add(item["item"])
    return {category: list(items) for category, items in categories.items()}

def expensive_purchases(purchases, min_price):
    return [item for item in purchases if item["price"] >= min_price]

def average_price_by_category(purchases):
    category_totals = {}
    category_counts = {}
    for item in purchases:
        category = item["category"]
        category_totals[category] = category_totals.get(category, 0) + item["price"]
        category_counts[category] = category_counts.get(category, 0) + 1
    return {category: category_totals[category] / category_counts[category] for category in category_totals}

def most_frequent_category(purchases):
    category_quantities = {}
    for item in purchases:
        category = item["category"]
        category_quantities[category] = category_quantities.get(category, 0) + item["quantity"]
    return max(category_quantities, key=category_quantities.get)

def generate_report(purchases, min_price=1.0):
    print(f"Общая выручка: {total_revenue(purchases)}")
    print(f"Товары по категориям: {items_by_category(purchases)}")
    print(f"Покупки дороже {min_price}: {expensive_purchases(purchases, min_price)}")
    print(f"Средняя цена по категориям: {average_price_by_category(purchases)}")
    print(f"Категория с наибольшим количеством проданных товаров: {most_frequent_category(purchases)}")

generate_report(purchases)