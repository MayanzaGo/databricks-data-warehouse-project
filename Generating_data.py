from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

# Generate Date Dimension data (for a range of years)
start_date = datetime(2020, 1, 1)
num_days = 365 * 5  # 5 years

date_dim_data = []
for i in range(num_days):
    current_date = start_date + timedelta(days=i)
    date_id = int(current_date.strftime('%Y%m%d'))
    date_dim_data.append((
        date_id,
        current_date.date(),
        current_date.day,
        current_date.strftime('%B'),
        current_date.year,
        f"Q{((current_date.month - 1) // 3) + 1}"
    ))

# Generate Product Dimension data
categories = ['Electronics', 'Stationery', 'Clothing', 'Groceries']
product_dim_data = []
for product_id in range(1001, 1101):  # 100 products
    product_dim_data.append((
        product_id,
        fake.word().capitalize(),
        random.choice(categories),
        round(random.uniform(10, 1000), 2)
    ))

# Generate Customer Dimension data
regions = ['USA', 'UK', 'Germany', 'France', 'Canada', 'Australia']
customer_dim_data = []
for customer_id in range(5001, 5201):  # 200 customers
    customer_dim_data.append((
        customer_id,
        fake.name(),
        random.choice(regions)
    ))

# Generate Store Dimension data
cities = ['New York', 'Berlin', 'London', 'Paris', 'Toronto', 'Sydney']
store_dim_data = []
for store_id in range(1, 21):  # 20 stores
    store_dim_data.append((
        store_id,
        fake.company(),
        random.choice(cities),
        random.choice(['USA', 'Germany', 'UK', 'France', 'Canada', 'Australia'])
    ))

# Generate Sales Fact data
sales_fact_data = []
for sales_id in range(10001, 11001):  # 1000 sales records
    date_id = random.choice(date_dim_data)[0]
    store_id = random.choice(store_dim_data)[0]
    product_id = random.choice(product_dim_data)[0]
    customer_id = random.choice(customer_dim_data)[0]
    sales_amount = round(random.uniform(20, 5000), 2)
    quantity_sold = random.randint(1, 10)
    sales_fact_data.append((
        sales_id,
        date_id,
        store_id,
        product_id,
        customer_id,
        sales_amount,
        quantity_sold
    ))

