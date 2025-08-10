from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType
from datetime import date 

#  Initialize Spark Session
spark = SparkSession.builder.appName('DataWarehouseA'). getOrCreate()

# Fact Table: Sales Fact
sales_fact_schema = StructType([
    StructField('SalesID', IntegerType(), False),
    StructField('DateID', IntegerType(), False),
    StructField('StoreID', IntegerType(), False),
    StructField('ProductID', IntegerType(), False),
    StructField('CustomerID', IntegerType(), False),
    StructField('Sales_Amount', FloatType(), False),
    StructField('Quantity_Sold', FloatType(), False)
])

# Dimension Tables : Date Dimension
date_dim_schema = StructType([
    StructField('DateID', IntegerType(), False),
    StructField('Date', DateType(), False),
    StructField('Day', IntegerType(), False),
    StructField('Month', StringType(), False),
    StructField('Year', IntegerType(), False),
    StructField('Quarter', StringType(), False)
])

# Dimesion Tables : Product Dimension
product_dim_schema = StructType([
    StructField('ProductID', IntegerType(), False),
    StructField('Product', StringType(), False),
    StructField('Category', StringType(), False),
    StructField('Price', FloatType(), False)
])

# Dimension Tables : Customer Dimension
customer_dim_schema = StructType([
    StructField('CustomerID', IntegerType(), False),
    StructField('Customer_Name', StringType(), False),
    StructField('Region', StringType(), False)
])

# Dimension Tables : Store Dimension
store_dim_schema = StructType([
    StructField('StoreID', IntegerType(), False),
    StructField('Store_Name', StringType(), False),
    StructField('City', StringType(), False),
    StructField('Country', StringType(), False)
])



# Let's import the necessary library
!pip install faker

%restart_python

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



# Now create DataFrames from  the generated data 
sales_fact_df = spark.createDataFrame(sales_fact_data, schema=sales_fact_schema)
date_dim_df = spark.createDataFrame(date_dim_data, schema=date_dim_schema)
product_dim_df = spark.createDataFrame(product_dim_data, schema=product_dim_schema)
customer_dim_df = spark.createDataFrame(customer_dim_data, schema=customer_dim_schema)
store_dim_df = spark.createDataFrame(store_dim_data, schema=store_dim_schema)

# Let's visualize the DataFrames we created
# Sales fact dataframe
print("Sales Fact Dataframe")
sales_fact_df.show()
# Data dimension dataframe
print("Date Dimension Dataframe")
date_dim_df.show()
# Product dimension dataframe
print("Product Dimension Dataframe")
product_dim_df.show()
# Customer dimension dataframe
print("Customer Dimension Dataframe")
customer_dim_df.show()
# Store dimension dataframe
print("Store Dimension Dataframe")
store_dim_df.show()

# Let's join sales fact table with all dimensions tables
# We can assign it to a variable : sales_with_details_df
sales_with_details_df = sales_fact_df \
    .join(date_dim_df, on = "DateID") \
    .join(customer_dim_df, on = "CustomerID") \
    .join(product_dim_df, on = "ProductID") \
    .join(store_dim_df, on = "StoreID")


# Let's visualize the joined table
sales_with_details_df\
    .show()

# Let's display the schema of the joined table
sales_with_details_df.printSchema()

# Let's display a star schema view using select
sales_with_details_df.select("Category", "Store_Name",'Date',"Product", "Sales_Amount", "Quantity_Sold", "Customer_Name", "City", "Country").show()

# Let's import necessary library to perform OLAP summaries
from pyspark.sql.functions import sum,  avg, count, desc, max, min

# Let's group by aggregation
# Let's find the total sales amount
sales_with_details_df.groupBy('Country').agg(
    sum('Sales_Amount').alias('Total_Sales')
    ).show()

# Let's find the total  function by category and by country
sales_with_details_df.groupBy('Country', "Category")\
    .agg(sum('Sales_Amount').alias('Total_Sales'))\
    .orderBy(desc('Total_Sales'))\
    .show() 

# Average Sales Amount by Quarter
sales_with_details_df.groupBy("Quarter")\
    .agg(avg("Sales_Amount").alias("Average_sales"))\
    .orderBy(desc('Average_sales'))\
    .show()

# Let's find 5 Customers by Total Purchase Value
sales_with_details_df.groupBy('Customer_Name')\
    .agg(sum("Sales_Amount").alias('Total_purchase'))\
    .orderBy(desc('Total_purchase'))\
    .limit(5)\
    .show()

#  Let's find Quantity Sold by Store Location
sales_with_details_df.groupBy("Store_Name", "Country")\
    .agg(sum("Quantity_Sold").alias('Total_Quantity'))\
    .orderBy(desc('Total_Quantity'))\
    .orderBy("Country", "Total_Quantity")\
    .show()


# Save fact table as managed Delta table
sales_fact_df.write.format("delta").mode("overwrite").saveAsTable("sales_fact")

# Save dimension tables as managed Delta tables
date_dim_df.write.format("delta").mode("overwrite").saveAsTable("date_dim")
product_dim_df.write.format("delta").mode("overwrite").saveAsTable("product_dim")
customer_dim_df.write.format("delta").mode("overwrite").saveAsTable("customer_dim")
store_dim_df.write.format("delta").mode("overwrite").saveAsTable("store_dim")

# Load the managed tables back as DataFrames
sales_with_details_df = spark.table("sales_fact") \
    .join(spark.table("date_dim"), "DateID") \
    .join(spark.table("customer_dim"), "CustomerID") \
    .join(spark.table("product_dim"), "ProductID") \
    .join(spark.table("store_dim"), "StoreID")

# Let's run our star schema query
sales_with_details_df.select(
    "Category", "Date", "Product",
    "Sales_Amount", "Quantity_Sold",
    "Customer_Name", "City", "Country"
).show()

# Let's run  OLAP queries directly on the tables
from pyspark.sql.functions import sum as sum, desc

# Let's find the total sales by category 
spark.table("sales_fact") \
    .join(spark.table("product_dim"), "ProductID") \
    .groupBy("Category") \
    .agg(sum("Sales_Amount").alias("Total_Sales")) \
    .orderBy(desc("Total_Sales")) \
    .show()

# Let's install the required library 
%pip install networkx

%restart_python

import matplotlib.pyplot as plt
import networkx as nx

# Let's create a graph
G = nx.DiGraph()

# Let's add Fact Table
G.add_node('SalesFact', color = 'red', shape = 'box')

# Let's add Dimension Tables
dimensions = ['Datedim', 'Productdim', 'Customerdim', 'StoreDim']
for d in dimensions:
    G.add_node(d, color = 'skyblue')
    G.add_edge('SalesFact', d)

# Layout and Draw
pos = nx.spring_layout(G, seed = 42)
node_colors = [G.nodes[n]['color']  for n in G.nodes]

plt.figure(figsize=(10, 5))
nx.draw(G, pos, with_labels=True, node_color = node_colors, node_size = 3000, font_size = 8, arrows = True)
plt.title('Star Schema ER Diagram')
plt.axis('off')
plt.show()



from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType
from datetime import date 

#  Initialize Spark Session
spark = SparkSession.builder.appName('DataWarehouseA'). getOrCreate()

# Fact Table: Sales Fact
sales_fact_schema = StructType([
    StructField('SalesID', IntegerType(), False),
    StructField('DateID', IntegerType(), False),
    StructField('StoreID', IntegerType(), False),
    StructField('ProductID', IntegerType(), False),
    StructField('CustomerID', IntegerType(), False),
    StructField('Sales_Amount', FloatType(), False),
    StructField('Quantity_Sold', FloatType(), False)
])

# Dimension Tables : Date Dimension
date_dim_schema = StructType([
    StructField('DateID', IntegerType(), False),
    StructField('Date', DateType(), False),
    StructField('Day', IntegerType(), False),
    StructField('Month', StringType(), False),
    StructField('Year', IntegerType(), False),
    StructField('Quarter', StringType(), False)
])

# Dimesion Tables : Product Dimension
product_dim_schema = StructType([
    StructField('ProductID', IntegerType(), False),
    StructField('Product', StringType(), False),
    StructField('Category', StringType(), False),
    StructField('Price', FloatType(), False)
])

# Dimension Tables : Customer Dimension
customer_dim_schema = StructType([
    StructField('CustomerID', IntegerType(), False),
    StructField('Customer_Name', StringType(), False),
    StructField('Region', StringType(), False)
])

# Dimension Tables : Store Dimension
store_dim_schema = StructType([
    StructField('StoreID', IntegerType(), False),
    StructField('Store_Name', StringType(), False),
    StructField('City', StringType(), False),
    StructField('Country', StringType(), False)
])



# Let's import the necessary library
!pip install faker

%restart_python

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



# Now create DataFrames from  the generated data 
sales_fact_df = spark.createDataFrame(sales_fact_data, schema=sales_fact_schema)
date_dim_df = spark.createDataFrame(date_dim_data, schema=date_dim_schema)
product_dim_df = spark.createDataFrame(product_dim_data, schema=product_dim_schema)
customer_dim_df = spark.createDataFrame(customer_dim_data, schema=customer_dim_schema)
store_dim_df = spark.createDataFrame(store_dim_data, schema=store_dim_schema)

# Let's visualize the DataFrames we created
# Sales fact dataframe
print("Sales Fact Dataframe")
sales_fact_df.show()
# Data dimension dataframe
print("Date Dimension Dataframe")
date_dim_df.show()
# Product dimension dataframe
print("Product Dimension Dataframe")
product_dim_df.show()
# Customer dimension dataframe
print("Customer Dimension Dataframe")
customer_dim_df.show()
# Store dimension dataframe
print("Store Dimension Dataframe")
store_dim_df.show()

# Let's join sales fact table with all dimensions tables
# We can assign it to a variable : sales_with_details_df
sales_with_details_df = sales_fact_df \
    .join(date_dim_df, on = "DateID") \
    .join(customer_dim_df, on = "CustomerID") \
    .join(product_dim_df, on = "ProductID") \
    .join(store_dim_df, on = "StoreID")


# Let's visualize the joined table
sales_with_details_df\
    .show()

# Let's display the schema of the joined table
sales_with_details_df.printSchema()

# Let's display a star schema view using select
sales_with_details_df.select("Category", "Store_Name",'Date',"Product", "Sales_Amount", "Quantity_Sold", "Customer_Name", "City", "Country").show()

# Let's import necessary library to perform OLAP summaries
from pyspark.sql.functions import sum,  avg, count, desc, max, min

# Let's group by aggregation
# Let's find the total sales amount
sales_with_details_df.groupBy('Country').agg(
    sum('Sales_Amount').alias('Total_Sales')
    ).show()

# Let's find the total  function by category and by country
sales_with_details_df.groupBy('Country', "Category")\
    .agg(sum('Sales_Amount').alias('Total_Sales'))\
    .orderBy(desc('Total_Sales'))\
    .show() 

# Average Sales Amount by Quarter
sales_with_details_df.groupBy("Quarter")\
    .agg(avg("Sales_Amount").alias("Average_sales"))\
    .orderBy(desc('Average_sales'))\
    .show()

# Let's find 5 Customers by Total Purchase Value
sales_with_details_df.groupBy('Customer_Name')\
    .agg(sum("Sales_Amount").alias('Total_purchase'))\
    .orderBy(desc('Total_purchase'))\
    .limit(5)\
    .show()

#  Let's find Quantity Sold by Store Location
sales_with_details_df.groupBy("Store_Name", "Country")\
    .agg(sum("Quantity_Sold").alias('Total_Quantity'))\
    .orderBy(desc('Total_Quantity'))\
    .orderBy("Country", "Total_Quantity")\
    .show()


# Save fact table as managed Delta table
sales_fact_df.write.format("delta").mode("overwrite").saveAsTable("sales_fact")

# Save dimension tables as managed Delta tables
date_dim_df.write.format("delta").mode("overwrite").saveAsTable("date_dim")
product_dim_df.write.format("delta").mode("overwrite").saveAsTable("product_dim")
customer_dim_df.write.format("delta").mode("overwrite").saveAsTable("customer_dim")
store_dim_df.write.format("delta").mode("overwrite").saveAsTable("store_dim")

# Load the managed tables back as DataFrames
sales_with_details_df = spark.table("sales_fact") \
    .join(spark.table("date_dim"), "DateID") \
    .join(spark.table("customer_dim"), "CustomerID") \
    .join(spark.table("product_dim"), "ProductID") \
    .join(spark.table("store_dim"), "StoreID")

# Let's run our star schema query
sales_with_details_df.select(
    "Category", "Date", "Product",
    "Sales_Amount", "Quantity_Sold",
    "Customer_Name", "City", "Country"
).show()

# Let's run  OLAP queries directly on the tables
from pyspark.sql.functions import sum as sum, desc

# Let's find the total sales by category 
spark.table("sales_fact") \
    .join(spark.table("product_dim"), "ProductID") \
    .groupBy("Category") \
    .agg(sum("Sales_Amount").alias("Total_Sales")) \
    .orderBy(desc("Total_Sales")) \
    .show()

# Let's install the required library 
%pip install networkx

%restart_python

import matplotlib.pyplot as plt
import networkx as nx

# Let's create a graph
G = nx.DiGraph()

# Let's add Fact Table
G.add_node('SalesFact', color = 'red', shape = 'box')

# Let's add Dimension Tables
dimensions = ['Datedim', 'Productdim', 'Customerdim', 'StoreDim']
for d in dimensions:
    G.add_node(d, color = 'skyblue')
    G.add_edge('SalesFact', d)

# Layout and Draw
pos = nx.spring_layout(G, seed = 42)
node_colors = [G.nodes[n]['color']  for n in G.nodes]

plt.figure(figsize=(10, 5))
nx.draw(G, pos, with_labels=True, node_color = node_colors, node_size = 3000, font_size = 8, arrows = True)
plt.title('Star Schema ER Diagram')
plt.axis('off')
plt.show()


