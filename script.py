import numpy as np
import pandas as pd
import pyodbc
from unidecode import unidecode
import re

server = 'Tarash'
database = 'CampusXproject2'

connection_string = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={server};"
    f"DATABASE={database};"
    "Trusted_Connection=yes;"
)

conn = pyodbc.connect(connection_string)

def remove_numbers(s):
    return re.sub(r'\d+', '', str(s))


query = "SELECT * FROM olist_customers_dataset;"
customers = pd.read_sql(query, conn)
customers["customer_city"] = customers["customer_city"].str.title()
customers.drop(columns="customer_unique_id", inplace=True)
customers["customer_city"] = customers["customer_city"].apply(lambda x: unidecode(x))
customers["customer_city"] = customers["customer_city"].apply(remove_numbers)
customers["customer_city"] = customers["customer_city"].str.replace("'", "", regex=False)
customers["customer_city"] = customers["customer_city"].apply(
    lambda x: x[3:] if str(x).startswith("...") else x
)
customers["customer_city"] = customers["customer_city"].apply(
    lambda x: x[1:] if str(x).startswith("*") else x
)


query = "SELECT * FROM olist_geolocation_dataset"
geo = pd.read_sql(query, conn)
geo.drop(columns=["geolocation_lat", "geolocation_lng"], inplace=True)
geo["geolocation_city"] = geo["geolocation_city"].apply(lambda x: unidecode(x))
geo["geolocation_city"] = geo["geolocation_city"].apply(remove_numbers)
geo["geolocation_city"] = geo["geolocation_city"].str.replace("'", "", regex=False)
geo["geolocation_city"] = geo["geolocation_city"].apply(
    lambda x: x[3:] if str(x).startswith("...") else x
)
geo["geolocation_city"] = geo["geolocation_city"].apply(
    lambda x: x[1:] if str(x).startswith("*") else x
)
geo["geolocation_city"] = geo["geolocation_city"].str.title()


query = "SELECT * FROM olist_products_dataset"
products = pd.read_sql(query, conn)
products.drop(columns=[
    'product_name_lenght',
    'product_description_lenght',
    'product_weight_g',
    'product_length_cm',
    'product_height_cm',
    'product_width_cm'
], inplace=True)

query = "SELECT * FROM product_category_name_translation"
product_name = pd.read_sql(query, conn)

products = pd.merge(
    products,
    product_name,
    left_on="product_category_name",
    right_on="column1",
    how="left"
)

products.loc[
    products["product_category_name"] == "pc_gamer",
    "column2"
] = "computers"

products.loc[
    products["product_category_name"] == "portateis_cozinha_e_preparadores_de_alimentos",
    "column2"
] = "small_appliances_home_oven_and_coffee"

products["product_category_name"] = products["column2"]
products.drop(columns=["column1", "column2"], inplace=True)


query = "SELECT * FROM olist_sellers_dataset"
seller = pd.read_sql(query, conn)
seller["seller_city"] = seller["seller_city"].str.title()


query = "SELECT * FROM olist_orders_dataset"
orders = pd.read_sql(query, conn)

date_cols = [
    'order_purchase_timestamp',
    'order_approved_at',
    'order_delivered_carrier_date',
    'order_delivered_customer_date',
    'order_estimated_delivery_date'
]

for c in date_cols:
    orders[c] = pd.to_datetime(orders[c], errors='coerce', infer_datetime_format=True)

orders = orders[
    ((orders['order_approved_at'] - orders['order_purchase_timestamp']).dt.total_seconds()/86400 >= 0) &
    ((orders['order_delivered_carrier_date'] - orders['order_approved_at']).dt.days >= 0)
]

to_remove = orders[
    (orders['order_delivered_customer_date'].notna()) &
    (orders["order_status"] == "canceled")
].index

orders = orders.drop(to_remove)


query = "SELECT * FROM olist_order_payments_dataset"
payments = pd.read_sql(query, conn)
payments.drop(columns=["payment_sequential", "payment_installments"], inplace=True)

payments = payments.groupby(["order_id", "payment_type"]).sum("payment_value").reset_index()

orders = pd.merge(orders, payments, on="order_id", how="inner")


query = "SELECT * FROM olist_order_items_dataset"
olist_items = pd.read_sql(query, conn)

orders = pd.merge(orders, olist_items, on="order_id", how="left")

orders = orders[
    (orders["shipping_limit_date"] > orders["order_purchase_timestamp"]) &
    (orders["shipping_limit_date"] > orders["order_approved_at"]) &
    (orders["shipping_limit_date"] < orders["order_delivered_customer_date"])
]

orders.sort_values(by=["order_item_id", "order_id"], ascending=False, inplace=True)

orders.drop(columns="order_item_id", inplace=True)

orders = orders.groupby([
    'order_id', 'customer_id', 'order_status', 'order_purchase_timestamp',
    'order_approved_at', 'order_delivered_carrier_date',
    'order_delivered_customer_date', 'order_estimated_delivery_date',
    'payment_type', 'payment_value', 'product_id', 'seller_id',
    'shipping_limit_date'
]).agg(
    price=("price", "sum"),
    freight_value=("freight_value", "sum"),
    Quantity=("product_id", "count")
).reset_index()


query = "SELECT * FROM olist_order_reviews_dataset"
reviews = pd.read_sql(query, conn)
reviews = reviews[reviews["order_id"].isin(orders["order_id"])]
reviews.drop(columns="review_comment_title", inplace=True)
reviews['review_answer_timestamp'] = reviews['review_answer_timestamp'].dt.date
