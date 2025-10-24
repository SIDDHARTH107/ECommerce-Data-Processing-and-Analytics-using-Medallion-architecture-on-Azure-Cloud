# Databricks notebook source
spark

# COMMAND ----------

dbutils.widgets.text("storage_key", "Paste Your Key Here", "Storage Account Key")

# COMMAND ----------

# 1. Defining my storage account and container names
storage_account_name = "olistdatastorageaccount4"
container_name = "olistdata" 
mount_point = f"/mnt/{container_name}"

# 2. Securely getting the key from the text box
storage_key_from_widget = dbutils.widgets.get("storage_key")

# 3. Unmounting if it already exists
try:
  dbutils.fs.unmount(mount_point)
except:
  pass 

# 4. Creating the connection using a consistent protocol
# We will use 'wasbs' and '.blob.core.windows.net' for both the source and the configuration.
source_url = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/"
config_key = f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net" # <-- This key is now consistent

dbutils.fs.mount(
  source = source_url,
  mount_point = mount_point,
  extra_configs = {config_key: storage_key_from_widget}
)

print(f"✅ Success! The '{container_name}' container is now available at {mount_point}")

# 5. Verifying the connection by listing the files INSIDE the 'bronze' folder
display(dbutils.fs.ls(f"{mount_point}/bronze/"))

# COMMAND ----------

# MAGIC %md
# MAGIC __DISPLAYING SOME SAMPLE DATA IN DATABRICKS__

# COMMAND ----------

# The mount point is a standard file path, without the "dbfs:" scheme
mount_point = "/mnt/olistdata" 

df = spark.read.format("csv").option(
    "header", "true"
).load(
    f"{mount_point}/bronze/olist_sellers_dataset.csv"
)

display(df)

# COMMAND ----------

# The mount point is a standard file path, without the "dbfs:" scheme
mount_point = "/mnt/olistdata" 

df = spark.read.format("csv").option(
    "header", "true"
).load(
    f"{mount_point}/bronze/olist_customers_dataset.csv"
)

display(df)

# COMMAND ----------

# row_count = df.count()
# col_count = len(df.columns)
# shape = (row_count, col_count)
# shape

(df.count(), len(df.columns))

# COMMAND ----------

# Defining the base path
base_path = "/mnt/olistdata/bronze"

# Reading each file into its own variable
orders_df = spark.read.option("header", "true").csv(f"{base_path}/olist_orders_dataset.csv")
payments_df = spark.read.option("header", "true").csv(f"{base_path}/olist_order_payments_dataset.csv")
reviews_df = spark.read.option("header", "true").csv(f"{base_path}/olist_order_reviews_dataset.csv")
items_df = spark.read.option("header", "true").csv(f"{base_path}/olist_order_items_dataset.csv")
customers_df = spark.read.option("header", "true").csv(f"{base_path}/olist_customers_dataset.csv")
sellers_df = spark.read.option("header", "true").csv(f"{base_path}/olist_sellers_dataset.csv")
geolocation_df = spark.read.option("header", "true").csv(f"{base_path}/olist_geolocation_dataset.csv")
products_df = spark.read.option("header", "true").csv(f"{base_path}/olist_products_dataset.csv")

# Now we can display any of them
display(customers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC __CONNECTION WITH MONGODB__

# COMMAND ----------

# MAGIC %md
# MAGIC __Setting up Libraries and Connection Details__

# COMMAND ----------

# 1. Importing the necessary library
from pymongo import MongoClient
import pandas as pd

# COMMAND ----------

# 2. Setting up my connection string
# IMPORTANT: Replacing <username>, <password>, and the cluster URL with your actual credentials from MongoDB Atlas.
username = "databricks_user"
password = "AK1wwQ0NVi8OdYbq"
cluster_url = "cluster0.1o5zju7.mongodb.net" # Atlas connection string

# COMMAND ----------

# These are the names of the database and collection I created
db_name = "olist_data"
collection_name = "product_category_translation"

# COMMAND ----------

# Building the connection string
uri = f"mongodb+srv://{username}:{password}@{cluster_url}/?retryWrites=true&w=majority"

# COMMAND ----------

print("Variables are set. Ready to connect in the next cell.")

# COMMAND ----------

# MAGIC %md
# MAGIC **Connecting to MongoDB Atlas:** This cell uses the uri variable from Cell 1 to create the connection. It will ping the database to confirm it's successful.

# COMMAND ----------

# This uses the 'uri' variable from Cell 1

print("Attempting to connect to MongoDB Atlas...")
try:
    client = MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]
    
    # Pinging the server to confirm the connection
    client.admin.command('ping')
    print("✅ MongoDB connection successful!")

except Exception as e:
    print("❌ Connection failed. Please check your username, password, or IP whitelist settings.")
    print(f"Error details: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC __Reading the Data from the Collection__: This cell will use the collection variable to find all the documents and pull them into a list.

# COMMAND ----------

# Cell 3: Reading the Data from the Collection
# This uses the 'collection' variable from Cell 2

print(f"Reading data from the '{collection_name}' collection...")

# Finding all documents ({}) and convert them to a list
data = list(collection.find({}))

if data:
    # The file had 71 documents or records, so this should match i guess
    print(f"✅ Successfully found {len(data)} documents.")
else:
    print("❌ No data found. Make sure you imported the data to your Atlas collection.")

# COMMAND ----------

# MAGIC %md
# MAGIC __Converting Data to a Spark DataFrame__: This cell takes the data list from Cell 3, cleans it up using Pandas, and converts it into a Spark DataFrame.

# COMMAND ----------

# Cell 4: Converting Data to a Spark DataFrame
# This uses the 'data' variable from Cell 3

if 'data' in locals() and data:
    print("Converting data to a Spark DataFrame...")
    
    # Converting the list of data to a Pandas DataFrame
    pandas_df = pd.DataFrame(data)
    
    # Dropping the '_id' column that MongoDB automatically adds
    if '_id' in pandas_df.columns:
        pandas_df = pandas_df.drop(columns=['_id'])
    
    # Converting the Pandas DataFrame into a Spark DataFrame
    translation_df = spark.createDataFrame(pandas_df)
    
    print("✅ Data successfully loaded into a Spark DataFrame named 'translation_df'.")
else:
    print("No data to convert. Please run Cell 3 first.")

# COMMAND ----------

# MAGIC %md
# MAGIC __Checking the Shape and Displaying the Data__: This is the final step. It uses the product_category_translation_df from Cell 4 to show the row/column count and then displays the data table.

# COMMAND ----------

# Cell 5: Checking Shape and Display the Data
# This uses the 'translation_df' variable from Cell 4

if 'translation_df' in locals():
    # Getting the row and column counts
    row_count = translation_df.count()
    column_count = len(translation_df.columns)
    
    print(f"--- Data Shape ---")
    print(f"Number of rows: {row_count}")
    print(f"Number of columns: {column_count}")
    print(f"--------------------")
    
    # Displaying the final DataFrame
    display(translation_df)
else:
    print("No DataFrame to display. Please run Cell 4 first.")

# COMMAND ----------

# MAGIC %md
# MAGIC __CLEANING THE DATA__

# COMMAND ----------

from pyspark.sql.functions import col, to_date, datediff, current_date

# COMMAND ----------

def clean_dataframe(df, name):
    print('Cleaning '+name)
    return df.dropDuplicates().na.drop('all')

orders_df = clean_dataframe(orders_df, 'Orders')
display(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC __CONVERTING DATE TIMESTAMPS TO ONLY DATE COLUMNS__

# COMMAND ----------

from pyspark.sql.functions import to_date, col, when

# COMMAND ----------

# Converting date time stamp to date columns
orders_df = orders_df \
    .withColumn("order_purchase_timestamp", to_date(col("order_purchase_timestamp"))) \
    .withColumn("order_approved_at", to_date(col("order_approved_at"))) \
    .withColumn("order_delivered_carrier_date", to_date(col("order_delivered_carrier_date"))) \
    .withColumn("order_delivered_customer_date", to_date(col("order_delivered_customer_date"))) \
    .withColumn("order_estimated_delivery_date", to_date(col("order_estimated_delivery_date")))


# COMMAND ----------

# To Check the Schema
orders_df.printSchema()

# COMMAND ----------

display(orders_df)

# COMMAND ----------

# Calculating Delivery and Time Delays
orders_df = orders_df.withColumn('actual_delivery_time', datediff('order_delivered_customer_date', 'order_purchase_timestamp'))
orders_df = orders_df.withColumn('estimated_delivery_time', datediff('order_estimated_delivery_date', 'order_purchase_timestamp'))
# orders_df = orders_df.withColumn('delay', when(col('actual_delivery_time') > col('estimated_delivery_time'), 1).otherwise(0))
orders_df = orders_df.withColumn('Delay_Time_boolean', col('actual_delivery_time') > col('estimated_delivery_time'))

display(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Here, I am finding to estimate or figure it out why are we taking so much time to deliver it to the customer address? Is there any problem in Supply chain or in the delivery process by delivery boy or something else? Like we can raise this question.

# COMMAND ----------

orders_df = orders_df.drop("delay_boolean")
display(orders_df)

# COMMAND ----------

display(orders_df.tail(5))
display(orders_df.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC __JOINING THE TABLES ACCORDINGLY__

# COMMAND ----------

orders_payments_df.columns

# COMMAND ----------

# Joining Orders and Customers
orders_customers_df = orders_df.join(
    customers_df,
    orders_df.customer_id == customers_df.customer_id,
    "left"
)

# Joining the above result with Payments
orders_payments_df = orders_customers_df.join(
    payments_df,
    orders_customers_df.order_id == payments_df.order_id,
    "left"
)

# Joining Orders + Payments with Items using order_id
orders_items_df = orders_payments_df.join(items_df, "order_id", "left")

# Joining Orders + Items with Products
orders_items_products_df = orders_items_df.join(
    products_df,
    orders_items_df.product_id == products_df.product_id,
    "left"
)

final_df = orders_items_products_df.join(
    sellers_df,
    orders_items_products_df.seller_id == sellers_df.seller_id,
    "left"
)

# COMMAND ----------

display(final_df)

# COMMAND ----------

# Converting my mongodb data from pandas format to spark format
display(translation_df)

# COMMAND ----------

# translation_df = translation_df.drop('_id')
# mongo_spark_df = spark.createDataFrame(translation_df)
type(translation_df)

# COMMAND ----------

# Dropping the _id column (no conversion needed)
translation_df = translation_df.drop('_id')
# Displaying & checking schema
display(translation_df)

# COMMAND ----------

final_df = final_df.join(translation_df, "product_category_name", "left")

# COMMAND ----------

display(final_df)

# COMMAND ----------

display(final_df.head(10))

# COMMAND ----------

# Dropping duplicate columns in the final_df
# This function scans through all the columns in your DataFrame, finds any column names that appear more than once, and removes the duplicate ones — keeping only the first occurrence.
def remove_duplicate_columns(df):
    columns = df.columns
    seen_columns = set()
    columns_to_drop = []

    for column in columns:
        if column in seen_columns:
            columns_to_drop.append(column)
        else:
            seen_columns.add(column)
    
    df_cleaned = df.drop(*columns_to_drop)
    return df_cleaned

final_df = remove_duplicate_columns(final_df)

# COMMAND ----------

# This line is saving the final Spark DataFrame (final_df) as a Parquet file into the Azure Databricks storage location /mnt/olistdata/silver (Silver) folder. It's like a read_csv in pandas.
final_df.write.mode("overwrite").parquet("/mnt/olistdata/silver")

# COMMAND ----------

