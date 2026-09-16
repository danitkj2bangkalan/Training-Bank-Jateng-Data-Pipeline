```python
# install required dependencies
!pip install pyspark
```
**Output:**
```
Requirement already satisfied: pyspark in /usr/local/lib/python3.13/dist-packages (4.0.4)
Requirement already satisfied: py4j<0.10.9.10,>=0.10.9.7 in /usr/local/lib/python3.13/dist-packages (from pyspark) (0.10.9.9)
```

```python
from google.colab import drive
import sqlite3
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
```

```python
# mounting google drive as data lake
drive.mount('/content/drive')

products_csv_path = '/content/drive/MyDrive/Bank Jateng Training/products.csv'
transactions_csv_path = '/content/drive/MyDrive/Bank Jateng Training/transactions.csv'
customers_csv_path = '/content/drive/MyDrive/Bank Jateng Training/customers.csv'
```
**Output:**
```
Mounted at /content/drive
```

```python
# Initialize Spark session
spark = SparkSession.builder \
    .appName("GoogleColabPySpark") \
    .getOrCreate()

print("Spark Version:", spark.version)
```
**Output:**
```
Spark Version: 4.0.4
```

```python
# create connector
conn = sqlite3.connect('my_database.db')
cursor = conn.cursor()
```

```python
cursor.execute('CREATE TABLE IF NOT EXISTS customers (customer_id TEXT, name TEXT, email TEXT, age INTEGER, region TEXT, restructure_date TEXT)')
```
**Output:**
```
<sqlite3.Cursor at 0x7c507d398d40>
```

```python
# Extract Layer
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Extract CSV Example") \
    .getOrCreate()

df_products = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(products_csv_path)

df_products.show(5)

df_products.printSchema()
```
**Output:**
```
+----------+--------------+-----------+-----+
|product_id|  product_name|   category|price|
+----------+--------------+-----------+-----+
|      P001| Laptop Pro 14|Electronics| 1500|
|      P002|Wireless Mouse|Electronics|   25|
|      P003|  Office Chair|  Furniture|  220|
|      P004|     USB-C Hub|Electronics|   45|
|      P005|     Desk Lamp|  Furniture|   35|
+----------+--------------+-----------+-----+

root
 |-- product_id: string (nullable = true)
 |-- product_name: string (nullable = true)
 |-- category: string (nullable = true)
 |-- price: integer (nullable = true)

```

```python
df_transactions = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(transactions_csv_path)

df_transactions.show(5)

df_transactions.printSchema()
```
**Output:**
```
+--------------+-----------+----------+----------------+------+---------+
|transaction_id|customer_id|product_id|transaction_date|amount|   status|
+--------------+-----------+----------+----------------+------+---------+
|          T001|       C001|      P001|      2025-05-01|  1500|COMPLETED|
|          T002|       C001|      P002|      2025-05-02|    25|COMPLETED|
|          T003|       C002|      P003|      05/03/2025|   220|completed|
|          T004|       C003|      P004|      2025-05-04|    45|COMPLETED|
|          T005|       C004|      P005|      2025-05-05|    35|COMPLETED|
+--------------+-----------+----------+----------------+------+---------+
only showing top 5 rows
root
 |-- transaction_id: string (nullable = true)
 |-- customer_id: string (nullable = true)
 |-- product_id: string (nullable = true)
 |-- transaction_date: string (nullable = true)
 |-- amount: integer (nullable = true)
 |-- status: string (nullable = true)

```

```python
df_customers = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(customers_csv_path)

df_customers.show(5)

df_customers.printSchema()
```
**Output:**
```
+-----------+------------+--------------------+----+-------+----------+
|customer_id|        name|               email| age| region|created_at|
+-----------+------------+--------------------+----+-------+----------+
|       C001|    John Doe|JOHN.DOE@EXAMPLE.COM|  34|   WEST|2025-01-10|
|       C002|   john doe |john.doe@example.com|NULL|   WEST|2025-02-15|
|       C003|MARIA GARCIA|maria.garcia@exam...|  29|CENTRAL|2025-01-22|
|       C004|Budi Santoso| BUDI.SANTOSO@EXA...|  41|   east|2025-03-02|
|       C005| Siti Aminah|                NULL|  37|CENTRAL|2025-03-11|
+-----------+------------+--------------------+----+-------+----------+
only showing top 5 rows
root
 |-- customer_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- email: string (nullable = true)
 |-- age: string (nullable = true)
 |-- region: string (nullable = true)
 |-- created_at: date (nullable = true)

```

```python
# transform layer

df_cust = df_customers.withColumn("restructure_date", sf.date_format(sf.col("created_at"),"dd/MM/yyyy")) \
            .withColumn("region", sf.upper(sf.col("region"))) \
            .withColumnRenamed("customer_id", "user_id") \
            .drop("created_at")\
            .fillna('Unknown')
df_cust.show()
```
**Output:**
```
+-------+-------------+--------------------+-------------+-------+----------------+
|user_id|         name|               email|          age| region|restructure_date|
+-------+-------------+--------------------+-------------+-------+----------------+
|   C001|     John Doe|JOHN.DOE@EXAMPLE.COM|           34|   WEST|      10/01/2025|
|   C002|    john doe |john.doe@example.com|      Unknown|   WEST|      15/02/2025|
|   C003| MARIA GARCIA|maria.garcia@exam...|           29|CENTRAL|      22/01/2025|
|   C004| Budi Santoso| BUDI.SANTOSO@EXA...|           41|   EAST|      02/03/2025|
|   C005|  Siti Aminah|             Unknown|           37|CENTRAL|      11/03/2025|
|   C006| Robert Smith|robert.smith@exam...|not_available|   WEST|      18/03/2025|
|   C007|  Andi Wijaya|andi.wijaya@examp...|           25|  NORTH|      21/03/2025|
|   C008|   Laura Chen|laura.chen@exampl...|           31|   EAST|      05/04/2025|
|   C009|Michael Brown|michael.brown@exa...|           52|   WEST|      18/04/2025|
|   C010| Dewi Lestari|dewi.lestari@exam...|           28|CENTRAL|      22/04/2025|
+-------+-------------+--------------------+-------------+-------+----------------+

```

```python
df_transactions = df_transactions.select(
    df_transactions.amount,
    sf.when(df_transactions.amount < 0, "poor")
    .when((df_transactions.amount > 0) & (df_transactions.amount <= 500), "standart")
    .otherwise("good").alias("transaction_type")
)
df_transactions.show()
```
**Output:**
```
+------+----------------+
|amount|transaction_type|
+------+----------------+
|  1500|            good|
|    25|        standart|
|   220|        standart|
|    45|        standart|
|    35|        standart|
|  NULL|            good|
| -1500|            poor|
|    25|        standart|
|   220|        standart|
|    45|        standart|
|    35|        standart|
|    35|        standart|
|    25|        standart|
|    50|        standart|
+------+----------------+

```

```python
pandas_df = df_cust.toPandas()
pandas_df.to_sql("customers", conn, if_exists="replace", index=False)
```
**Output:**
```
10
```

```python
cursor.execute('select * from customers')
print(cursor.fetchall())
# conn.close()
```
**Output:**
```
[('C001', 'John Doe', 'JOHN.DOE@EXAMPLE.COM', '34', 'WEST', '10/01/2025'), ('C002', ' john doe ', 'john.doe@example.com', 'Unknown', 'WEST', '15/02/2025'), ('C003', 'MARIA GARCIA', 'maria.garcia@example.com', '29', 'CENTRAL', '22/01/2025'), ('C004', 'Budi Santoso', ' BUDI.SANTOSO@EXAMPLE.COM ', '41', 'EAST', '02/03/2025'), ('C005', 'Siti Aminah', 'Unknown', '37', 'CENTRAL', '11/03/2025'), ('C006', 'Robert Smith', 'robert.smith@example.com', 'not_available', 'WEST', '18/03/2025'), ('C007', 'Andi Wijaya', 'andi.wijaya@example.com', '25', 'NORTH', '21/03/2025'), ('C008', 'Laura Chen', 'laura.chen@example.com', '31', 'EAST', '05/04/2025'), ('C009', 'Michael Brown', 'michael.brown@example.com', '52', 'WEST', '18/04/2025'), ('C010', 'Dewi Lestari', 'dewi.lestari@example.com', '28', 'CENTRAL', '22/04/2025')]
```

```python
import requests
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RestApiIngestion").getOrCreate()

# 1. Fetch JSON data from API
url = "https://jsonplaceholder.typicode.com/posts"
response = requests.get(url)
data = response.json()  # List of dictionaries

# 2. Convert raw JSON into a Spark DataFrame
df = spark.createDataFrame(data)
df.show(5)
```
**Output:**
```
+--------------------+---+--------------------+------+
|                body| id|               title|userId|
+--------------------+---+--------------------+------+
|quia et suscipit\...|  1|sunt aut facere r...|     1|
|est rerum tempore...|  2|        qui est esse|     1|
|et iusto sed quo ...|  3|ea molestias quas...|     1|
|ullam et saepe re...|  4|eum et est occaecati|     1|
|repudiandae venia...|  5|  nesciunt quas odio|     1|
+--------------------+---+--------------------+------+
only showing top 5 rows
```

```python
import requests
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DistributedApiIngestion").getOrCreate()

urls = [f"https://jsonplaceholder.typicode.com/posts?_page={i}" for i in range(1, 6)]

rdd_urls = spark.sparkContext.parallelize(urls)

def fetch_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    return []

rdd_results = rdd_urls.flatMap(fetch_data)
df_api = spark.createDataFrame(rdd_results)

df_api.show(5)
```
**Output:**
```
+--------------------+---+--------------------+------+
|                body| id|               title|userId|
+--------------------+---+--------------------+------+
|quia et suscipit\...|  1|sunt aut facere r...|     1|
|est rerum tempore...|  2|        qui est esse|     1|
|et iusto sed quo ...|  3|ea molestias quas...|     1|
|ullam et saepe re...|  4|eum et est occaecati|     1|
|repudiandae venia...|  5|  nesciunt quas odio|     1|
+--------------------+---+--------------------+------+
only showing top 5 rows
```

