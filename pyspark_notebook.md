# Dependencies Installation

```python
# install required dependencies
!pip install pyspark
```

**Output:**
```text
Requirement already satisfied: pyspark in /usr/local/lib/python3.13/dist-packages (4.0.4)
Requirement already satisfied: py4j<0.10.9.10,>=0.10.9.7 in /usr/local/lib/python3.13/dist-packages (from pyspark) (0.10.9.9)
```

---

# Import Libraries

```python
from google.colab import drive
import sqlite3
import requests
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
```

---

# Data Lake Initialization & Mount

```python
# mounting google drive as data lake
drive.mount('/content/drive')

products_csv_path = '/content/drive/MyDrive/Bank Jateng Training/products.csv'
transactions_csv_path = '/content/drive/MyDrive/Bank Jateng Training/transactions_dirty_100000.csv'
customers_csv_path = '/content/drive/MyDrive/Bank Jateng Training/customers_transactions.csv'
```

**Output:**
```text
Drive already mounted at /content/drive; to attempt to forcibly remount, call drive.mount("/content/drive", force_remount=True).
```

---

# REST API Ingestion Examples

```python
spark = SparkSession.builder.appName("RestApiIngestion").getOrCreate()

url = "https://jsonplaceholder.typicode.com/posts"
response = requests.get(url)
data = response.json()

df = spark.createDataFrame(data)
df.show(5)
```

**Output:**
```text
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

```pythonspark = SparkSession.builder.appName("DistributedApiIngestion").getOrCreate()

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
```text
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

---

# Spark Session Initialization

```python
# Initialize Spark session
spark = SparkSession.builder \
    .appName("GoogleColabPySpark") \
    .getOrCreate()

print("Spark Version:", spark.version)
```

**Output:**
```text
Spark Version: 4.0.4
```

---

# Extract Layer: Loading CSV Files

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
```text
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
```text
+--------------+-----------+----------+----------------+------+---------+
|transaction_id|customer_id|product_id|transaction_date|amount|   status|
+--------------+-----------+----------+----------------+------+---------+
|       T000001|     C42446|      P007|      2025-09-17| 216.0|COMPLETED|
|       T000002|     C28141|      P945|      2025-01-31| 320.0|COMPLETED|
|       T000003|     C74116|      P004|      2026-01-08|  90.0|COMPLETED|
|       T000004|      C6106|      P003|      2025-04-03| 220.0|COMPLETED|
|       T000005|     C13508|      P004|      2025-12-15|  90.0|CANCELLED|
+--------------+-----------+----------+----------------+------+---------+
only showing top 5 rows

root
 |-- transaction_id: string (nullable = true)
 |-- customer_id: string (nullable = true)
 |-- product_id: string (nullable = true)
 |-- transaction_date: string (nullable = true)
 |-- amount: double (nullable = true)
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
```text
+--------------+-----------+----------+----------------+------+---------+-------------+--------------------+---+---------+----------+
|transaction_id|customer_id|product_id|transaction_date|amount|   status|         name|               email|age|   region|created_at|
+--------------+-----------+----------+----------------+------+---------+-------------+--------------------+---+---------+----------+
|       T000001|     C42446|      P007|       9/17/2025| 216.0|COMPLETED| Laura Taylor|laura.taylor@exam...| 38|     WEST|  6/4/2025|
|       T000002|     C28141|      P945|       1/31/2025| 320.0|COMPLETED| Putri Taylor|putri.taylor@exam...| 21|  CENTRAL|  4/1/2025|
|       T000003|     C74116|      P004|        1/8/2026|  90.0|COMPLETED|     Budi Lee|budi.lee@example.com| 42|NORTHWEST| 7/21/2025|
|       T000004|      C6106|      P003|        4/3/2025| 220.0|COMPLETED|  Sari Wilson|sari.wilson@examp...| 56|NORTHWEST|  4/1/2025|
|       T000005|     C13508|      P004|      12/15/2025|  90.0|CANCELLED|Michael Jones|michael.jones@exa...| 32|     EAST|  3/4/2025|
+--------------+-----------+----------+----------------+------+---------+-------------+--------------------+---+---------+----------+
only showing top 5 rows

root
 |-- transaction_id: string (nullable = true)
 |-- customer_id: string (nullable = true)
 |-- product_id: string (nullable = true)
 |-- transaction_date: string (nullable = true)
 |-- amount: double (nullable = true)
 |-- status: string (nullable = true)
 |-- name: string (nullable = true)
 |-- email: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- region: string (nullable = true)
 |-- created_at: string (nullable = true)
```

---

# Transform Layer

```python
# transform layer

df_clean_all = df_customers.dropna(how="all")

df_customer_clean = df_clean_all.withColumn("Date", sf.to_date(sf.col("created_at"), "M/d/yyyy")) \
            .withColumn("region", sf.upper(sf.col("region"))) \
            .withColumnRenamed("customer_id", "user_id") \
            .drop("created_at")\
            .fillna('Unknown')

df_customer_clean.show()
```

**Output:**
```text
+--------------+-------+----------+----------------+------+---------+----------------+--------------------+---+---------+----------+
|transaction_id|user_id|product_id|transaction_date|amount|   status|            name|               email|age|   region|      Date|
+--------------+-------+----------+----------------+------+---------+----------------+--------------------+---+---------+----------+
|       T000001| C42446|      P007|       9/17/2025| 216.0|COMPLETED|    Laura Taylor|laura.taylor@exam...| 38|     WEST|2025-06-04|
|       T000002| C28141|      P945|       1/31/2025| 320.0|COMPLETED|    Putri Taylor|putri.taylor@exam...| 21|  CENTRAL|2025-04-01|
|       T000003| C74116|      P004|        1/8/2026|  90.0|COMPLETED|        Budi Lee|budi.lee@example.com| 42|NORTHWEST|2025-07-21|
|       T000004|  C6106|      P003|        4/3/2025| 220.0|COMPLETED|     Sari Wilson|sari.wilson@examp...| 56|NORTHWEST|2025-04-01|
|       T000005| C13508|      P004|      12/15/2025|  90.0|CANCELLED|   Michael Jones|michael.jones@exa...| 32|     EAST|2025-03-04|
|       T000006| C69694|      P006|        5/5/2025|  85.0|COMPLETED|     Agus Aminah|agus.aminah@examp...| 48|  CENTRAL|2025-07-07|
|       T000007| C10729|      P009|        8/3/2025| 160.0|COMPLETED|    Laura Aminah|laura.aminah@exam...| 65|     EAST|2025-01-13|
|       T000008| C21622|      P003|       6/10/2025|-440.0|  PENDING|      Fajar Chen|fajar.chen@exampl...| 62|  CENTRAL|2025-03-08|
|       T000009| C44581|      P010|       12/7/2025| 560.0|COMPLETED|       Putri Lee|putri.lee@example...| 58|     EAST|2025-01-29|
|       T000010|  C8520|      P005|       6/27/2025| 105.0|COMPLETED| MICHAEL AMINAH |             Unknown| 45|     WEST|2025-04-14|
|       T000011|  C2958|      P006|        5/7/2025|  42.5|COMPLETED|    PUTRI JONES |             Unknown| 48|     WEST|2025-05-01|
|       T000012| C52154|      P008|       10/9/2025|  24.0|COMPLETED|      Daniel Doe|             Unknown| 28|     EAST|2025-06-09|
|       T000013| C36494|      P006|       12/4/2025|  85.0|COMPLETED|     Maya Wijaya|maya.wijaya@examp...| 60|     EAST|2025-04-29|
|       T000014| C30584|      P997|       10/1/2025|  35.0|COMPLETED|    Laura Aminah|laura.aminah@exam...| 18|     EAST|2025-02-27|
|       T000015| C48399|      P006|       1/28/2025| 127.5|CANCELLED|    James Wilson|james.wilson@exam...| 21|     EAST|2025-01-24|
|       T000016| C59854|      P009|        2/4/2025| 320.0|COMPLETED|     Kevin Smith|kevin.smith@examp...| 26|  Unknown|2025-05-09|
|       T000017| C27364|      P002|        7/6/2025|  12.5|COMPLETED|     Kevin Brown|kevin.brown@examp...| 31|  CENTRAL|2025-02-16|
|       T000018| C80444|      P997|       6/27/2025|  45.0|COMPLETED|    MARIA PUTRA | MARIA.PUTRA@EXAM...| 59|  CENTRAL|2025-04-05|
|       T000019| C78942|      P002|       3/15/2025|  25.0|COMPLETED|      Sari Brown|sari.brown@exampl...| 46|    NORTH|2025-06-27|
|       T000020| C13394|      P005|      12/20/2025| 105.0|  pending|     Arif Aminah|arif.aminah@examp...| 39|    NORTH|2025-07-20|
+--------------+-------+----------+----------------+------+---------+----------------+--------------------+---+---------+----------+
only showing top 20 rows
```

```python
df_customer_categorize = df_customer_clean.select(
    df_customer_clean.transaction_id,
    sf.when(df_customer_clean.amount < 0, "poor")
    .when((df_customer_clean.amount > 0) & (df_customer_clean.amount <= 500), "standart")
    .otherwise("good").alias("transaction_type")
)

merged_df = df_customer_clean.join(df_customer_categorize, on="transaction_id", how="left")
merged_df.show()
```

**Output:**
```text
+--------------+-------+----------+----------------+------+---------+----------------+--------------------+---+---------+----------+----------------+
|transaction_id|user_id|product_id|transaction_date|amount|   status|            name|               email|age|   region|      Date|transaction_type|
+--------------+-------+----------+----------------+------+---------+----------------+--------------------+---+---------+----------+----------------+
|       T000001| C42446|      P007|       9/17/2025| 216.0|COMPLETED|    Laura Taylor|laura.taylor@exam...| 38|     WEST|2025-06-04|        standart|
|       T000002| C28141|      P945|       1/31/2025| 320.0|COMPLETED|    Putri Taylor|putri.taylor@exam...| 21|  CENTRAL|2025-04-01|        standart|
|       T000003| C74116|      P004|        1/8/2026|  90.0|COMPLETED|        Budi Lee|budi.lee@example.com| 42|NORTHWEST|2025-07-21|        standart|
|       T000004|  C6106|      P003|        4/3/2025| 220.0|COMPLETED|     Sari Wilson|sari.wilson@examp...| 56|NORTHWEST|2025-04-01|        standart|
|       T000005| C13508|      P004|      12/15/2025|  90.0|CANCELLED|   Michael Jones|michael.jones@exa...| 32|     EAST|2025-03-04|        standart|
|       T000006| C69694|      P006|        5/5/2025|  85.0|COMPLETED|     Agus Aminah|agus.aminah@examp...| 48|  CENTRAL|2025-07-07|        standart|
|       T000007| C10729|      P009|        8/3/2025| 160.0|COMPLETED|    Laura Aminah|laura.aminah@exam...| 65|     EAST|2025-01-13|        standart|
|       T000008| C21622|      P003|       6/10/2025|-440.0|  PENDING|      Fajar Chen|fajar.chen@exampl...| 62|  CENTRAL|2025-03-08|            poor|
|       T000009| C44581|      P010|       12/7/2025| 560.0|COMPLETED|       Putri Lee|putri.lee@example...| 58|     EAST|2025-01-29|            good|
|       T000010|  C8520|      P005|       6/27/2025| 105.0|COMPLETED| MICHAEL AMINAH |             Unknown| 45|     WEST|2025-04-14|        standart|
|       T000011|  C2958|      P006|        5/7/2025|  42.5|COMPLETED|    PUTRI JONES |             Unknown| 48|     WEST|2025-05-01|        standart|
|       T000012| C52154|      P008|       10/9/2025|  24.0|COMPLETED|      Daniel Doe|             Unknown| 28|     EAST|2025-06-09|        standart|
|       T000013| C36494|      P006|       12/4/2025|  85.0|COMPLETED|     Maya Wijaya|maya.wijaya@examp...| 60|     EAST|2025-04-29|        standart|
|       T000014| C30584|      P997|       10/1/2025|  35.0|COMPLETED|    Laura Aminah|laura.aminah@exam...| 18|     EAST|2025-02-27|        standart|
|       T000015| C48399|      P006|       1/28/2025| 127.5|CANCELLED|    James Wilson|james.wilson@exam...| 21|     EAST|2025-01-24|        standart|
|       T000016| C59854|      P009|        2/4/2025| 320.0|COMPLETED|     Kevin Smith|kevin.smith@examp...| 26|  Unknown|2025-05-09|        standart|
|       T000017| C27364|      P002|        7/6/2025|  12.5|COMPLETED|     Kevin Brown|kevin.brown@examp...| 31|  CENTRAL|2025-02-16|        standart|
|       T000018| C80444|      P997|       6/27/2025|  45.0|COMPLETED|    MARIA PUTRA | MARIA.PUTRA@EXAM...| 59|  CENTRAL|2025-04-05|        standart|
|       T000019| C78942|      P002|       3/15/2025|  25.0|COMPLETED|      Sari Brown|sari.brown@exampl...| 46|    NORTH|2025-06-27|        standart|
|       T000020| C13394|      P005|      12/20/2025| 105.0|  pending|     Arif Aminah|arif.aminah@examp...| 39|    NORTH|2025-07-20|        standart|
+--------------+-------+----------+----------------+------+---------+----------------+--------------------+---+---------+----------+----------------+
only showing top 20 rows
```

---

# SQLite Database Storage

```python
# create connector
conn = sqlite3.connect('my_database.db')
cursor = conn.cursor()
```

```python
cursor.execute('CREATE TABLE IF NOT EXISTS customers_transaction (customer_id TEXT, name TEXT, email TEXT, age INTEGER, region TEXT, Date TEXT, transaction_type TEXT)')
```

**Output:**
```text
<sqlite3.Cursor at 0x7a5b8029cc40>
```

```python
pandas_df = merged_df.toPandas()
pandas_df.to_sql("customers", conn, if_exists="replace", index=False)
```

```python
cursor.execute('select * from customers limit 10')
print(cursor.fetchall())
# conn.close()
```


```python
# 1. Standardize and Cast Data Types
cleaned_df = merged_df \
    .withColumn("status_clean", sf.upper(sf.trim(sf.col("status")))) \
    .withColumn("region_clean", sf.upper(sf.trim(sf.col("region")))) \
    .withColumn("parsed_tx_date", sf.to_date(sf.col("transaction_date"), "M/d/yyyy"))
cleaned_df.show()
```


```python
# 2. Apply Rule Checks
EMAIL_REGEX = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
VALID_STATUSES = ["COMPLETED", "CANCELLED", "PENDING"]

validated_df = cleaned_df.withColumns({
    "check_amount": sf.col("amount").isNotNull() & (sf.col("amount") > 0),
    "check_age": sf.col("age").isNotNull() & (sf.col("age") >= 18) & (sf.col("age") <= 100),
    "check_email": sf.col("email").isNotNull() & sf.col("email").rlike(EMAIL_REGEX),
    "check_status": sf.col("status_clean").isin(VALID_STATUSES),
    "check_region": sf.col("region_clean").isNotNull() & ~sf.col("region_clean").isin(["UNKNOWN", ""]),
    "check_date": sf.col("parsed_tx_date").isNotNull()
})
validated_df.show()
```


```python# 3. Calculate Record Validity
final_df = validated_df.withColumn(
    "is_valid_record",
    sf.col("check_amount") & 
    sf.col("check_age") & 
    sf.col("check_email") & 
    sf.col("check_status") & 
    sf.col("check_region") & 
    sf.col("check_date")
)
final_df.show()
```


```python
# 4. Route Records into Valid Data and Quarantine Data
valid_records = final_df.filter(sf.col("is_valid_record") == True)

# 5. Generate Data Quality Summary Report
dq_summary = final_df.select(
    sf.count("*").alias("total_records"),
    sf.sum(sf.col("check_amount").cast("int")).alias("valid_amount_count"),
    sf.sum(sf.col("check_age").cast("int")).alias("valid_age_count"),
    sf.sum(sf.col("check_email").cast("int")).alias("valid_email_count"),
    sf.sum(sf.col("check_status").cast("int")).alias("valid_status_count"),
    sf.sum(sf.col("check_region").cast("int")).alias("valid_region_count"),
    sf.sum(sf.col("is_valid_record").cast("int")).alias("total_passed_records")
)

dq_summary.show()
```
