from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("OlistBigData").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BUCKET = "s3://proyecto-bigdata-olist"

orders = spark.read.csv(f"{BUCKET}/raw/olist_orders_dataset.csv", header=True, inferSchema=False)
items = spark.read.csv(f"{BUCKET}/raw/olist_order_items_dataset.csv", header=True, inferSchema=False)
reviews = spark.read.csv(f"{BUCKET}/raw/olist_order_reviews_dataset.csv", header=True, inferSchema=False)
customers = spark.read.csv(f"{BUCKET}/raw/olist_customers_dataset.csv", header=True, inferSchema=False)
products = spark.read.csv(f"{BUCKET}/raw/olist_products_dataset.csv", header=True, inferSchema=False)

items = items.withColumn("price", F.expr("try_cast(price as double)")).withColumn("freight_value", F.expr("try_cast(freight_value as double)"))
reviews = reviews.withColumn("review_score", F.expr("try_cast(review_score as double)"))

items_agg = items.filter((F.col("price").isNotNull()) & (F.col("price") >= 0)).groupBy("order_id").agg(
    F.sum("price").alias("total_price"),
    F.sum("freight_value").alias("total_freight"),
    F.first("product_id").alias("product_id")
)

reviews_agg = reviews.filter(F.col("review_score").isNotNull()).groupBy("order_id").agg(
    F.avg("review_score").alias("review_score_avg")
)

df = orders.filter(F.col("order_id").isNotNull())
df = df.join(customers.select("customer_id","customer_city","customer_state"), on="customer_id", how="left")
df = df.join(items_agg, on="order_id", how="left")
df = df.join(products.select("product_id","product_category_name"), on="product_id", how="left")
df = df.join(reviews_agg, on="order_id", how="left")

df = df.withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp", "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("order_delivered_customer_date", F.to_timestamp("order_delivered_customer_date", "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("order_estimated_delivery_date", F.to_timestamp("order_estimated_delivery_date", "yyyy-MM-dd HH:mm:ss"))

df = df.withColumn("entrega_tarde",
    F.when(F.col("order_delivered_customer_date").isNull(), None)
    .when(F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date"), 1)
    .otherwise(0))

df = df.withColumn("dias_retraso",
    F.when(F.col("order_delivered_customer_date").isNull(), None)
    .otherwise(F.datediff("order_delivered_customer_date", "order_estimated_delivery_date")))

df = df.withColumn("anio_mes_compra", F.date_format("order_purchase_timestamp", "yyyy-MM"))
df = df.withColumn("categoria",
    F.when(F.col("product_category_name").isNull(), "sin_categoria")
    .otherwise(F.lower(F.col("product_category_name"))))

df_final = df.select(
    "order_id","customer_id","order_status",
    "order_purchase_timestamp","order_delivered_customer_date","order_estimated_delivery_date",
    "customer_state","customer_city","categoria",
    "total_price","total_freight","review_score_avg",
    "entrega_tarde","dias_retraso","anio_mes_compra"
)

df_final.write.mode("overwrite").parquet(f"{BUCKET}/analytics/tabla_final/")
print("COMPLETADO OK")
spark.stop()
