from pyspark.sql import SparkSession
from pyspark import SparkContext
spark = SparkSession.builder \
  .appName('clean_products') \
  .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar') \
  .getOrCreate()

spark.conf.set("spark.sql.repl.eagerEval.enable",True)

table_amazon_daily = "amazon_daily_updates.compras"
amazon_daily = spark.read \
  .format("bigquery") \
  .option("table", table_amazon_daily) \
  .load()
amazon_daily.printSchema()

from pyspark.sql.functions import current_date, date_sub

amazon_daily_2=amazon_daily.filter(amazon_daily['fecha_compra']==date_sub(current_date(),1))

amazon_daily_3=amazon_daily_2.withColumn("isprime",amazon_daily_2.isprime.cast('boolean')) \
    .withColumnRenamed("id","compra_id")\
    .withColumn("cantidad",amazon_daily_2.cantidad.cast("int"))\
    .withColumn("fecha_compra",amazon_daily_2.fecha_compra.cast("TIMESTAMP"))


amazon_daily_3.write \
  .format("bigquery") \
  .option("table","becade_rgarciaf.stg_compras") \
  .option("temporaryGcsBucket", "amazon_bucket_ramiro") \
  .mode('append') \
  .save()