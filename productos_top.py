from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, row_number, desc, date_format
from pyspark.sql.window import Window

# --- CONFIGURACION DE RUTAS ---
# Se utiliza la IP privada del Master: 172.31.30.219
master_url = "spark://172.31.30.219:7077"

# Inicializar SparkSession
spark = SparkSession.builder \
    .appName("Top Products") \
    .master(master_url) \
    .config("spark.jars", "/home/ec2-user/mysql-connector-j-8.0.33.jar,/home/ec2-user/hadoop-aws-3.3.4.jar,/home/ec2-user/aws-java-sdk-bundle-1.12.262.jar") \
    .getOrCreate()

# --- CREDENCIALES AWS (YA ENTIENDO QUE NO ES BUENA PRACTICA HARDCODEARLOS ASI PERO COMO NO PARAN DE RENOVARSE CUANDO SE REINICIA Y NO QUIERO COMPLICARME LO HAGO ASI) )
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", "ASIATOCVTWJH6N4XTRDE")
hadoop_conf.set("fs.s3a.secret.key", "lO1uI4HbBWF1gLjOq2VKAJcR/byTNWp+tfadRfkW")
hadoop_conf.set("fs.s3a.session.token", "IQoJb3JpZ2luX2VjEPH//////////wEaCXVzLXdlc3QtMiJHMEUCIQCcvHOxFacDeI8IqOZDgaeXZKiYkmPOJoib1U7bqg8BigIgQbL5V+98rQ8qDAobPPoDSqBpppEwn/7I+Gckiof3gToquAIIuv//////////ARAAGgwyMzY0MDI3NTAwMzEiDJlZaiMidqD5bKdo6CqMAoILDoBwQ88yOab9rgmcu0g1JQCFBiEKzBp0MemYnxpb3VAgYzn8I7gEEgdkqIOEjRothssbOdnIa/F56a7BJRpmMux03f8q5XnQMpgFuP/HgbfECZeVTJgtArh5uI/IJRGrFSpLbJiPb+3cXX/SwnE38JLHLIOSoPoWCOG1Tq/0RVzygS+q2ISq/X6os8mj5dk6WhyhJbjO6DI6sKQSpCUmHHwNeidNOCDxQ6+G7cIzKTERhX1B0VSek6dS/T/28UuHBpH+A4YTTX3xBHTDZSUMqxjgKKeZhB7V3spztKntOg8GpwfZmXiSL4FXb8j+ItpZ01y16+ShxrR5zijHtmucP8WHwLFZobY3jmYwtYexzAY6nQFt2f0NYRbX8Cf+jU7bSGGLBy2UUEmTYr6mP13LA7/s6WPj+ByPJ6QXQKA5HZzaPZSaXjql2NK+VyUR8PqdauZFJn/x+cFXofvPFVwddic3/q8q31jrOEqPyF4HnzC3wALIeNcOk2QxC6n2llWMYy2KV853mmTD4BU3jKmMp27yOHQNwGLEZLFn5q8nwnB7GuV0O9Da4V/Y0ZEH9wTv")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")

# --- LECTURA DE DATOS DESDE S3 ---
orders = spark.read.csv("s3a://mybucket-comercio360/comercio360/raw/orders.csv", header=True, inferSchema=True)
order_items = spark.read.csv("s3a://mybucket-comercio360/comercio360/raw/order_items.csv", header=True, inferSchema=True)

# --- PROCESAMIENTO ---
joined = orders.join(order_items, "order_id") \
    .withColumn("importe_total", col("quantity") * col("unit_price")) \
    .withColumn("fecha", date_format(col("order_date"), "yyyy-MM-dd"))

daily = joined.groupBy("fecha", "product_id") \
    .agg(sum("quantity").alias("unidades"), sum("importe_total").alias("importe_total"))

window = Window.partitionBy("fecha").orderBy(desc("importe_total"))
top = daily.withColumn("ranking", row_number().over(window)) \
    .filter(col("ranking") <= 10)

# --- ESCRITURA EN RDS Y S3 (OTRA VEZ HARDCODEO..) ---
jdbc_url = "jdbc:mysql://myrds.c0uzhxteqgwo.us-west-2.rds.amazonaws.com:3306/comercio360"
props = {
    "user": "admin", 
    "password": "password", 
    "driver": "com.mysql.cj.jdbc.Driver"
}

print("Escribiendo resultados en la base de datos RDS...")
top.write.jdbc(url=jdbc_url, table="top_products", mode="overwrite", properties=props)

print("Escribiendo resultados en S3...")
top.write.mode("overwrite").csv("s3a://mybucket-comercio360/comercio360/resultado_top_productos/")

top.explain(True)

print("Proceso finalizado con éxito.")
time.sleep(300)
spark.stop()