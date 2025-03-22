# Importando as bibliotecas necessárias
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, min, max, sum, desc, regexp_extract
from pyspark.sql.functions import to_date
from pyspark.sql.functions import dayofweek

# Inicializando SparkSession
spark = SparkSession.builder.appName("LogAnalysis").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Carregando os dados
file_path = "/FileStore/tables/access_log.txt"
logs_df = spark.read.text(file_path)


# Definição das expressões regulares para cada campo
regex_patterns = {
    "ip": r"^(\S+)", 
    "timestamp": r"\[(.*?)\]",  
    "method": r"\"(\S+)",  
    "endpoint": r"\"[^\"]* (\S+) HTTP",  
    "status": r" (\d{3}) ",  
    "bytes": r" (\d+)$"  
}


# Aplicação das expressões para criar novas colunas
logs_df = logs_df \
    .withColumn("ip", regexp_extract("value", regex_patterns["ip"], 1)) \
    .withColumn("timestamp", regexp_extract("value", regex_patterns["timestamp"], 1)) \
    .withColumn("method", regexp_extract("value", regex_patterns["method"], 1)) \
    .withColumn("endpoint", regexp_extract("value", regex_patterns["endpoint"], 1)) \
    .withColumn("status", regexp_extract("value", regex_patterns["status"], 1)) \
    .withColumn("bytes", regexp_extract("value", regex_patterns["bytes"], 1).cast("int"))

# Removendo a coluna original para manter o DataFrame limpo
logs_df = logs_df.drop("value")

#Identifique as 10 maiores origens de acesso (Client IP):
logs_df.groupBy("ip").count() \
       .orderBy(desc("count"))\
       .limit(10) \
       .show()

#Liste os 6 endpoints mais acessados (desconsiderando arquivos):
# Filtrando endpoints sem extensão de arquivos
filtered_df = logs_df.filter(~col("endpoint").rlike(r"\.\w+$"))

filtered_df.groupBy("endpoint").count() \
           .orderBy(desc("count"))\
           .limit(6) \
           .show()

#Quantidade de Client IPs distintos:
logs_df.select("ip").distinct().count()

#Quantos dias de dados estão representados no arquivo:
logs_df.withColumn("date", to_date("timestamp", "dd/MMM/yyyy")) \
       .select("date").distinct().count()

#Análise dos volumes de dados (em bytes):
logs_df.agg(
    sum("bytes").alias("total_volume"),
    max("bytes").alias("max_volume"),
    min("bytes").alias("min_volume"),
    avg("bytes").alias("avg_volume")
).show()

#Dia da semana com maior número de erros
logs_df.filter(col("status").startswith("4")) \
       .withColumn("day_of_week", dayofweek(to_date("timestamp", "dd/MMM/yyyy"))) \
       .groupBy("day_of_week").count() \
       .orderBy(desc("count")) \
       .show()

logs_df.write.format("delta").mode("overwrite").save("/mnt/delta/logs_delta")