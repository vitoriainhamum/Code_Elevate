from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, min, max, sum, desc, regexp_extract, to_date, dayofweek, date_format, when
import matplotlib.pyplot as plt

# Inicializando SparkSession
spark = SparkSession.builder.appName("LogAnalysis").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Carregando os dados
file_path = "/FileStore/tables/access_log.txt"
delta_path = "/mnt/delta/logs_delta"  # Caminho para salvar a tabela Delta

# Definição das expressões regulares para cada campo
regex_patterns = {
    "ip": r"^(\S+)",  
    "timestamp": r"\[(.*?)\]", 
    "method": r"\"(\S+)",  
    "endpoint": r"\"[^\"]* (\S+) HTTP",  
    "status": r" (\d{3}) ",  
    "bytes": r" (\d+)$" 
}

logs_df = spark.read.text(file_path)

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

# Gravando os dados processados como tabela Delta
logs_df.write.format("delta").mode("overwrite").save(delta_path) #Salvando tabela delta
df_delta = spark.read.format("delta").load(delta_path) #Lendo tabela delta

#Identifique as 10 maiores origens de acesso (Client IP):
df_delta.groupBy("ip").count() \
        .orderBy(desc("count")) \
        .limit(10) \
        .show()

# Criar um gráfico com os 10 IPs mais frequentes
ip_counts = logs_df.groupBy("ip").count().orderBy(desc("count")).limit(10).toPandas()
plt.bar(ip_counts["ip"], ip_counts["count"])
plt.xlabel("IP")
plt.ylabel("Acessos")
plt.title("Top 10 IPs mais frequentes")
plt.xticks(rotation=90)
plt.show()

#Liste os 6 endpoints mais acessados (desconsiderando arquivos):
# Filtrando endpoints sem extensão de arquivos
filtered_df = df_delta.filter(~col("endpoint").rlike(r"\.\w+$"))  

filtered_df.groupBy("endpoint").count() \
           .orderBy(desc("count")) \
           .limit(6) \
           .show()

#Quantidade de Client IPs distintos
distinct_ips = df_delta.select("ip").distinct().count()
print(f"Quantidade de IPs distintos: {distinct_ips}")

#Quantos dias de dados estão representados no arquivo:
df_delta.withColumn("date", to_date("timestamp", "dd/MMM/yyyy")) \
        .select("date").distinct().count()

#Análise dos volumes de dados (em bytes):
df_delta.agg(
    sum("bytes").alias("total_volume"),
    max("bytes").alias("max_volume"),
    min("bytes").alias("min_volume"),
    avg("bytes").alias("avg_volume")
).show()

#Dia da semana com maior número de erros
df_delta.filter(col("status").startswith("4")) \
        .withColumn("day_of_week", date_format(to_date("timestamp", "dd/MMM/yyyy"), "EEEE")) \
        .groupBy("day_of_week").count() \
        .orderBy(desc("count")) \
        .show()