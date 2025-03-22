from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, min, max, sum, desc, regexp_extract, to_date, dayofweek, date_format, when
import matplotlib.pyplot as plt

# Inicializando SparkSession
spark = SparkSession.builder.appName("LogAnalysis").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Carregando os dados
file_path = "/FileStore/tables/access_log.txt"
delta_path = "/mnt/delta/logs_delta"  # Caminho para salvar a tabela Delta

regex_patterns = {
    "ip": r"^(\S+)",  
    "timestamp": r"\[(.*?)\]", 
    "method": r"\"(\S+)",  
    "endpoint": r"\"[^\"]* (\S+) HTTP",  
    "status": r" (\d{3}) ",  
    "bytes": r" (\d+)$" 
}
# **Função para testes unitários**
def test_data_extraction():
    # Dados de exemplo para testes
    test_logs = [
        "10.223.157.186 - - [15/Jul/2009:14:58:59 -0700] \"GET / HTTP/1.1\" 200 123",
        "192.168.0.1 - - [16/Jul/2009:16:14:32 -0700] \"POST /login HTTP/1.1\" 404 456",
        "172.16.0.2 - - [17/Jul/2009:10:45:00 -0700] \"GET /dashboard HTTP/1.1\" 500 789"
    ]
    
    # Criando DataFrame de teste
    test_df = spark.createDataFrame(test_logs, "string").toDF("value")
    
    # Aplicando regex para extrair os campos
    extracted_df = test_df \
        .withColumn("ip", regexp_extract("value", regex_patterns["ip"], 1)) \
        .withColumn("timestamp", regexp_extract("value", regex_patterns["timestamp"], 1)) \
        .withColumn("method", regexp_extract("value", regex_patterns["method"], 1)) \
        .withColumn("endpoint", regexp_extract("value", regex_patterns["endpoint"], 1)) \
        .withColumn("status", regexp_extract("value", regex_patterns["status"], 1)) \
        .withColumn("bytes", regexp_extract("value", regex_patterns["bytes"], 1).cast("int"))
    
    # Verificando resultados esperados
    expected_results = [
        {"ip": "10.223.157.186", "timestamp": "15/Jul/2009:14:58:59 -0700", "method": "GET", "endpoint": "/", "status": "200", "bytes": 123},
        {"ip": "192.168.0.1", "timestamp": "16/Jul/2009:16:14:32 -0700", "method": "POST", "endpoint": "/login", "status": "404", "bytes": 456},
        {"ip": "172.16.0.2", "timestamp": "17/Jul/2009:10:45:00 -0700", "method": "GET", "endpoint": "/dashboard", "status": "500", "bytes": 789}
    ]
    
    for idx, row in enumerate(extracted_df.collect()):
        assert row["ip"] == expected_results[idx]["ip"], f"Erro no campo 'ip' para a linha {idx + 1}"
        assert row["timestamp"] == expected_results[idx]["timestamp"], f"Erro no campo 'timestamp' para a linha {idx + 1}"
        assert row["method"] == expected_results[idx]["method"], f"Erro no campo 'method' para a linha {idx + 1}"
        assert row["endpoint"] == expected_results[idx]["endpoint"], f"Erro no campo 'endpoint' para a linha {idx + 1}"
        assert row["status"] == expected_results[idx]["status"], f"Erro no campo 'status' para a linha {idx + 1}"
        assert row["bytes"] == expected_results[idx]["bytes"], f"Erro no campo 'bytes' para a linha {idx + 1}"
    
    print("Todos os testes foram aprovados!")

# Testando a extração
test_data_extraction()

# **Processamento dos Logs**
logs_df = spark.read.text(file_path)  
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
logs_df.write.format("delta").mode("overwrite").save(delta_path)
df_delta = spark.read.format("delta").load(delta_path)


#Identifique as 10 maiores origens de acesso (Client IP):
top_ips = df_delta.groupBy("ip").count().orderBy(desc("count")).limit(10)
top_ips.show()

# Criar um gráfico com os 10 IPs mais frequentes
ip_counts = top_ips.toPandas()
plt.bar(ip_counts["ip"], ip_counts["count"])
plt.xlabel("IP")
plt.ylabel("Acessos")
plt.title("Top 10 IPs mais frequentes")
plt.xticks(rotation=90)
plt.show()

#Liste os 6 endpoints mais acessados (desconsiderando arquivos):
# Filtrando endpoints sem extensão de arquivos
filtered_df = df_delta.filter(~col("endpoint").rlike(r"\.\w+$"))
filtered_df.groupBy("endpoint").count().orderBy(desc("count")).limit(6).show()

#Quantidade de Client IPs distintos
distinct_ips = df_delta.select("ip").distinct().count()
print(f"Quantidade de IPs distintos: {distinct_ips}")

#Quantos dias de dados estão representados no arquivo:
days_count = df_delta.withColumn("date", to_date("timestamp", "dd/MMM/yyyy")).select("date").distinct().count()
print(f"Dias distintos no arquivo: {days_count}")

#Análise dos volumes de dados (em bytes):

# Adicionar uma coluna categorizando os tipos de resposta baseados nos códigos de status HTTP
df_delta = df_delta.withColumn(
    "response_category",
    when(col("status").startswith("2"), "Success") 
    .when(col("status").startswith("3"), "Redirect")  
    .when(col("status").startswith("4"), "Client Error")  
    .when(col("status").startswith("5"), "Server Error")  
    .otherwise("Unknown")  
)

analysis_result = df_delta.groupBy("response_category").agg(
    sum("bytes").alias("total_volume"),  
    max("bytes").alias("max_volume"),  
    min("bytes").alias("min_volume"), 
    avg("bytes").alias("avg_volume"))
analysis_result.show()


#Dia da semana com maior número de erros
df_delta.filter(col("status").startswith("4")) \
        .withColumn("day_of_week", date_format(to_date("timestamp", "dd/MMM/yyyy"), "EEEE")) \
        .groupBy("day_of_week").count().orderBy(desc("count")).limit(1).show()
