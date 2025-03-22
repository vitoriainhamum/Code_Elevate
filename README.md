Análise de Logs com Apache Spark
Este projeto processa, analisa e visualiza dados de logs de servidor para extrair insights como os principais IPs de cliente, endpoints mais acessados e volumes de resposta, utilizando Apache Spark e Delta Lake para processamento distribuído de dados.
Siga as instruções abaixo para configurar, executar o projeto e realizar as análises descritas.

Pré-requisitos
Para executar o projeto, certifique-se de ter os seguintes itens instalados em sua máquina:

Python (>= 3.7)

Apache Spark (>= 3.0)

Delta Lake (configurado com o Spark)

Matplotlib para visualização de dados

Baixe o arquivo de log: 
Faça o download do arquivo de log a partir do link: https://drive.google.com/file/d/1p5fWpn53iuMZx06uRM9VEwRgAdgjDCii/view?usp=sharing

Coloque o arquivo de log: 
Salve o arquivo baixado no diretório /FileStore/tables/ se estiver executando localmente, ou carregue-o no seu ambiente Databricks.

Execução no Databricks
Crie um Cluster:

Acesse o ambiente Databricks.

1. Crie um novo cluster com Spark versão >= 3.0 e Delta Lake instalado.
    - Carregue o Código:

2. Importe o arquivo .py fornecido ou copie-o para um novo notebook Databricks.
    - Carregue o Arquivo de Log:

3. Navegue para a seção Data no Databricks e faça o upload do arquivo de log (access_log.txt) em /FileStore/tables/.
    - Execute o Notebook:

4. Execute cada célula do notebook sequencialmente.
    - Resultados:

5. Visualize as saídas diretamente na interface do Databricks.O programa analisa os logs e gera:
    - Top 10 IPs de cliente e um gráfico de barras visualizando suas contagens de acesso.
    - 6 Endpoints mais acessados, excluindo requisições de arquivos estáticos.
    - Número de IPs de cliente únicos.
    - Número de dias distintos presentes nos logs.
    - Análise dos volumes de resposta, categorizados por sucesso, redirecionamentos, erros de cliente e erros de servidor.
    - Dia da Semana com maior número de erros do tipo cliente.

Tecnologias Utilizadas:
    - Apache Spark: Framework distribuído para processamento rápido de dados.
    - Delta Lake: Expande o Spark com transações ACID e armazenamento escalável.
    - Python: Linguagem principal para implementação.
    - Matplotlib: Biblioteca para visualização de dados.

Por Que Usar Estas Tecnologias?

Apache Spark:
    - Perfeito para processamento de grandes volumes de dados de forma distribuída.
    - Fornece bibliotecas integradas para manipulação de dados estruturados e semi-estruturados.

Delta Lake:
    - Garantia de consistência dos dados com transações ACID.
    - Suporte a versionamento e consultas avançadas.

Matplotlib:
    - Facilita a criação de gráficos simples e compreensíveis para visualização de métricas.

Autora
Vitória Inhamum
