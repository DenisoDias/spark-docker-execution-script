<H2>Projeto para executar um script em Spark</H2>

<h3>
É necessário somente substituir o arquivo main.py

Para executar é necessário utilizar os seguintes comandos:</h3>

Esse comando deve ser executado no mesmo diretório do arquivo docker-compose.yml, 
ele fará o download das imagens utilizadas e colocará o ambiente no ar 
> docker-compose up

Executar o seguinte comando para executar o script dentro do container:
> docker exec ess_spark_1 spark-submit /app/main.py