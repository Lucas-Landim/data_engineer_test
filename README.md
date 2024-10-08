﻿# data_engineer_test

# Projeto Airflow para ETL e Análise de Dados

Este projeto utiliza o Apache Airflow para automatizar processos de ETL e análise de dados utilizando dados de uma API de câmbio e informações financeiras armazenadas em um banco de dados PostgreSQL.

## Visão Geral

O projeto é composto por duas DAGs principais que realizam diferentes tarefas para processar e analisar os dados.

### DAG 1: `alimenta_banco_recursos_dag`

Esta DAG é responsável por amazenar os dados no banco com base os dois dataframes de despesas e receitas do estados.

1. **Cria tabela no banco de dados**:
  - Verifica e cria a tabela fonte_recursos no banco de dados PostgreSQL, caso ela ainda não exista. Esta tabela é usada para armazenar dados de fontes de recursos, incluindo o total liquidado e o total arrecadado para cada fonte.

2. **Processar arquivos CSV**
   - Lê dois arquivos CSV (gdvDespesasExcel.csv e gdvReceitasExcel.csv) que contêm informações de despesas e receitas.
   - Realiza a transformação dos dados dos CSVs para extrair informações relevantes, como o ID da fonte de recursos, nome da fonte, total liquidado (despesas) e total arrecadado (receitas).
   - Agrupa os dados por fonte de recursos e calcula os totais para cada fonte.

3. **Inserir Dados no banco de dados**
   - Insere os dados combinados na tabela fonte_recursos no PostgreSQL, garantindo que cada registro seja enriquecido com uma marcação de tempo (dt_insert) indicando quando o registro foi inserido.
   
### DAG 2: `api_postgres`

Esta DAG é responsável por extrair dados de câmbio de uma API pública e inseri-los em uma tabela no banco.

1. **Extrair dados da API**:
   - Faz uma requisição HTTP GET para a API AwesomeAPI para obter a taxa de câmbio USD/BRL do dia 22/06/2022.
   
2. **Criar tabela no banco de dados**:
   - Verifica e cria a tabela `cambio_do_dia` no banco, caso ela ainda não exista.

## Consultas SQL Requisitadas

```sql
--Quais são as 5 fontes de recursos que mais arrecadaram?
WITH cte_recursos AS (
    SELECT
         f.id_fonte_recurso
        ,f.nome_fonte_recurso
        ,f.total_liquidado
        ,f.total_arrecadado
        ,c.alto
        ,c.lance
        ,(f.total_arrecadado * c.alto) AS total_arrecadado_convertido
    FROM fonte_recursos AS f
    LEFT JOIN cambio_do_dia AS c ON true
)

SELECT
     id_fonte_recurso
	,nome_fonte_recurso
	,total_arrecadado_convertido
FROM cte_recursos
ORDER BY total_arrecadado_convertido DESC
LIMIT 5;


--Quais são as 5 fontes de recursos que mais gastaram?
WITH cte_recursos AS (
    SELECT
         f.id_fonte_recurso
        ,f.nome_fonte_recurso
        ,f.total_liquidado
        ,f.total_arrecadado
        ,c.alto
        ,c.lance
        ,(f.total_liquidado * c.alto) AS total_liquidado_convertido
        ,(f.total_arrecadado * c.alto) AS total_arrecadado_convertido
    FROM fonte_recursos AS f
    LEFT JOIN cambio_do_dia AS c ON true
)

SELECT
     id_fonte_recurso
	,nome_fonte_recurso
	,total_liquidado_convertido
FROM cte_recursos
ORDER BY total_liquidado_convertido DESC
LIMIT 5;

--Quais são as 5 fontes de recursos com a melhor margem bruta?
WITH cte_recursos AS (
    SELECT
         f.id_fonte_recurso
        ,f.nome_fonte_recurso
        ,f.total_liquidado
        ,f.total_arrecadado
        ,c.alto
        ,c.lance
        ,(f.total_arrecadado * c.alto) AS total_arrecadado_convertido
        ,(f.total_liquidado * c.alto) AS total_liquidado_convertido
    FROM fonte_recursos AS f
    LEFT JOIN cambio_do_dia AS c ON true
)

SELECT
     id_fonte_recurso
    ,nome_fonte_recurso
    ,(total_arrecadado_convertido - total_liquidado_convertido) AS margem_bruta
FROM cte_recursos
ORDER BY margem_bruta DESC
LIMIT 5;

--Quais são as 5 fontes de recursos que menir arrecadaram?
WITH cte_recursos AS (
    SELECT
         f.id_fonte_recurso
        ,f.nome_fonte_recurso
        ,f.total_liquidado
        ,f.total_arrecadado
        ,c.alto
        ,c.lance
        ,(f.total_arrecadado * c.alto) AS total_arrecadado_convertido
    FROM fonte_recursos AS f
    LEFT JOIN cambio_do_dia AS c ON true
)

SELECT
     id_fonte_recurso
	,nome_fonte_recurso
	,total_arrecadado_convertido
FROM cte_recursos
ORDER BY total_arrecadado_convertido ASC
LIMIT 5;

--Quais são as 5 fontes de recursos que menir gastaram?
WITH cte_recursos AS (
    SELECT
         f.id_fonte_recurso
        ,f.nome_fonte_recurso
        ,f.total_liquidado
        ,f.total_arrecadado
        ,c.alto
        ,c.lance
        ,(f.total_liquidado * c.alto) AS total_liquidado_convertido
        ,(f.total_arrecadado * c.alto) AS total_arrecadado_convertido
    FROM fonte_recursos AS f
    LEFT JOIN cambio_do_dia AS c ON true
)

SELECT
     id_fonte_recurso
	,nome_fonte_recurso
	,total_liquidado_convertido
FROM cte_recursos
ORDER BY total_liquidado_convertido ASC
LIMIT 5;

--Quais são as 5 fontes de recursos com a pior margem bruta?
WITH cte_recursos AS (
    SELECT
         f.id_fonte_recurso
        ,f.nome_fonte_recurso
        ,f.total_liquidado
        ,f.total_arrecadado
        ,c.alto
        ,c.lance
        ,(f.total_arrecadado * c.alto) AS total_arrecadado_convertido
        ,(f.total_liquidado * c.alto) AS total_liquidado_convertido
    FROM fonte_recursos AS f
    LEFT JOIN cambio_do_dia AS c ON true
)

SELECT
     id_fonte_recurso
    ,nome_fonte_recurso
    ,(total_arrecadado_convertido - total_liquidado_convertido) AS margem_bruta
FROM cte_recursos
ORDER BY margem_bruta ASC
LIMIT 5;

--Qual a média de arrecadação por fonte de recurso?
WITH cte_recursos AS (
    SELECT
         f.id_fonte_recurso
        ,f.nome_fonte_recurso
        ,f.total_liquidado
        ,f.total_arrecadado
        ,c.alto
        ,c.lance
        ,(f.total_arrecadado * c.alto) AS total_arrecadado_convertido
    FROM fonte_recursos AS f
    LEFT JOIN cambio_do_dia AS c ON true
)

SELECT
	 id_fonte_recurso
    ,nome_fonte_recurso
    ,AVG(total_arrecadado_convertido) AS media_arrecadacao 
FROM cte_recursos
GROUP BY id_fonte_recurso, nome_fonte_recurso
ORDER BY media_arrecadacao DESC;

--Qual a média de gastos por fonte de recurso?
WITH cte_recursos AS (
    SELECT
         f.id_fonte_recurso
        ,f.nome_fonte_recurso
        ,f.total_liquidado
        ,f.total_arrecadado
        ,c.alto
        ,c.lance
        ,(f.total_liquidado * c.alto) AS total_liquidado_convertido
    FROM fonte_recursos AS f
    LEFT JOIN cambio_do_dia AS c ON true
)

SELECT
	 id_fonte_recurso
	,nome_fonte_recurso
    ,AVG(total_liquidado_convertido) AS media_liquidado
FROM cte_recursos
GROUP BY id_fonte_recurso, nome_fonte_recurso
ORDER BY media_liquidado DESC;
