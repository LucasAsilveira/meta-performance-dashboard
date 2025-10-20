import os
import datetime
from google.cloud import bigquery
import pandas as pd

# Seleção de Data Dinâmica 
print("--- Selecione a data de referência para a análise ---")
print("1. Rodar para o mês atual")
print("2. Escolher um mês e ano específico (passado)")

while True:
    choice = input("Digite sua opção (1 ou 2): ")
    if choice in ['1', '2']:
        break
    print("Opção inválida. Por favor, digite 1 ou 2.")

if choice == '1':
    # Opção 1: Mês atual
    today = datetime.date.today()
    target_year_month = today.strftime('%Y-%m') # Formato 'YYYY-MM'
    print(f"\nAnálise será rodada para o mês atual: {target_year_month}\n")
else:
    # Opção 2: Mês/ano escolhido pelo usuário
    while True:
        try:
            month = int(input("Digite o mês (MM): "))
            year = int(input("Digite o ano (YYYY): "))
            # Valida a data para garantir que é um mês válido
            target_date = datetime.date(year, month, 1)
            target_year_month = target_date.strftime('%Y-%m')
            print(f"\nAnálise será rodada para: {target_year_month}\n")
            break
        except ValueError:
            print("Data inválida. Por favor, tente novamente. Ex: Mês: 05, Ano: 2024")

# Configurar caminhos
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
RAW_DIR = os.path.join(DATA_DIR, 'raw')

# Criar diretórios
os.makedirs(RAW_DIR, exist_ok=True)

# Conectar ao BigQuery
client = bigquery.Client(project='data-resources-448418')

# Query 1: Preços e disponibilidade
query_price = """
SELECT 
  listing,
  ROUND(AVG(IF(occupied = TRUE, price, NULL)), 2) AS media_preco_ocupado,
  ROUND(AVG(IF(
    blocked = FALSE 
    AND occupied = FALSE 
    AND date > CURRENT_DATE(),
    price_last_aquisition, 
    NULL
  )),2) AS media_preco_disponivel,
  COUNTIF(
    blocked = FALSE 
    AND occupied = FALSE 
    AND date > CURRENT_DATE()
  ) AS ocupacao_ainda_disponivel
FROM `data-resources-448418.revenuedata.daily_revenue_sapron` AS drs
INNER JOIN `data-resources-448418.saprondata.listing_status` AS ls
  ON drs.listing = ls.code
  AND ls.status = 'Active'
WHERE 
  DATE_TRUNC(date, MONTH) = DATE_TRUNC(@target_date, MONTH)
GROUP BY listing;
"""

# Query 2: Performance e meta
query_performance = """
WITH ultima_data AS (
  SELECT MAX(DATE(timestamp)) AS max_date
  FROM `data-resources-448418.meta.output_monthly`
  WHERE year_month = @target_year_month
),
registros_do_dia AS (
  SELECT 
    listing, 
    group_name, 
    num_listing_blocked, 
    n_days_status, 
    listing_fat, 
    n_competitors, 
    meta_value, 
    year_month, 
    to_listings, 
    to_competitors, 
    days_occupied, 
    total_days,
    timestamp,
    ROW_NUMBER() OVER (PARTITION BY listing ORDER BY timestamp DESC) AS rn
  FROM 
    `data-resources-448418.meta.output_monthly`
  WHERE 
    year_month = @target_year_month
    AND meta_result IS NOT NULL
    AND DATE(timestamp) = (SELECT max_date FROM ultima_data)
)

SELECT 
  listing, 
  group_name, 
  num_listing_blocked, 
  n_days_status, 
  ROUND(listing_fat, 2) AS listing_fat,
  n_competitors, 
  ROUND(meta_value, 2) AS meta_value,
  year_month, 
  ROUND(to_listings, 4) AS to_listings,
  ROUND(to_competitors, 4) AS to_competitors,
  days_occupied, 
  total_days
FROM registros_do_dia
WHERE rn = 1;
"""

# Query 3: Localização
query_location = """
SELECT  
  id_seazone as listing,
  MAX(IF(group_type = 'Carteira', group_name, NULL)) AS carteira,
  MAX(IF(group_type = 'Estado', group_name, NULL)) AS estado,
  MAX(IF(group_type = 'Cidade', group_name, NULL)) AS cidade,
  MAX(IF(group_type = 'Bairro', group_name, NULL)) AS Bairro
FROM `data-resources-448418.inputdata.setup_groups` 
WHERE 
  group_type IN ('Carteira', 'Estado', 'Cidade', 'Bairro')
  AND state = 'current'
GROUP BY id_seazone;
"""

# Query 4: Preço Mínimo (Pmin)
query_pmin = """
SELECT 
    listing, 
    n_dates_special_price
FROM "auditfatgoaldata-fkjh3fcvp0ed"."seazone_data"
WHERE 
    year_month = '2025-10' 
    and date_trunc('day', acquisition_date) >= current_date
"""


# Executar queries e salvar CSVs
queries = {
    'meta_analysis_price': query_price,
    'meta_analysis_performance_value_meta': query_performance,
    'meta_analysis_location': query_location
}

# Capturar data de execução
run_date = pd.to_datetime('today').normalize() 

# ==============================================================================
# Conectar à AWS Athena para a query de Pmin
# ==============================================================================
try:
    import boto3
    from pyathena import connect
    print("🔗 Conectando à AWS Athena...")
    
    session = boto3.Session(profile_name='sirius-prod')
    athena_connection = connect(
        s3_staging_dir='s3://aws-athena-query-results-835316524622-us-west-2/',
        profile_name='sirius-prod',
        region_name='us-west-2'
    )
    print("✅ Conexão com Athena estabelecida.")
    
    # Executar apenas a query de Pmin
    print(f"Executando query: meta_analysis_pmin")
    df_pmin = pd.read_sql(query_pmin, athena_connection)
    
    # Salvar arquivo com nome fixo
    output_path = os.path.join(RAW_DIR, 'meta_analysis_pmin.csv')
    df_pmin.to_csv(output_path, index=False)
    print(f"Salvo: {output_path}")

except ImportError:
    print("⚠️ Bibliotecas 'boto3' ou 'PyAthena' não encontradas. Pulando a query de Pmin.")
    print("Para instalar, execute: pip install boto3 'PyAthena[SQL]'")
except Exception as e:
    print(f"❌ Erro ao conectar ou executar a query de Pmin: {e}")
    print("Verifique suas credenciais AWS e o perfil 'sirius-prod'.")

# ==============================================================================
# FIM DA CONEXÃO AWS
# ==============================================================================


for name, query in queries.items():
    print(f"Executando query: {name}")
    job_config = bigquery.QueryJobConfig()
        
    if name == 'meta_analysis_price':
        job_config.query_parameters = [
            bigquery.ScalarQueryParameter("target_date", "DATE", f"{target_year_month}-01")
        ]
    elif name == 'meta_analysis_performance_value_meta':
        job_config.query_parameters = [
            bigquery.ScalarQueryParameter("target_year_month", "STRING", target_year_month)
        ]
    df = client.query(query, job_config=job_config).to_dataframe()
    df['data_da_execucao'] = run_date
    # Salvar arquivo
    output_path = os.path.join(RAW_DIR, f'{name}.csv')
    df.to_csv(output_path, index=False)
    print(f"Salvo: {output_path}")
    print(f"Registros: {len(df)}\n")

print("Processo concluído!")