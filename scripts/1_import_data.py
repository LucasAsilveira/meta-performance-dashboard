import os
from google.cloud import bigquery
import pandas as pd

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
  DATE_TRUNC(date, MONTH) = DATE_TRUNC(CURRENT_DATE(), MONTH)
GROUP BY listing;
"""

# Query 2: Performance e meta
query_performance = """
WITH ultima_data AS (
  SELECT MAX(DATE(timestamp)) AS max_date
  FROM `data-resources-448418.meta.output_monthly`
  WHERE year_month = '2025-09'
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
    year_month = '2025-09'
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

# Executar queries e salvar CSVs
queries = {
    'meta_analysis_price': query_price,
    'meta_analysis_performance_value_meta': query_performance,
    'meta_analysis_location': query_location
}

for name, query in queries.items():
    print(f"Executando query: {name}")
    df = client.query(query).to_dataframe()
    
    # Salvar arquivo
    output_path = os.path.join(RAW_DIR, f'{name}.csv')
    df.to_csv(output_path, index=False)
    print(f"Salvo: {output_path}")
    print(f"Registros: {len(df)}\n")

print("Processo concluído!")