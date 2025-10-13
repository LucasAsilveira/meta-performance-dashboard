import pandas as pd
import os
import numpy as np

# Configurar caminhos
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
RAW_DIR = os.path.join(DATA_DIR, 'raw')
PROCESSED_DIR = os.path.join(DATA_DIR, 'processed')

# Criar diretórios se não existirem
os.makedirs(PROCESSED_DIR, exist_ok=True)

print("🚀 Iniciando preparação dos dados...")
print(f"📂 Lendo arquivos de: {RAW_DIR}")
print(f"💾 Salvando resultados em: {PROCESSED_DIR}")

# %%
# Ler os 3 arquivos CSV
try:
    df_location = pd.read_csv(os.path.join(RAW_DIR, "meta_analysis_location.csv"))
    df_meta = pd.read_csv(os.path.join(RAW_DIR, "meta_analysis_performance_value_meta.csv"))
    df_prices = pd.read_csv(os.path.join(RAW_DIR, "meta_analysis_price.csv"))
    print("✅ Todos os arquivos CSV lidos com sucesso")
except FileNotFoundError as e:
    print(f"❌ Erro ao ler arquivos: {e}")
    print("Execute primeiro o script 1_import_data.py")
    exit(1)

# %%
# Renomear colunas do df_meta conforme o padrão
df_meta = df_meta.rename(columns={
    "group_name": "categoria",
    "num_listing_blocked": "dias_bloqueados",
    "n_days_status": "dias_ativo",
    "listing_fat": "faturamento_mes",
    "n_competitors": "n_concorrentes",
    "meta_value": "meta",
    "year_month": "mes_ano",
    "to_competitors": "to_concorrentes",
    "days_occupied": "dias_ocupados",
    "total_days": "total_dias"
})

print("📝 Colunas renomeadas no df_meta")

# %%
# Verificar nomes das colunas
print("\n📌 Colunas de df_location:")
print(df_location.columns.tolist())

print("\n📌 Colunas de df_meta:")
print(df_meta.columns.tolist())

print("\n📌 Colunas de df_prices:")
print(df_prices.columns.tolist())

# %%
# Fazer merge entre os DataFrames
print("\n🔗 Realizando merges dos DataFrames...")
# Primeiro, juntar df_meta com df_prices
df_merged = df_meta.merge(df_prices, on="listing", how="left")

# Depois, juntar com df_location
df_final = df_merged.merge(df_location, on="listing", how="left")
print(df_final.columns)
print(f"✅ Merge concluído")
print(f" - df_meta: {len(df_meta)} linhas")
print(f" - df_final: {len(df_final)} linhas")

# %%
# Reordenar colunas
colunas_ordenadas = [
    "listing",
    "categoria",
    "carteira",
    "estado",
    "cidade",
    "Bairro",
    "dias_bloqueados",
    "dias_ativo",
    "faturamento_mes",
    "n_concorrentes",
    "meta",
    "mes_ano",
    "to_listings",
    "to_concorrentes",
    "dias_ocupados",
    "total_dias",
    "media_preco_ocupado",
    "media_preco_disponivel",
    "ocupacao_ainda_disponivel",
    "data_da_execucao"
]

# Manter apenas colunas que existem no df_final
colunas_ordenadas = [col for col in colunas_ordenadas if col in df_final.columns]
df_final = df_final[colunas_ordenadas]
print(df_final.columns)
print("📋 Colunas reordenadas")

# %%
# Calcular atingimento da meta (evitar divisão por zero)
print("\n📊 Calculando métricas derivadas...")
df_final["atingimento_meta"] = df_final.apply(
    lambda row: row["faturamento_mes"] / row["meta"] if row["meta"] > 0 else 0,
    axis=1
)

# %%
# Calcular métricas de impacto financeiro
print("\n💰 Calculando métricas de impacto financeiro...")

# Garantir que as colunas sejam numéricas para o cálculo, tratando erros
df_final['dias_bloqueados'] = pd.to_numeric(df_final['dias_bloqueados'], errors='coerce').fillna(0)
df_final['media_preco_ocupado'] = pd.to_numeric(df_final['media_preco_ocupado'], errors='coerce').fillna(0)
df_final['faturamento_mes'] = pd.to_numeric(df_final['faturamento_mes'], errors='coerce').fillna(0)
df_final['meta'] = pd.to_numeric(df_final['meta'], errors='coerce').fillna(0)

# Calcular Faturamento Perdido por Bloqueio
df_final['faturamento_perdido_bloqueio'] = df_final['dias_bloqueados'] * df_final['media_preco_ocupado']

# Calcular Falta para a Meta
df_final['falta_meta'] = df_final['meta'] - df_final['faturamento_mes']

print("✅ Métricas de impacto financeiro calculadas")

# Classificar grupo de criticidade
def classificar_criticidade(atingimento):
    if atingimento <= 0.5:
        return "crítico"
    elif atingimento <= 0.8:
        return "atenção"
    elif atingimento <= 1.1:
        return "berlinda"
    elif atingimento <= 2.0:
        return "ok"
    else:
        return "meta_subestimada"

df_final["grupo_criticidade"] = df_final["atingimento_meta"].apply(classificar_criticidade)

# Arredondar atingimento para 2 casas
df_final["atingimento_meta"] = df_final["atingimento_meta"].round(2)

print("✅ Métricas derivadas calculadas")

# %%
# Calcular métricas para a Berlinda
print("\n🎯 Calculando métricas específicas para Berlinda...")

# Filtrar apenas Berlinda
df_berlinda = df_final[df_final["grupo_criticidade"] == "berlinda"].copy()

if len(df_berlinda) > 0:

    data_execucao_berlinda = pd.to_datetime(df_berlinda['data_da_execucao'].iloc[0])
    ULTIMO_DIA_MES = data_execucao_berlinda + pd.offsets.MonthEnd(0)
    
    if data_execucao_berlinda.date() == ULTIMO_DIA_MES.date():
        df_berlinda['dias_disponiveis'] = 0
    else:
        dias_restantes = (ULTIMO_DIA_MES - data_execucao_berlinda).days
        df_berlinda['dias_disponiveis'] = df_berlinda['ocupacao_ainda_disponivel'].clip(upper=dias_restantes).fillna(0).astype(int)   

    # Calcular dias necessários (evitar divisão por zero)
    df_berlinda["dias_necessarios"] = df_berlinda.apply(
        lambda row: np.ceil(row["falta_meta"] / row["media_preco_disponivel"]) 
        if row["media_preco_disponivel"] > 0 else 0,
        axis=1
    )
    
    # Calcular potencial máximo
    df_berlinda["potencial_max"] = (
        df_berlinda["faturamento_mes"] + 
        (df_berlinda["dias_disponiveis"] * df_berlinda["media_preco_disponivel"]) # <<< Usar a nova coluna
    )
    
    # Calcular potencial realista
    df_berlinda["potencial_realista"] = (
        df_berlinda["faturamento_mes"] + 
        (df_berlinda["to_listings"] * df_berlinda["dias_disponiveis"] * df_berlinda["media_preco_disponivel"]) # <<< Usar a nova coluna
    )
    
    # Calcular score bruto
    df_berlinda["score_bruto"] = (
        (df_berlinda["falta_meta"] / df_berlinda["meta"]) *
        (1 / df_berlinda["dias_disponiveis"].replace(0, 1)) * # <<< Usar a nova coluna
        (df_berlinda["potencial_max"] - df_berlinda["faturamento_mes"]) *
        (1 / df_berlinda["dias_necessarios"].replace(0, 1))
    )
    
    # Normalizar score por rank percentil
    df_berlinda["score_normalizado"] = df_berlinda["score_bruto"].rank(pct=True) * 100
    
    # <<< CORREÇÃO 3: Atualizar a função e o nome da coluna de prioridade
    def classificar_prioridade(score):
        if score >= 80:
            return "Crítico"
        elif score >= 50:
            return "Alta"
        elif score >= 20:
            return "Média"
        else:
            return "Baixa"
    
    df_berlinda["faixa_prioridade"] = df_berlinda["score_normalizado"].apply(classificar_prioridade) # <<< Nome da coluna corrigido
    
    # Classificar status operacional
    def classificar_status(row):
        # >>> REGRA 1: Acima ou igual à meta
        if row["atingimento_meta"] >= 1.0:
            if row["dias_disponiveis"] > 0:
                # Se está na meta ou acima, mas ainda tem dias, está em risco de cair.
                return "🟡 Acima com risco"
            else:
                # Se está na meta ou acima e não tem mais dias, a ação é apenas monitorar.
                return "🟡 Acima sem ação"
        # >>> REGRA 2: Abaixo da meta
        else:
            if row["dias_disponiveis"] == 0:
                # Se está abaixo da meta e não tem mais dias, é inviável bater a meta no mês.
                return "🔴 Abaixo inviável"
            elif row["potencial_realista"] >= row["meta"]:
                # Se está abaixo, tem dias e o potencial realista permite bater a meta, é viável.
                return "🟢 Abaixo viável"
            else:
                # Se está abaixo, tem dias, mas mesmo com o potencial realista não bate a meta, precisa de esforço.
                return "🟠 Abaixo precisa esforço"
    
    df_berlinda["status_operacional"] = df_berlinda.apply(classificar_status, axis=1)

# %%
# Salvar resultados
print("\n💾 Salvando arquivos processados...")

# ==============================================================================
# 1. NOVO: Extrair a data de execução para o nome do arquivo
# ==============================================================================
# Verificamos se o DataFrame não está vazio antes de tentar acessar a coluna
if not df_final.empty:
    # Pega a data da primeira linha, converte para o formato datetime e depois para string 'YYYY-MM-DD'
    run_date_str = pd.to_datetime(df_final['data_da_execucao'].iloc[0]).strftime('%Y-%m-%d')
else:
    # Fallback: caso o DataFrame esteja vazio, usa a data atual
    run_date_str = pd.to_datetime('today').strftime('%Y-%m-%d')
print("⚠️ DataFrame final vazio. Usando data atual para o nome do arquivo.")

# O nome do arquivo agora inclui a data da execução
output_final = os.path.join(PROCESSED_DIR, f"meta_analysis_final_enriched_{run_date_str}.csv")
df_final.to_csv(output_final, index=False, encoding='utf-8')
print(f"✅ Salvo: {output_final}")

# Salvar DataFrame da Berlinda
if len(df_berlinda) > 0:
    # O nome do arquivo também inclui a data da execução
    output_berlinda = os.path.join(PROCESSED_DIR, f"berlinda_prepared_{run_date_str}.csv")
    df_berlinda.to_csv(output_berlinda, index=False, encoding='utf-8')
    print(f"✅ Salvo: {output_berlinda}")

# %%
# %%
# Exibir estatísticas finais
print("\n📊 Estatísticas finais:")
print(f"📈 Total de imóveis analisados: {len(df_final)}")
print(f"🎯 Imóveis na Berlinda: {len(df_berlinda)}")

if len(df_berlinda) > 0:
    print("\n📋 Distribuição de status na Berlinda:")
    print(df_berlinda["status_operacional"].value_counts())
    
    # <<< CORREÇÃO: Alterar o nome da coluna para 'faixa_prioridade'
    print("\n📋 Distribuição de prioridade na Berlinda:")
    print(df_berlinda["faixa_prioridade"].value_counts())

print("\n📉 Valores nulos no DataFrame final:")
print(df_final.isnull().sum())

print("\n🎉 Processamento concluído com sucesso!")
