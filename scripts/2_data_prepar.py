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
# %%
# %%
# Ler os 4 arquivos CSV (incluindo Pmin)
try:
    df_location = pd.read_csv(os.path.join(RAW_DIR, "meta_analysis_location.csv"))
    df_meta = pd.read_csv(os.path.join(RAW_DIR, "meta_analysis_performance_value_meta.csv"))
    df_prices = pd.read_csv(os.path.join(RAW_DIR, "meta_analysis_price.csv"))
    print("✅ Arquivos CSV principais lidos com sucesso")

    # <<< NOVO: Tentar ler o arquivo de Pmin (nome fixo)
    try:
        df_pmin = pd.read_csv(os.path.join(RAW_DIR, 'meta_analysis_pmin.csv'))
        # <<< CORREÇÃO: Remover colunas desnecessárias para não quebrar o merge
        if 'data_da_execucao' in df_pmin.columns:
            df_pmin = df_pmin.drop(columns=['data_da_execucao'])
        print("✅ Arquivo de Pmin lido e limpo com sucesso: meta_analysis_pmin.csv")
    except FileNotFoundError:
        df_pmin = pd.DataFrame()
        print("⚠️ Arquivo 'meta_analysis_pmin.csv' não encontrado. A coluna 'dias_pmin' não será criada.")
    except Exception as e:
        df_pmin = pd.DataFrame()
        print(f"⚠️ Erro inesperado ao ler Pmin: {e}. A coluna 'dias_pmin' não será criada.")

except FileNotFoundError as e:
    print(f"❌ Erro ao ler arquivos principais: {e}")
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
# %%
# Fazer merge entre os DataFrames
print("\n🔗 Realizando merges dos DataFrames...")
# Primeiro, juntar df_meta com df_prices
df_merged = df_meta.merge(df_prices, on="listing", how="left")

# Depois, juntar com df_location
df_merged = df_merged.merge(df_location, on="listing", how="left")

#Juntar com df_pmin, se existir
if not df_pmin.empty:
    df_final = df_merged.merge(df_pmin, on="listing", how="left")
    # Renomear a coluna para o padrão do nosso DataFrame
    df_final = df_final.rename(columns={'n_dates_special_price': 'dias_pmin'})
    print("✅ Merge com Pmin concluído.")
else:
    df_final = df_merged
    print("⚠️ Merge com Pmin pulado (arquivo não encontrado).")

#Limpar colunas de data duplicadas do merge
cols_to_drop = [col for col in df_final.columns if 'data_da_execucao' in col and col != 'data_da_execucao']
df_final = df_final.drop(columns=cols_to_drop)
if cols_to_drop:
    print(f"🧹 Limpando colunas de data duplicadas: {cols_to_drop}")

print(df_final.columns)
print(f"✅ Merge geral concluído")

print(df_final.columns)
print(f"✅ Merge geral concluído")
print(f" - df_meta: {len(df_meta)} linhas")
print(f" - df_final: {len(df_final)} linhas")

# ==============================================================================
# NOVO: Modificar coluna 'carteira' para indicar System Price
# ==============================================================================
print("\n🔧 Ajustando coluna 'carteira' para System Price...")

# Garantir que a coluna has_system_price seja booleana para a comparação
# Preencher valores nulos com False (caso o merge não encontre correspondência)
df_final['has_system_price'] = df_final['has_system_price'].fillna(False)

# Aplicar a modificação usando np.where para eficiência
# Se has_system_price for True, adiciona '_sys-price', senão mantém o original
df_final['carteira'] = np.where(
    df_final['has_system_price'] == True,
    df_final['carteira'] + '_sys-price',
    df_final['carteira']
)

print("✅ Coluna 'carteira' ajustada.")


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
    "dias_pmin",
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

# ==============================================================================
# BLOCO DE DEPURAÇÃO (PARA REMOVER DEPOIS)
# ==============================================================================
DEBUG_MODE = True  # Mude para False para desativar os prints

if DEBUG_MODE:
    print("\n" + "="*50)
    print("🐛 INICIANDO MODO DEPURAÇÃO")
    print("="*50)

    # 1. Verificar as colunas base antes de qualquer cálculo
    print("\n📌 Análise da coluna 'meta' em df_meta:")
    print(df_meta['meta'].describe())
    print(f"Quantos 'meta' são zero ou nulos? {(df_meta['meta'].isna() | (df_meta['meta'] == 0)).sum()}")

    print("\n📌 Análise da coluna 'faturamento_mes' em df_meta:")
    print(df_meta['faturamento_mes'].describe())
    print(f"Quantos 'faturamento_mes' são zero ou nulos? {(df_meta['faturamento_mes'].isna() | (df_meta['faturamento_mes'] == 0)).sum()}")

    # 2. Verificar o atingimento e a criticidade no DataFrame final
    print("\n📌 Análise da coluna 'atingimento_meta' em df_final:")
    print(df_final['atingimento_meta'].describe())

    print("\n📌 DISTRIBUIÇÃO FINAL - Contagem de grupo_criticidade em df_final:")
    contagem_criticidade = df_final['grupo_criticidade'].value_counts()
    print(contagem_criticidade)
    
    if 'berlinda' not in contagem_criticidade:
        print("\n🚨 ATENÇÃO: Nenhum imóvel foi classificado como 'berlinda'!")
    else:
        print(f"\n✅ Encontrados {contagem_criticidade['berlinda']} imóveis na 'berlinda'.")

    print("="*50)
    print("🐛 FIM DO MODO DEPURAÇÃO")
    print("="*50 + "\n")
# ==============================================================================
# FIM DO BLOCO DE DEPURAÇÃO
# ==============================================================================

# %%
# %%
# Calcular métricas para a Berlinda
print("\n🎯 Calculando métricas específicas para Berlinda...")

# Filtrar apenas Berlinda
df_berlinda = df_final[df_final["grupo_criticidade"] == "berlinda"].copy()

# --- Funções Auxiliares ---
def calcular_dias_disponiveis(df, data_ref):
    """Calcula os dias disponíveis com base na data de referência."""
    ultimo_dia_mes = data_ref + pd.offsets.MonthEnd(0)
    if data_ref.date() == ultimo_dia_mes.date():
        return 0
    dias_restantes = (ultimo_dia_mes - data_ref).days
    return df['ocupacao_ainda_disponivel'].clip(upper=dias_restantes).fillna(0).astype(int)

def calcular_potenciais(df):
    """Calcula os potenciais máximo e realista."""
    df["potencial_max"] = (
        df["faturamento_mes"] + (df["dias_disponiveis"] * df["media_preco_disponivel"])
    )
    df["potencial_realista"] = (
        df["faturamento_mes"] + 
        (df["to_listings"] * df["dias_disponiveis"] * df["media_preco_disponivel"])
    )

def classificar_status(row):
    """Classifica o status operacional de um imóvel."""
    if row["atingimento_meta"] >= 1.0:
        return "🟡 Acima com risco" if row["dias_disponiveis"] > 0 else "🟡 Acima sem ação"
    else:
        if row["dias_disponiveis"] == 0:
            return "🔴 Abaixo inviável"
        return "🟢 Abaixo viável" if row["potencial_realista"] >= row["meta"] else "🟠 Abaixo precisa esforço"

# --- Lógica Principal ---
if len(df_berlinda) > 0:
    print(f"🐛 DEBUG: Colunas encontradas no df_berlinda: {df_berlinda.columns.tolist()}")
    
    # Verificação crítica da coluna de data
    if 'data_da_execucao' not in df_berlinda.columns or df_berlinda['data_da_execucao'].isnull().all():
        print("⚠️ A coluna 'data_da_execucao' não foi encontrada ou está toda nula. Pulando o cálculo de métricas.")
        df_berlinda = pd.DataFrame()
    else:
        df_berlinda.dropna(subset=['data_da_execucao'], inplace=True)
        if df_berlinda.empty:
            print("⚠️ Após limpar nulos, o df_berlinda ficou vazio. Pulando o cálculo.")
        else:
            # Preparação inicial
            data_ref = pd.to_datetime(df_berlinda['data_da_execucao'].iloc[0])
            df_berlinda['dias_disponiveis'] = calcular_dias_disponiveis(df_berlinda, data_ref)
            df_berlinda["dias_necessarios"] = df_berlinda.apply(
                lambda row: np.ceil(row["falta_meta"] / row["media_preco_disponivel"]) 
                if row["media_preco_disponivel"] > 0 else 0, axis=1
            )
            calcular_potenciais(df_berlinda)

            # Separar para cálculo de score
            df_abaixo = df_berlinda[df_berlinda["atingimento_meta"] < 1.0].copy()
            df_acima = df_berlinda[df_berlinda["atingimento_meta"] >= 1.0].copy()

            # --- Cálculo de Score para "Abaixo da Meta" ---
            if not df_abaixo.empty:
                df_abaixo["score_bruto"] = (
                    (df_abaixo["falta_meta"] / df_abaixo["meta"]) *
                    (1 / df_abaixo["dias_disponiveis"].replace(0, 1)) *
                    (df_abaixo["potencial_max"] - df_abaixo["faturamento_mes"]) *
                    (1 / df_abaixo["dias_necessarios"].replace(0, 1))
                )
                df_abaixo["score_normalizado"] = df_abaixo["score_bruto"].rank(pct=True) * 100

            # --- Cálculo de Score para "Acima da Meta" (Risco) ---
            if not df_acima.empty:
                df_acima["score_risco_bruto"] = df_acima["dias_disponiveis"] * (1 / (df_acima["atingimento_meta"] - 0.99))
                df_acima["score_normalizado"] = df_acima["score_risco_bruto"].rank(pct=True) * 100

            # --- Aplicar Classificações ---
            def classificar_prioridade_abaixo(score):
                if score >= 80: return "Crítico"
                elif score >= 50: return "Alta"
                elif score >= 20: return "Média"
                else: return "Baixa"

            def classificar_prioridade_acima(score):
                if score >= 80: return "Risco Alto"
                elif score >= 50: return "Risco Médio"
                elif score >= 20: return "Risco Baixo"
                else: return "Sem Risco"
                
            if not df_abaixo.empty:
                df_abaixo["faixa_prioridade"] = df_abaixo["score_normalizado"].apply(classificar_prioridade_abaixo)
                df_abaixo["status_operacional"] = df_abaixo.apply(classificar_status, axis=1)
            
            if not df_acima.empty:
                df_acima["faixa_prioridade"] = df_acima["score_normalizado"].apply(classificar_prioridade_acima)
                df_acima["status_operacional"] = df_acima.apply(classificar_status, axis=1)

            # --- Recombinar e Finalizar ---
            df_berlinda = pd.concat([df_abaixo, df_acima], ignore_index=True)
            print(f"✅ Métricas da Berlinda calculadas para {len(df_berlinda)} imóveis.")

else:
    print("⚠️ Nenhum imóvel encontrado na Berlinda neste snapshot.")

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

# <<< NOVA VERIFICAÇÃO: Mostrar estatísticas da Berlinda apenas se ela não estiver vazia
if not df_berlinda.empty and 'status_operacional' in df_berlinda.columns:
    print("\n📋 Distribuição de status na Berlinda:")
    print(df_berlinda["status_operacional"].value_counts())
    
    print("\n📋 Distribuição de prioridade na Berlinda:")
    print(df_berlinda["faixa_prioridade"].value_counts())
else:
    print("\n📋 Não há dados da Berlinda para exibir as estatísticas.")

print("\n📉 Valores nulos no DataFrame final:")
print(df_final.isnull().sum())

print("\n🎉 Processamento concluído com sucesso!")
