import streamlit as st
import pandas as pd
import plotly.express as px
import os
import sys

# Diretório onde o script está rodando
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
# Diretório raiz do repositório (um nível acima)
REPO_ROOT = os.path.dirname(CURRENT_DIR)

@st.cache_data
def load_data():
    """Carrega o dataset principal"""
    try:
        caminho = os.path.join(REPO_ROOT, "meta_analysis_final_enriched.csv")
        df = pd.read_csv(caminho)
        if df['atingimento_meta'].max() > 5:
            df['atingimento_meta'] = df['atingimento_meta'] / 100
        return df
    except FileNotFoundError:
        st.error(f"Arquivo não encontrado: {caminho}")
        st.write(f"Diretório atual: {CURRENT_DIR}")
        st.write(f"Raiz do repositório: {REPO_ROOT}")
        st.write(f"Conteúdo da raiz: {os.listdir(REPO_ROOT)}")
        return pd.DataFrame()

@st.cache_data
def load_berlinda():
    """Carrega o dataset da Berlinda"""
    try:
        caminho = os.path.join(REPO_ROOT, "berlinda_prepared.csv")
        return pd.read_csv(caminho)
    except FileNotFoundError:
        st.error(f"Arquivo não encontrado: {caminho}")
        st.write(f"Conteúdo da raiz: {os.listdir(REPO_ROOT)}")
        return pd.DataFrame()

# Título
st.title("📊 Meta Performance Dashboard")

# --- Carregar dados ---
df = load_data()
df_berlinda = load_berlinda()

# Mostrar informações de debug (opcional)
with st.expander("🔍 Informações de Debug"):
    st.write(f"Diretório do app: {APP_DIR}")
    st.write(f"Diretório raiz: {PROJECT_ROOT}")
    st.write(f"Arquivo principal carregado: {len(df)} linhas")
    st.write(f"Arquivo Berlinda carregado: {len(df_berlinda)} linhas")

# Verificar se os dados foram carregados
if df.empty:
    st.error("Não foi possível carregar os dados principais.")
    st.stop()


# Verificar se os dados foram carregados
if df.empty:
    st.error("Não foi possível carregar os dados principais. Verifique se os scripts de preparação foram executados.")
    st.stop()

# --- FILTROS (compartilhados) ---
st.sidebar.header("Filtros")

# Obter opções de filtro do dataset principal
categorias = sorted(df["categoria"].dropna().unique())
carteiras = sorted(df["carteira"].dropna().unique())
estados = sorted(df["estado"].dropna().unique())
cidades = sorted(df["cidade"].dropna().unique())
grupos = sorted(df["grupo_criticidade"].dropna().unique())

categoria_sel = st.sidebar.multiselect("Categoria", options=categorias, default=[])
carteira_sel = st.sidebar.multiselect("Carteira", options=carteiras, default=[])
estado_sel = st.sidebar.multiselect("Estado", options=estados, default=[])
cidade_sel = st.sidebar.multiselect("Cidade", options=cidades, default=[])
grupo_sel = st.sidebar.multiselect("Grupo de Criticidade", options=grupos, default=[])
dias_min = st.sidebar.number_input("Mínimo de Dias Disponíveis", min_value=0, max_value=30, value=0, step=1)

# Aplicar filtros no dataset principal
df_filtered = df.copy()
if categoria_sel:
    df_filtered = df_filtered[df_filtered["categoria"].isin(categoria_sel)]
if carteira_sel:
    df_filtered = df_filtered[df_filtered["carteira"].isin(carteira_sel)]
if estado_sel:
    df_filtered = df_filtered[df_filtered["estado"].isin(estado_sel)]
if cidade_sel:
    df_filtered = df_filtered[df_filtered["cidade"].isin(cidade_sel)]
if grupo_sel:
    df_filtered = df_filtered[df_filtered["grupo_criticidade"].isin(grupo_sel)]
if dias_min > 0:
    df_filtered = df_filtered[df_filtered["ocupacao_ainda_disponivel"] >= dias_min]

if df_filtered.empty:
    st.warning("Nenhum dado encontrado com os filtros aplicados.")
    st.stop()

# Filtrar Berlinda com os mesmos critérios (exceto grupo_criticidade)
if not df_berlinda.empty:
    df_berlinda_filtered = df_berlinda.copy()
    if categoria_sel:
        df_berlinda_filtered = df_berlinda_filtered[df_berlinda_filtered["categoria"].isin(categoria_sel)]
    if carteira_sel:
        df_berlinda_filtered = df_berlinda_filtered[df_berlinda_filtered["carteira"].isin(carteira_sel)]
    if estado_sel:
        df_berlinda_filtered = df_berlinda_filtered[df_berlinda_filtered["estado"].isin(estado_sel)]
    if cidade_sel:
        df_berlinda_filtered = df_berlinda_filtered[df_berlinda_filtered["cidade"].isin(cidade_sel)]
    if dias_min > 0:
        df_berlinda_filtered = df_berlinda_filtered[df_berlinda_filtered["ocupacao_ainda_disponivel"] >= dias_min]
else:
    df_berlinda_filtered = pd.DataFrame()

# --- ABAS ---
tab1, tab2 = st.tabs(["📊 Visão Geral", "🎯 Berlinda Detalhada"])

# =============== ABA 1: VISÃO GERAL ===============
with tab1:
    st.subheader("📌 Visão Geral de Performance")
    st.caption("Foco na Berlinda: imóveis entre 80–110% da meta com potencial de ação.")

    # Calcular métricas
    total_imoveis = len(df_filtered)
    berlinda_df = df_filtered[df_filtered['grupo_criticidade'] == 'berlinda']
    berlinda_com_potencial = berlinda_df[berlinda_df['ocupacao_ainda_disponivel'] > 5]

    col_kpi1, col_kpi2, col_kpi3, col_kpi4 = st.columns(4)
    col_kpi1.metric("Listings Analisados", f"{total_imoveis:,}")
    perc_berlinda = len(berlinda_df) / total_imoveis * 100 if total_imoveis > 0 else 0
    col_kpi2.metric("Na Berlinda (80–110%)", f"{perc_berlinda:.1f}%")
    col_kpi3.metric("Com Potencial de Ação", f"{len(berlinda_com_potencial)}")

    # --- GRÁFICO DE BARRAS ---
    st.subheader("Distribuição por Grupo de Criticidade")
    criticidade_counts = df_filtered["grupo_criticidade"].value_counts().reset_index()
    criticidade_counts.columns = ["grupo_criticidade", "quantidade"]
    total = criticidade_counts["quantidade"].sum()
    criticidade_counts["percentual"] = (criticidade_counts["quantidade"] / total * 100).round(1)
    criticidade_counts["percentual_str"] = criticidade_counts["percentual"].astype(str) + "%"

    ordem = ["crítico", "atenção", "berlinda", "ok", "meta_subestimada"]
    criticidade_counts["grupo_criticidade"] = pd.Categorical(
        criticidade_counts["grupo_criticidade"], categories=ordem, ordered=True
    )
    criticidade_counts = criticidade_counts.sort_values("grupo_criticidade")

    label_map = {
        "crítico": "crítico (≤ 50%)",
        "atenção": "atenção (50%–80%)",
        "berlinda": "berlinda (80–110%)",
        "ok": "ok (110%–200%)",
        "meta_subestimada": "meta_subestimada (> 200%)"
    }
    criticidade_counts["grupo_legenda"] = criticidade_counts["grupo_criticidade"].map(label_map)

    fig1 = px.bar(
        criticidade_counts,
        x="grupo_legenda",
        y="quantidade",
        text="percentual_str",
        color="grupo_criticidade",
        color_discrete_map={
            "crítico": "#d32f2f",
            "atenção": "#f57c00",
            "berlinda": "#388e3c",
            "ok": "#1976d2",
            "meta_subestimada": "#7b1fa2"
        },
        labels={"grupo_legenda": "Grupo de Criticidade", "quantidade": "Quantidade"},
        title="Quantidade por Grupo"
    )
    fig1.update_traces(textposition="outside")
    st.plotly_chart(fig1, use_container_width=True)

    # --- HEATMAP ---
    st.subheader("Heatmap: % de Imóveis por Categoria e Grupo de Criticidade")
    agrupamento = st.radio("Agrupar por:", options=["Estado", "Carteira"], horizontal=True)
    coluna_agrupamento = 'estado' if agrupamento == "Estado" else 'carteira'

    heatmap_abs = df_filtered.pivot_table(
        index=coluna_agrupamento,
        columns='grupo_criticidade',
        values='listing',
        aggfunc='count',
        fill_value=0
    )
    ordem_grupos = ["crítico", "atenção", "berlinda", "ok", "meta_subestimada"]
    heatmap_abs = heatmap_abs.reindex(columns=ordem_grupos, fill_value=0)
    heatmap_prop = heatmap_abs.div(heatmap_abs.sum(axis=1), axis=0) * 100
    heatmap_prop = heatmap_prop.fillna(0)

    fig_heatmap = px.imshow(
        heatmap_prop,
        text_auto=".1f",
        color_continuous_scale='Reds',
        aspect="auto",
        labels={'x': 'Grupo de Criticidade', 'y': agrupamento, 'color': f'% por {agrupamento}'},
        title=f"Proporção de imóveis por {agrupamento} e Grupo de Criticidade (%)"
    )
    st.plotly_chart(fig_heatmap, use_container_width=True)

    # --- SCATTER PLOT ---
    st.subheader("Scatter Plot: Análise de Performance")
    x_options = ['ocupacao_ainda_disponivel', 'to_listings']
    x_col = st.selectbox("Eixo X", options=x_options, index=0)

    df_scatter = df_filtered.dropna(subset=[x_col, 'atingimento_meta'])
    hover_cols = ['listing', 'categoria', 'carteira', 'estado', 'cidade', 'to_listings', 'ocupacao_ainda_disponivel']
    valid_hover_cols = [col for col in hover_cols if col in df_scatter.columns]

    fig2 = px.scatter(
        df_scatter,
        x=x_col,
        y='atingimento_meta',
        color='grupo_criticidade',
        color_discrete_map={
            "crítico": "#d32f2f",
            "atenção": "#f57c00",
            "berlinda": "#388e3c",
            "ok": "#1976d2",
            "meta_subestimada": "#7b1fa2"
        },
        hover_data=valid_hover_cols,
        labels={
            x_col: x_col.replace('_', ' ').title(),
            'atingimento_meta': 'Atingimento da Meta (%)',
            'grupo_criticidade': 'Grupo de Criticidade'
        },
        title=f"{x_col.replace('_', ' ').title()} vs Atingimento da Meta"
    )
    fig2.add_hline(y=0.5, line_dash="dot", line_color="#d32f2f", annotation_text="50% (Crítico)")
    fig2.add_hline(y=0.8, line_dash="dot", line_color="#f57c00", annotation_text="80% (Berlinda)")
    fig2.add_hline(y=1.1, line_dash="dot", line_color="#1976d2", annotation_text="110% (OK)")
    fig2.update_layout(yaxis_tickformat='.0%')
    st.plotly_chart(fig2, use_container_width=True)

    # --- TABELA COMPLETA ---
    st.subheader("Tabela Completa (com filtros aplicados)")
    st.dataframe(df_filtered, use_container_width=True, height=400)

    @st.cache_data
    def convert_df_full(df):
        return df.to_csv(index=False).encode('utf-8')
    csv_full = convert_df_full(df_filtered)
    st.download_button("📥 Exportar Tabela Completa", csv_full, "dados_completos_filtrados.csv", "text/csv")

# =============== ABA 2: BERLINDA DETALHADA ===============
with tab2:
    st.subheader("🎯 Dashboard da Berlinda")
    st.caption("Análise tática dos imóveis entre 80–110% da meta, com foco em ação operacional.")   

    if df_berlinda_filtered.empty:
        st.warning("Nenhum imóvel na Berlinda com os filtros aplicados.")
        st.stop()

    # --- KPIs da Berlinda ---
    total_berlinda = len(df_berlinda_filtered)
    viaveis = df_berlinda_filtered[df_berlinda_filtered['status_operacional'].isin(['🟢 Abaixo viável', '🟠 Abaixo precisa esforço'])]
    acima_risco = df_berlinda_filtered[df_berlinda_filtered['status_operacional'] == '🟡 Acima com risco']
    prioritarios = df_berlinda_filtered[df_berlinda_filtered['prioridade'].isin(['Crítica', 'Média'])]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total na Berlinda", total_berlinda)
    col2.metric("Viáveis", len(viaveis))
    col3.metric("Acima com risco", len(acima_risco))
    col4.metric("Prioritários", len(prioritarios))

    # --- STATUS OPERACIONAL (barras horizontais) ---
    st.subheader("Status Operacional")
    status_counts = df_berlinda_filtered['status_operacional'].value_counts().reset_index()
    status_counts.columns = ['status', 'count']
    status_counts = status_counts.sort_values('count', ascending=False)

    fig_status = px.bar(
        status_counts,
        x='status',
        y='count',
        text='count',
        color='status',
        color_discrete_map={
            '🟢 Abaixo viável': '#388e3c',
            '🟠 Abaixo precisa esforço': '#ffa726',
            '🔴 Abaixo inviável': '#d32f2f',
            '🟡 Acima com risco': '#fbc02d',
            '🟡 Acima sem ação': '#bdbdbd'
        },
        labels={'status': 'Status Operacional', 'count': 'Quantidade'}
    )
    fig_status.update_traces(textposition="outside")
    st.plotly_chart(fig_status, use_container_width=True)

    st.markdown("""
    ##### 📋 Como interpretar os status operacionais?
    - **🟢 Abaixo viável**: Está abaixo da meta, mas tem dias disponíveis e consegue bater a meta com o desempenho atual.
    - **🟠 Abaixo precisa esforço**: Está abaixo da meta, tem dias disponíveis, mas precisa melhorar desempenho (preço, campanha etc.).
    - **🔴 Abaixo inviável**: Está abaixo da meta e **não tem mais dias disponíveis** → não há ação possível neste mês.
    - **🟡 Acima com risco**: Está **na meta ou acima**, mas **ainda tem dias disponíveis** → pode cair se concorrentes seguirem faturando.
    - **🟡 Acima sem ação**: Já bateu a meta e **não tem mais dias disponíveis** → só monitorar.""")

    # --- SCATTER PLOT DE VIABILIDADE ---
    st.subheader("Scatter Plot: Viabilidade e Prioridade")

    # Opções para eixo X
    x_options_berlinda = {
        "Dias Disponíveis": "ocupacao_ainda_disponivel",
        "Falta Meta (R$)": "falta_meta",
        "Preço Médio Disponível": "media_preco_disponivel",
        "Taxa de Ocupação (TO)": "to_listings"
    }
    x_label = st.selectbox("Eixo X", options=list(x_options_berlinda.keys()), index=0)
    x_col = x_options_berlinda[x_label]

    df_scatter_berlinda = df_berlinda_filtered[df_berlinda_filtered['ocupacao_ainda_disponivel'] > 0].copy()
    if not df_scatter_berlinda.empty:
        # Usar valor absoluto para falta_meta no tamanho (evitar negativos)
        df_scatter_berlinda['falta_meta_abs'] = df_scatter_berlinda['falta_meta'].abs()

        fig_scatter = px.scatter(
            df_scatter_berlinda,
            x=x_col,
            y='score_normalizado',
            color='status_operacional',
            size='falta_meta_abs',
            hover_data=['listing', 'carteira', 'estado', 'ocupacao_ainda_disponivel', 'falta_meta'],
            color_discrete_map={
                '🟢 Abaixo viável': '#388e3c',
                '🟠 Abaixo precisa esforço': '#ffa726',
                '🟡 Acima com risco': '#fbc02d'
            },
            labels={
                x_col: x_label,
                'score_normalizado': 'Prioridade (%)',
                'falta_meta_abs': 'Falta Meta (R$)'
            }
        )
        st.plotly_chart(fig_scatter, use_container_width=True)
    else:
        st.info("Nenhum imóvel com dias disponíveis para análise.")

    st.markdown("""
    ##### 🎯 O que é a "Prioridade"?
    É um **score de 0 a 100** que indica **quão crítico é agir agora**:
    - **Crítica (80–100)**: Alto impacto + baixo esforço (ex: falta 1 dia para bater meta).
    - **Média (50–79)**: Viável, mas exige atenção.
    - **Baixa (0–49)**: Pouco impacto ou inviável.
    - **Baseado em**: proximidade da meta, dias disponíveis e potencial de ajuste.
    """)

    # --- TABELA OPERACIONAL ---
    st.subheader("Tabela Operacional")
    col_filt1, col_filt2 = st.columns(2)
    with col_filt1:
        filtro_status = st.multiselect(
            "Filtrar por Status",
            options=df_berlinda_filtered['status_operacional'].unique(),
            default=df_berlinda_filtered['status_operacional'].unique()
        )
    with col_filt2:
        filtro_prioridade = st.multiselect(
            "Filtrar por Prioridade",
            options=df_berlinda_filtered['prioridade'].unique(),
            default=df_berlinda_filtered['prioridade'].unique()
        )

    # Aplicar filtros locais
    df_tabela_filtrada = df_berlinda_filtered[
        (df_berlinda_filtered['status_operacional'].isin(filtro_status)) &
        (df_berlinda_filtered['prioridade'].isin(filtro_prioridade))
    ]

    # Colunas expandidas
    col_order = [
        'listing', 'carteira', 'estado', 'status_operacional', 'prioridade',
        'faturamento_mes', 'meta', 'falta_meta',
        'ocupacao_ainda_disponivel', 'dias_necessarios',
        'to_listings', 'media_preco_disponivel',
        'score_normalizado'
    ]

    # Verificar quais colunas existem no dataframe
    col_order = [col for col in col_order if col in df_tabela_filtrada.columns]

    df_tabela_final = df_tabela_filtrada[col_order].sort_values(
        ['prioridade', 'score_normalizado', 'dias_necessarios'],
        ascending=[False, False, True]
    )

    st.dataframe(df_tabela_final, use_container_width=True, height=500)

    # Botão de exportação (só dos filtrados)
    @st.cache_data
    def convert_df_berlinda(df):
        return df.to_csv(index=False).encode('utf-8')
    
    csv_berlinda = convert_df_berlinda(df_tabela_final)
    st.download_button("📥 Exportar Tabela Filtrada", csv_berlinda, "berlinda_filtrada.csv", "text/csv")

#a
# --- Rodapé ---
st.caption(f"Total de listings exibidos: {len(df_filtered)} | Atualizado em {pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')}")