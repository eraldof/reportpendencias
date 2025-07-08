import streamlit as st
import tempfile
from io import BytesIO
import pandas as pd
import support
import nomes_colaboradores

# Cache converter to Excel bytes
@st.cache_data
def df_to_excel(df: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

st.set_page_config(
    page_title="Análise de Ponto - PDF para Excel",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🕐 Sistema de Análise de Ponto")

# Mapeamento de Centros de Distribuição para listas de colaboradores
CD_MAP = {
    "31": nomes_colaboradores.COLABORADORES31,
    "59": nomes_colaboradores.COLABORADORES59,
    "67": nomes_colaboradores.COLABORADORES67,
}

# Sidebar para configurações
with st.sidebar:
    st.header("⚙️ Configurações")
    cd_selecionado = st.selectbox(
        "Centro de Distribuição:",
        options=list(CD_MAP.keys()),
        help="Selecione o CD para análise"
    )
    
    pdf_file = st.file_uploader(
        "Upload do PDF:",
        type=["pdf"],
        help="Faça o upload do PDF de ponto para conversão"
    )

# Inicializar session state
if 'df_processed' not in st.session_state:
    st.session_state.df_processed = None

# Processamento do PDF
if pdf_file:
    if st.sidebar.button("🚀 Processar PDF", type="primary"):
        colaboradores = CD_MAP[cd_selecionado]
        
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(pdf_file.read())
            tmp_path = tmp.name

        with st.spinner("Processando PDF..."):
            try:
                df_final = support.main(tmp_path)
                st.session_state.df_processed = df_final
                st.sidebar.success("✅ Processamento concluído!")
            except Exception as e:
                st.sidebar.error(f"❌ Erro ao processar PDF: {e}")

# Verificar se há dados processados
if st.session_state.df_processed is not None:
    df = st.session_state.df_processed
    
    # Criar abas
    tab1, tab2 = st.tabs([
        "📋 Dados Brutos", 
        "📥 Download"
    ])
    
    with tab1:
        st.header("📋 Dados Brutos")
        
        # Filtros
        col1, col2, col3 = st.columns(3)
        
        with col1:
            colaboradores_filtro = st.multiselect(
                "Filtrar por Colaborador:",
                options=df['COLABORADOR'].unique(),
                help="Selecione colaboradores específicos"
            )
        
        with col2:
            mostrar_apenas_alertas = st.checkbox(
                "Apenas registros com alertas",
                help="Mostrar somente registros que possuem alertas"
            )
        
        with col3:
            mostrar_apenas_ausencias = st.checkbox(
                "Apenas registros com ausências",
                help="Mostrar somente registros com ausências"
            )
        
        # Aplicar filtros
        df_filtered = df.copy()
        df_filtered = df_filtered[['Dia', 'Data', 'COLABORADOR', 'ENTRADA', 'SAIDA INTERVALO', 'VOLTA INTERVALO', 'SAIDA', 'AUSENCIA', 'ALERTA']]
        
        if colaboradores_filtro:
            df_filtered = df_filtered[df_filtered['COLABORADOR'].isin(colaboradores_filtro)]
        
        if mostrar_apenas_alertas:
            df_filtered = df_filtered[df_filtered['ALERTA'].notna() & (df_filtered['ALERTA'] != '')]
        
        if mostrar_apenas_ausencias:
            df_filtered = df_filtered[df_filtered['AUSENCIA'].notna() & (df_filtered['AUSENCIA'] != '')]
        
        st.subheader(f"📊 Dados Filtrados ({len(df_filtered)} registros)")
        st.dataframe(df_filtered, use_container_width=True)
    
    with tab2:
        st.header("📥 Downloads")
        

        st.subheader("📊 Dados Completos")
        excel_bytes = df_to_excel(df)
        st.download_button(
            label="📄 Baixar dados completos (Excel)",
            data=excel_bytes,
            file_name=f"CD_{cd_selecionado}_ponto_completo.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        

else:
    # Página inicial quando não há dados
    st.markdown("""
    ## 👋 Bem-vindo ao Sistema de Análise de Ponto
    
    
    ### 🚀 Como usar:
    1. Selecione o Centro de Distribuição na barra lateral
    2. Faça upload do arquivo PDF
    3. Clique em "Processar PDF"
    4. Explore as diferentes abas com análises e relatórios
    
    """)
    
    # Informações sobre os dados
    st.info("📁 Faça o upload de um arquivo PDF na barra lateral para começar a análise.")
