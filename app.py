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
    page_title="PDF para Excel",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("Conversor de PDF para Excel")

# Mapeamento de Centros de Distribuição para listas de colaboradores
CD_MAP = {
    "31": nomes_colaboradores.COLABORADORES31,
    "59": nomes_colaboradores.COLABORADORES59,
    "67": nomes_colaboradores.COLABORADORES67,
}

cd_selecionado = st.selectbox(
    "Selecione o Centro de Distribuição:",
    options=list(CD_MAP.keys())
)

pdf_file = st.file_uploader(
    "Upload do arquivo PDF:",
    type=["pdf"],
    help="Faça o upload do PDF de ponto para conversão"
)

if pdf_file:
    if st.button("Iniciar Conversão"):
        colaboradores = CD_MAP[cd_selecionado]
        # Grava PDF em arquivo temporário se suporte.main requer caminho
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(pdf_file.read())
            tmp_path = tmp.name

        with st.spinner("Processando PDF..."):
            try:
                df_final = support.main(tmp_path, colaboradores, nomes_colaboradores.GESTORES)
                st.success("Processamento concluído com sucesso!", icon="🔥")

                # Gera e oferece download do Excel
                excel_bytes = df_to_excel(df_final)
                st.download_button(
                    label="Baixar resultado em Excel",
                    data=excel_bytes,
                    file_name=f"{cd_selecionado}_ponto.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            except Exception as e:
                st.error(f"Erro ao processar PDF: {e}")
else:
    st.info("Aguardando upload do arquivo PDF.")
