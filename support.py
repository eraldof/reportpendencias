import pandas as pd
import pdfplumber
from typing import List

def extrair_tabelas_espelho_ponto(caminho_pdf: str) -> List[pd.DataFrame]:
    """
    Extrai tabelas de espelho de ponto que contenham as colunas 'Data' e 'Observação'.

    Args:
        caminho_pdf (str): Caminho para o arquivo PDF

    Returns:
        List[pd.DataFrame]: Lista de DataFrames com as tabelas encontradas
    """
    tabelas_encontradas = []

    with pdfplumber.open(caminho_pdf) as pdf:
        for num_pagina, pagina in enumerate(pdf.pages):

            # Extrai todas as tabelas da página
            tabelas = pagina.extract_tables()

            for idx_tabela, tabela in enumerate(tabelas):
                if not tabela or len(tabela) < 2:  # Pula tabelas vazias ou muito pequenas
                    continue

                # Converte para DataFrame para facilitar a manipulação
                df = pd.DataFrame(tabela[1:], columns=tabela[0])  # Primeira linha como cabeçalho

                # Limpa valores None e espaços em branco
                df = df.fillna('')
                df = df.map(lambda x: str(x).strip() if x is not None else '')

                # Verifica se a tabela contém as colunas de interesse
                colunas = [col.strip().lower() for col in df.columns if col]

                tem_data = any('data' in col for col in colunas)
                tem_observacao = any('observação' in col or 'observacao' in col for col in colunas)

                if tem_data and tem_observacao:
                    # Adiciona metadados sobre a tabela
                    df.attrs['pagina'] = num_pagina + 1
                    df.attrs['tabela_index'] = idx_tabela

                    # Remove linhas completamente vazias
                    df = df.loc[~(df == '').all(axis=1)]

                    # Reset do index
                    df = df.reset_index(drop=True)

                    tabelas_encontradas.append(df)

    return tabelas_encontradas


def processar_celulas_mescladas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processa células mescladas preenchendo valores vazios com base no contexto.
    Funciona tanto para mesclagem vertical quanto horizontal.

    Args:
        df (pd.DataFrame): DataFrame com possíveis células mescladas

    Returns:
        pd.DataFrame: DataFrame processado
    """
    df_processado = df.copy()

    # Processar mesclagem HORIZONTAL (novo)
    for i in df_processado.index:
        colunas = list(df_processado.columns)

        for j, coluna in enumerate(colunas):
            valor = df_processado.loc[i, coluna]

            # Verifica se é uma célula mesclada horizontalmente
            if pd.notna(valor) and str(valor).strip():
                valor_str = str(valor).strip()

                # Identifica padrões típicos de mesclagem horizontal
                eh_mesclado = (
                    '**' in valor_str or
                    'AUSENTE' in valor_str.upper() or
                    'D.S.R' in valor_str.upper() or
                    'PERIODO' in valor_str.upper() or
                    'BANCO' in valor_str.upper()
                )

                if eh_mesclado:
                    # Preenche células vazias à direita com o mesmo valor
                    for k in range(j + 1, len(colunas)):
                        proxima_coluna = colunas[k]
                        proximo_valor = df_processado.loc[i, proxima_coluna]

                        # Se a célula à direita está vazia, preenche
                        if pd.isna(proximo_valor) or str(proximo_valor).strip() == '':
                            df_processado.loc[i, proxima_coluna] = valor_str
                        else:
                            # Para quando encontra uma célula preenchida
                            break

    return df_processado


def identificar_situacoes_especiais(valor):
    """
    Identifica situações especiais nas marcações de ponto.
    
    Args:
        valor: Valor da célula a ser analisado
    
    Returns:
        dict: Dicionário com informações sobre a situação especial
    """
    if pd.isna(valor) or valor == '':
        return {'tipo': 'vazio', 'valor_original': valor}
    
    valor_str = str(valor).strip().upper()
    
    # Verificar ausência
    if '**' in valor_str and 'AUSENT' in valor_str:
        return {'tipo': 'ausente', 'valor_original': valor}
    
    # Verificar isento de marcação
    if 'ISENTO' in valor_str and 'MARCAÇÃO' in valor_str:
        return {'tipo': 'isento', 'valor_original': valor}
    
    # Verificar férias
    if 'FÉRIAS' in valor_str or 'FERIAS' in valor_str:
        return {'tipo': 'ferias', 'valor_original': valor}
    
    # Verificar DSR
    if 'D.S.R' in valor_str or 'DSR' in valor_str:
        return {'tipo': 'dsr', 'valor_original': valor}
    
    # Se chegou até aqui, provavelmente é um horário normal
    return {'tipo': 'horario', 'valor_original': valor}


def limpar_e_converter_horarios(df):
    """
    Função para limpar e converter colunas de horário em um DataFrame,
    preservando informações especiais como ausências, isenções, etc.
    
    Parâmetros:
    df (pandas.DataFrame): DataFrame com as colunas de horário
    
    Retorna:
    pandas.DataFrame: DataFrame com as colunas limpas e com informações especiais preservadas
    """
    
    # Fazer uma cópia do dataframe para não modificar o original
    df_limpo = df.copy()
    
    # Definir as colunas de horário que precisam ser processadas
    colunas_horario = ['1a E.', '1a S.', '2a E.', '2a S.', '3a E.', '3a S.']
    
    # Adicionar colunas para armazenar informações especiais se não existirem
    if 'SITUACAO_ESPECIAL' not in df_limpo.columns:
        df_limpo['SITUACAO_ESPECIAL'] = ''
    
    def limpar_horario(valor):
        """
        Função auxiliar para limpar um valor de horário individual,
        mantendo apenas valores que parecem ser horários válidos.
        """
        situacao = identificar_situacoes_especiais(valor)
        
        if situacao['tipo'] != 'horario':
            return valor  # Preserva valores especiais como estão
            
        # Converter para string se não for
        valor_str = str(valor).strip()
        
        # Remover os caracteres indesejados: " O", " I", " P" (apenas para horários)
        valor_limpo = valor_str.replace('O', '').replace('I', '').replace('P', '')
        
        # Remover espaços extras
        valor_limpo = valor_limpo.strip()
        
        # Se ficou vazio após limpeza, retornar None
        if not valor_limpo:
            return None
            
        return valor_limpo

    
    # Processar cada linha para identificar situações especiais
    for idx, row in df_limpo.iterrows():
        situacoes_especiais = []
        
        # Verificar a primeira entrada especialmente
        primeira_entrada = row.get('1a E.', '')
        situacao_primeira = identificar_situacoes_especiais(primeira_entrada)
        
        if situacao_primeira['tipo'] in ['ausente', 'isento', 'ferias', 'dsr']:
            situacoes_especiais.append(situacao_primeira['tipo'])
        
        # Armazenar informações especiais
        if situacoes_especiais:
            df_limpo.at[idx, 'SITUACAO_ESPECIAL'] = ', '.join(situacoes_especiais)
    
    # Processar cada coluna de horário
    for coluna in colunas_horario:
        if coluna in df_limpo.columns:
            # Aplicar limpeza
            df_limpo[coluna] = df_limpo[coluna].apply(limpar_horario)
            
            # Converter para time apenas se for horário
            # (situações especiais permanecem como string)
            # df_limpo[coluna] = df_limpo[coluna].apply(converter_para_time)
    
    return df_limpo


def transformar_ponto(df, nome_colaborador=None, lista_gestores=None):
    """
    Transforma o DataFrame de ponto adicionando colunas de controle,
    considerando situações especiais identificadas e verificando se é gestor.
    
    Args:
        df (pandas.DataFrame): DataFrame com colunas de marcação de ponto
        nome_colaborador (str): Nome do colaborador para verificação se é gestor
        lista_gestores (list): Lista com nomes dos gestores
    
    Returns:
        pandas.DataFrame: DataFrame transformado com novas colunas
    """
    # Criar uma cópia do DataFrame para não modificar o original
    df_transformed = df.copy()
    
    # Verificar se o colaborador é gestor
    eh_gestor = False
    if lista_gestores and nome_colaborador:
        eh_gestor = nome_colaborador in lista_gestores
    
    # Adicionar as novas colunas se não existirem
    novas_colunas = ["AUSENCIA", "ENTRADA", "SAIDA INTERVALO", "VOLTA INTERVALO", "SAIDA", "ALERTA"]
    for col in novas_colunas:
        if col not in df_transformed.columns:
            df_transformed[col] = ""
    
    # Iterar pelas linhas do DataFrame
    for idx, row in df_transformed.iterrows():
        dia_semana = row.get('Dia', '')
        primeira_entrada = row.get('1a E.', '')
        primeira_saida = row.get('1a S.', '')
        segunda_entrada = row.get('2a E.', '')
        segunda_saida = row.get('2a S.', '')
        observacao = row.get('Observação', '')
        
        
        # Verificar se os valores são NaN ou vazios
        def is_empty(val):
            return pd.isna(val) or val == "" or val == " "
        
        # Identificar situações especiais na primeira entrada
        situacao_primeira = identificar_situacoes_especiais(primeira_entrada)
        
        # NOVA VERIFICAÇÃO: Se for gestor, sempre colocar 'N' no alerta
        if eh_gestor:
            df_transformed.at[idx, 'ALERTA'] = 'N'
            continue
        
        if not is_empty(observacao):
            df_transformed.at[idx, 'ALERTA'] = 'N'
            continue

        # 1. Caso seja Domingo
        elif dia_semana == 'Domingo':
            df_transformed.at[idx, 'ALERTA'] = 'N'
            continue
        
        # 2. Caso seja ausente
        elif situacao_primeira['tipo'] == 'ausente':
            df_transformed.at[idx, 'AUSENCIA'] = 'S'
            df_transformed.at[idx, 'ALERTA'] = 'S'
            continue
        
        # 3. Caso seja isento de marcação ou férias
        elif situacao_primeira['tipo'] in ['isento', 'ferias']:
            df_transformed.at[idx, 'ALERTA'] = 'N'
            continue
        
        # 4. Caso seja DSR
        elif situacao_primeira['tipo'] == 'dsr':
            df_transformed.at[idx, 'ALERTA'] = 'N'
            continue

        # 5. Caso seja Sabado
        elif dia_semana == 'Sabado':
            count = 0
            # Verificar se há 2 marcações (1a E. e 1a S.)
            if not is_empty(primeira_entrada): 
                df_transformed.at[idx, 'ENTRADA'] = 'OK'
                count += 1
            if not is_empty(primeira_saida):
                df_transformed.at[idx, 'SAIDA'] = 'OK'
                count += 1
            if count < 2:
                df_transformed.at[idx, 'ALERTA'] = 'S'  # Alerta para marcações incompletas
            
            else: 
                df_transformed.at[idx, 'ALERTA'] = 'N'
        
        # 6. Outros dias da semana
        else:
            # Verificar se não há marcações
            marcacoes_vazias = (is_empty(primeira_entrada) and 
                              is_empty(primeira_saida) and 
                              is_empty(segunda_entrada) and 
                              is_empty(segunda_saida))
            
            if marcacoes_vazias:
                # Verificar se existe observação
                if not is_empty(observacao):
                    df_transformed.at[idx, 'ALERTA'] = 'N'
                else:
                    # Sem marcações e sem observação - possível ausência
                    df_transformed.at[idx, 'AUSENCIA'] = 'SIM'
                    df_transformed.at[idx, 'ALERTA'] = 'S'
            else:
                 
                marcacoes = [
                    ('ENTRADA', primeira_entrada),
                    ('SAIDA INTERVALO', primeira_saida),
                    ('VOLTA INTERVALO', segunda_entrada),
                    ('SAIDA', segunda_saida)
                ]

                # Inicializa contador de marcações válidas
                qtd_marcacoes = 0

                # Processa as marcações
                for nome_campo, valor in marcacoes:
                    if not is_empty(valor):
                        df_transformed.at[idx, nome_campo] = 'OK'
                        qtd_marcacoes += 1

                # Define alerta com base na quantidade de marcações
                df_transformed.at[idx, 'ALERTA'] = 'N' if qtd_marcacoes >= 4 else 'S'                
    
    return df_transformed


def salvar_tabelas_concatenadas(tabelas: List[pd.DataFrame], lista_colaboradores, 
                                nome_arquivo: str = "ponto_consolidado.xlsx"):
    """
    Concatena todas as tabelas e salva em um único arquivo Excel.

    Args:
        tabelas (List[pd.DataFrame]): Lista de tabelas para concatenar e salvar
        lista_colaboradores: Lista com nomes dos colaboradores
        nome_arquivo (str): Nome do arquivo Excel de saída
    """
    if not tabelas:
        print("Nenhuma tabela para salvar.")
        return

    # Adiciona coluna identificadora para cada tabela
    tabelas_com_origem = []
    if len(tabelas) != len(lista_colaboradores):
        raise ValueError(f"O número de tabelas ({len(tabelas)}) não corresponde ao número de colaboradores ({len(lista_colaboradores)}).")

    for i, tabela in enumerate(tabelas):
        tabela_copy = tabela.copy()
        # Adiciona colunas de identificação
        tabela_copy.insert(0, 'COLABORADOR', lista_colaboradores[i])
        tabela_copy.insert(1, 'Pagina_PDF', tabela.attrs.get('pagina', 'N/A'))
        tabelas_com_origem.append(tabela_copy)

    # Concatena todas as tabelas
    try:
        tabela_consolidada = pd.concat(tabelas_com_origem, ignore_index=True, sort=False)

        # # Salva no formato Excel
        # with pd.ExcelWriter(nome_arquivo, engine='openpyxl') as writer:
        #     # Aba principal com dados consolidados
        #     tabela_consolidada.to_excel(writer, sheet_name='Dados_Consolidados', index=False)

        # print(f"Tabelas consolidadas salvas em: {nome_arquivo}")
        # print(f"Total de registros consolidados: {len(tabela_consolidada)}")
        # print(f"Colunas na tabela consolidada: {list(tabela_consolidada.columns)}")

        return tabela_consolidada

    except Exception as e:
        # print(f"Erro ao salvar arquivo Excel: {str(e)}")
        # # Fallback para CSV se Excel falhar
        # nome_csv = nome_arquivo.replace('.xlsx', '.csv')
        # tabela_consolidada = pd.concat(tabelas_com_origem, ignore_index=True, sort=False)
        # tabela_consolidada.to_csv(nome_csv, index=False, encoding='utf-8-sig')
        # print(f"Arquivo salvo como CSV alternativo: {nome_csv}")
        return tabela_consolidada


def main(caminho_pdf, colaboradores, lista_gestores=None):
    """
    Função principal para processar espelhos de ponto.
    
    Args:
        caminho_pdf (str): Caminho para o arquivo PDF
        colaboradores (list): Lista de nomes dos colaboradores
        lista_gestores (list): Lista de nomes dos gestores (opcional)
    
    Returns:
        pd.DataFrame: Tabela processada final
    """
    try:
        # Extrai as tabelas
        print("Iniciando extração das tabelas...")
        tabelas = extrair_tabelas_espelho_ponto(caminho_pdf)

        if not tabelas:
            print("Nenhuma tabela com as colunas 'Data' e 'Observação' foi encontrada.")
            return

        # Processa células mescladas em cada tabela
        print("Processando células mescladas...")
        tabelas_processadas = []
        for i, tabela in enumerate(tabelas):
            tabela_processada = processar_celulas_mescladas(tabela)
            tabelas_processadas.append(tabela_processada)

        # Limpa e processa horários, preservando informações especiais
        print("Limpando e processando horários...")
        tabelas_limpas = []
        for tabela in tabelas_processadas:
            tabela_limpa = limpar_e_converter_horarios(tabela)
            tabelas_limpas.append(tabela_limpa)

        # Transforma as tabelas adicionando colunas de controle
        print("Transformando tabelas...")
        tabelas_transformadas = []
        for i, tabela in enumerate(tabelas_limpas):
            # Passa o nome do colaborador e a lista de gestores para a função
            nome_colaborador = colaboradores[i] if i < len(colaboradores) else None
            tabela_transformada = transformar_ponto(tabela, nome_colaborador, lista_gestores)
            tabelas_transformadas.append(tabela_transformada)

        # Salva as tabelas consolidadas
        print("Salvando tabelas consolidadas...")
        tabela_final = salvar_tabelas_concatenadas(tabelas_transformadas,
                                                   colaboradores)

        print("Processamento concluído com sucesso!")
        return tabela_final[['Pagina_PDF', 'Dia', '1a E.', '1a S.', '2a E.',
                    '2a S.', '3a E.', '3a S.', 'Abono', 'Observação', 'Data', 'COLABORADOR', 
                    'AUSENCIA', 'ENTRADA', 'SAIDA INTERVALO', 'VOLTA INTERVALO', 'SAIDA', 'ALERTA']]

    except FileNotFoundError:
        print(f"Erro: Arquivo {caminho_pdf} não encontrado.")
    except Exception as e:
        print(f"Erro durante o processamento: {str(e)}")
        import traceback
        traceback.print_exc()