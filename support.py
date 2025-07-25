import pandas as pd
import pdfplumber
from typing import List, Dict, Optional
import re
from typing import List, Tuple, Optional, Dict, Any
from datetime import datetime, timedelta, time
import nomes_colaboradores


def import_horarios(uiid: str = '1Xo19_dftUc3GsTK-R6mKz8EAiLgGouBwKcxsu9ioJVc', gid: str = '806690514') -> pd.DataFrame:
    horarios = pd.read_csv(
        f'https://docs.google.com/spreadsheets/d/{uiid}/export?gid={gid}&format=csv'
    )

    def tolerancia(horario_str: str, minutos: int, subtract= False) -> Optional[time]:
        try:
            dt = datetime.strptime(horario_str, '%H:%M')
            if subtract:
                dt_com_tolerancia = dt - timedelta(minutes=minutos)
            else:
                dt_com_tolerancia = dt + timedelta(minutes=minutos)
            return dt_com_tolerancia.time()
        except:
            return None

    horarios['ENTRADA'] = horarios['ENTRADA'].apply(lambda x: tolerancia(x, 5))
    horarios['SAIDA'] = horarios['SAIDA'].apply(lambda x: tolerancia(x, 30, subtract=True))
    horarios['ENTRADA.1'] = horarios['ENTRADA.1'].apply(lambda x: tolerancia(x, 5))
    horarios['SAIDA.1'] = horarios['SAIDA.1'].apply(lambda x: tolerancia(x, 30, subtract=True))

    horarios_dias = {
        'SEG A SEX': 'Segunda, Terca, Quarta, Quinta, Sexta',
        'SEG A SAB': 'Segunda, Terca, Quarta, Quinta, Sexta, Sabado',
        'SEG A QUI': 'Segunda, Terca, Quarta, Quinta',
        'TER - QUI - SEX': 'Terca, Quinta, Sexta',
        'TER - QUI': 'Terca, Quinta',
        'SEG - QUA - SEX': 'Segunda, Quarta, Sexta',
        'SAB' : 'Sabado',
        'SEX' : 'Sexta'
    }

    horarios['PERIODO'] = horarios['PERIODO'].map(lambda x: horarios_dias.get(x, ''))
    horarios['PERIODO.1'] = horarios['PERIODO.1'].map(lambda x: horarios_dias.get(x, ''))

    return horarios

def converter_para_time(horario_str: Any, tolerancia: str = 5) -> Optional[time]:
    if pd.isna(horario_str) or horario_str == '' or horario_str is None:
        return None
    
    horario_str = str(horario_str).strip()
    
    if horario_str == '' or horario_str.lower() == 'nan':
        return None
    
    # Verifica palavras que indicam ausência/licença
    palavras_invalidas = ['ausente', 'falta', 'licença', 'férias', 'atestado', 'folga', 'dsr']
    if any(palavra in horario_str.lower() for palavra in palavras_invalidas):
        return None
    
    try:
        formatos = ['%H:%M:%S', '%H:%M', '%H.%M', '%H,%M']
        
        for formato in formatos:
            try:
                horario_dt = datetime.strptime(horario_str, formato)
                horario_dt += timedelta(minutes=tolerancia)  # Tolerância de 5 minutos
                return horario_dt.time()
            except ValueError:
                continue
        
        return None
        
    except:
        return None


def obter_horario_programado(colaborador: str, dia_semana: str, df_horarios: pd.DataFrame) -> Tuple[Optional[time], Optional[time]]:
    horario_colab = df_horarios[df_horarios['COLABORADORES'] == colaborador]
    
    if horario_colab.empty:
        return None, None, None
    
    # Sábado usa colunas diferentes (ENTRADA.1, SAIDA.1)
    if dia_semana.lower() in horario_colab['PERIODO.1'].iloc[0].lower():
        entrada_prog = horario_colab['ENTRADA.1'].iloc[0]
        saida_prog = horario_colab['SAIDA.1'].iloc[0]
    elif dia_semana.lower() in horario_colab['PERIODO'].iloc[0].lower():
        entrada_prog = horario_colab['ENTRADA'].iloc[0]
        saida_prog = horario_colab['SAIDA'].iloc[0]
    else:
        return None, None, None
    
    sab2t = horario_colab['SAB.2T'].iloc[0]
    entrada_prog = converter_para_time(entrada_prog)
    saida_prog = converter_para_time(saida_prog)
    
    return entrada_prog, saida_prog, sab2t



def extrair_tabelas_espelho_ponto(caminho_pdf: str) -> List[pd.DataFrame]:
    """
    Extrai tabelas de espelho de ponto de PDF, tratando casos onde
    as tabelas se estendem por múltiplas páginas.
    """
    tabelas_encontradas = []
    funcionarios_processados = {}
    
    with pdfplumber.open(caminho_pdf) as pdf:
        for num_pagina, pagina in enumerate(pdf.pages):
            # Extrai informações do funcionário da página
            info_funcionario = extrair_info_funcionario(pagina)
            
            # Extrai tabelas da página
            tabelas = pagina.extract_tables()
            
            for idx_tabela, tabela in enumerate(tabelas):
                if not tabela or len(tabela) < 2:
                    continue
                
                # Verifica se é uma tabela de ponto
                if not eh_tabela_ponto(tabela):
                    continue
                
                # Cria DataFrame
                df = criar_dataframe_ponto(tabela)
                if df is None or df.empty:
                    continue
                
                # Adiciona o nome do funcionário como coluna
                nome_funcionario = info_funcionario.get('nome', 'Nome não identificado') if info_funcionario else 'Nome não identificado'
                df.insert(0, 'COLABORADOR', nome_funcionario)
                funcao = info_funcionario.get('funcao', '') if info_funcionario else 'Funcao não identificada'
                df.insert(0, 'FUNCAO', funcao)

                # Processa mesclagem de células
                df = processar_celulas_mescladas(df)
                
                # Verifica se é continuação de uma tabela anterior
                chave_funcionario = gerar_chave_funcionario(info_funcionario)
                
                if chave_funcionario in funcionarios_processados:
                    # Concatena com a tabela anterior do mesmo funcionário
                    df_anterior = funcionarios_processados[chave_funcionario]
                    df_combinado = combinar_tabelas_funcionario(df_anterior, df)
                    funcionarios_processados[chave_funcionario] = df_combinado
                else:
                    # Primeira tabela deste funcionário
                    funcionarios_processados[chave_funcionario] = df
    
    # Converte o dicionário em lista de DataFrames
    for df in funcionarios_processados.values():
        # Remove linhas completamente vazias
        df = df.loc[~(df == '').all(axis=1)]
        df = df.reset_index(drop=True)
        tabelas_encontradas.append(df)
    
    return tabelas_encontradas

def extrair_info_funcionario(pagina) -> Optional[Dict]:
    """
    Extrai informações do funcionário do texto da página.
    """
    texto = pagina.extract_text()
    if not texto:
        return None
    
    info = {}
    
    # Busca por matrícula
    match_matricula = re.search(r'Matrícula:\s*(\d+\s*-\s*\d+)', texto)
    if match_matricula:
        info['matricula'] = match_matricula.group(1).strip()

    # Busca por Funcao
    match_funcao = re.search(r"Função:\s*(\d+)\s*-\s*(.+)", texto)
    if match_funcao:
        nome_funcao = match_funcao.group(2).strip()
        info['funcao'] = "MOTORISTA" if "MOTORISTA" in nome_funcao.upper() else ""

    # Busca por nome
    match_nome = re.search(r'Nome:\s*([A-Z\s]+)', texto)
    if match_nome:
        info['nome'] = match_nome.group(1).strip()
    
    # Busca por CPF
    match_cpf = re.search(r'CPF:\s*([\d\.\-]+)', texto)
    if match_cpf:
        info['cpf'] = match_cpf.group(1).strip()
    
    # Busca por período
    match_periodo = re.search(r'(\d{2}/\d{2}/\d{4})\s*-\s*(\d{2}/\d{2}/\d{4})', texto)
    if match_periodo:
        info['periodo_inicio'] = match_periodo.group(1)
        info['periodo_fim'] = match_periodo.group(2)
    
    return info if info else None

def eh_tabela_ponto(tabela: List[List]) -> bool:
    """
    Verifica se a tabela é uma tabela de espelho de ponto.
    """
    if not tabela or len(tabela) < 1:
        return False
    
    # Verifica o cabeçalho
    cabecalho = [str(cell).strip().lower() if cell else '' for cell in tabela[0]]
    
    # Procura por colunas características
    tem_data = any('data' in col for col in cabecalho)
    tem_entrada_saida = any(('1a e' in col or '1ª e' in col or '1a s' in col or '1ª s' in col) for col in cabecalho)
    
    # Exclui tabela de horários que contém coluna "turno"
    tem_turno = any('turno' in col for col in cabecalho)
    
    return tem_data and tem_entrada_saida and not tem_turno

def criar_dataframe_ponto(tabela: List[List]) -> Optional[pd.DataFrame]:
    """
    Cria um DataFrame a partir da tabela extraída.
    """
    try:
        # Encontra o cabeçalho válido
        cabecalho_idx = 0
        for i, linha in enumerate(tabela):
            if linha and any(str(cell).strip().lower() == 'data' for cell in linha if cell):
                cabecalho_idx = i
                break
        
        cabecalho = tabela[cabecalho_idx]
        dados = tabela[cabecalho_idx + 1:]
        
        # Remove células None e limpa o cabeçalho
        cabecalho_limpo = []
        for col in cabecalho:
            if col is None:
                cabecalho_limpo.append('')
            else:
                cabecalho_limpo.append(str(col).strip())
        
        # Cria o DataFrame
        df = pd.DataFrame(dados, columns=cabecalho_limpo)
        
        # Limpa os dados
        df = df.fillna('')
        df = df.map(lambda x: str(x).strip() if x is not None else '')
        
        # Remove linhas completamente vazias
        df = df.loc[~(df == '').all(axis=1)]
        
        return df if not df.empty else None
        
    except Exception as e:
        print(f"Erro ao criar DataFrame: {e}")
        return None

def gerar_chave_funcionario(info_funcionario: Optional[Dict]) -> str:
    """
    Gera uma chave única para identificar o funcionário.
    """
    if not info_funcionario:
        return "funcionario_sem_info"
    
    # Usa matrícula + CPF como chave única
    matricula = info_funcionario.get('matricula', '')
    cpf = info_funcionario.get('cpf', '')
    nome = info_funcionario.get('nome', '')
    
    return f"{matricula}_{cpf}_{nome}".replace(' ', '_')

def combinar_tabelas_funcionario(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """
    Combina duas tabelas do mesmo funcionário.
    """
    try:
        # Verifica se as colunas são compatíveis
        if list(df1.columns) != list(df2.columns):
            # Tenta alinhar as colunas
            df2 = alinhar_colunas(df1, df2)
        
        # Combina os DataFrames
        df_combinado = pd.concat([df1, df2], ignore_index=True)
        
        return df_combinado
        
    except Exception as e:
        print(f"Erro ao combinar tabelas: {e}")
        return df1  # Retorna a primeira tabela em caso de erro

def alinhar_colunas(df_ref: pd.DataFrame, df_target: pd.DataFrame) -> pd.DataFrame:
    """
    Alinha as colunas do df_target com as do df_ref.
    """
    colunas_ref = list(df_ref.columns)
    colunas_target = list(df_target.columns)
    
    # Se o número de colunas for diferente, ajusta
    if len(colunas_target) != len(colunas_ref):
        # Adiciona colunas vazias se necessário
        while len(colunas_target) < len(colunas_ref):
            colunas_target.append('')
        
        # Remove colunas extras se necessário
        colunas_target = colunas_target[:len(colunas_ref)]
    
    # Cria um novo DataFrame com as colunas alinhadas
    df_alinhado = pd.DataFrame(columns=colunas_ref)
    
    for i, linha in df_target.iterrows():
        nova_linha = {}
        for j, col_ref in enumerate(colunas_ref):
            if j < len(linha):
                nova_linha[col_ref] = linha.iloc[j]
            else:
                nova_linha[col_ref] = ''
        df_alinhado = pd.concat([df_alinhado, pd.DataFrame([nova_linha])], ignore_index=True)
    
    return df_alinhado

def processar_celulas_mescladas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processa células mescladas no DataFrame.
    """
    df_processado = df.copy()
    
    # Processa mesclagem horizontal
    for i in df_processado.index:
        colunas = list(df_processado.columns)
        for j, coluna in enumerate(colunas):
            valor = df_processado.loc[i, coluna]
            if pd.notna(valor) and str(valor).strip():
                valor_str = str(valor).strip()
                
                # Identifica padrões de mesclagem horizontal
                eh_mesclado = (
                    '**' in valor_str or
                    'AUSENTE' in valor_str.upper() or
                    'D.S.R' in valor_str.upper() or
                    'PERIODO' in valor_str.upper() or
                    'BANCO' in valor_str.upper() or
                    'FERIADO' in valor_str.upper() or
                    'HORARIO JUSTIFICADO' in valor_str.upper() or
                    'DESCONTO EM FOLHA' in valor_str.upper()
                )
                
                if eh_mesclado:
                    # Propaga valor para células vazias à direita
                    for k in range(j + 1, len(colunas)):
                        proxima_coluna = colunas[k]
                        proximo_valor = df_processado.loc[i, proxima_coluna]
                        if pd.isna(proximo_valor) or str(proximo_valor).strip() == '':
                            df_processado.loc[i, proxima_coluna] = valor_str
                        else:
                            break
    
    return df_processado

def identificar_situacoes_especiais(valor: Any) -> Dict[str, Any]:
    if pd.isna(valor) or valor == '':
        return {'tipo': 'vazio', 'valor_original': valor}
    
    valor_str = str(valor).strip().upper()
    
    if '**' in valor_str and 'AUSENT' in valor_str.upper():
        return {'tipo': 'ausente', 'valor_original': valor}
    
    if 'ISENTO' in valor_str or 'Isento de Marcação'.upper() in valor_str.upper():
        return {'tipo': 'isento', 'valor_original': valor}
    
    if 'FÉRIAS' in valor_str or 'FERIAS' in valor_str.upper():
        return {'tipo': 'ferias', 'valor_original': valor}
    
    if 'D.S.R' in valor_str or 'DSR' in valor_str.upper():
        return {'tipo': 'dsr', 'valor_original': valor}
    
    if 'REGISTRO NO POSITRON' in valor_str.upper():
        return {'tipo': 'registro_positron', 'valor_original': valor}
    
    return {'tipo': 'horario', 'valor_original': valor}


def limpar_e_converter_horarios(df: pd.DataFrame) -> pd.DataFrame:
    df_limpo = df.copy()
    colunas_horario = ['1a E.', '1a S.', '2a E.', '2a S.', '3a E.', '3a S.']
    
    if 'SITUACAO_ESPECIAL' not in df_limpo.columns:
        df_limpo['SITUACAO_ESPECIAL'] = ''
    
    def limpar_horario(valor: Any) -> Any:
        situacao = identificar_situacoes_especiais(valor)
        
        if situacao['tipo'] != 'horario':
            return valor
            
        valor_str = str(valor).strip()
        # Remove caracteres específicos do sistema de ponto
        valor_limpo = valor_str.replace('O', '').replace('I', '').replace('P', '')
        valor_limpo = valor_limpo.strip()
        
        if not valor_limpo:
            return None
            
        return valor_limpo

    # Identifica situações especiais por linha
    for idx, row in df_limpo.iterrows():
        situacoes_especiais = []
        primeira_entrada = row.get('1a E.', '')
        situacao_primeira = identificar_situacoes_especiais(primeira_entrada)
        
        if situacao_primeira['tipo'] in ['ausente', 'isento', 'ferias', 'dsr']:
            situacoes_especiais.append(situacao_primeira['tipo'])
        
        if situacoes_especiais:
            df_limpo.at[idx, 'SITUACAO_ESPECIAL'] = ', '.join(situacoes_especiais)
    
    for coluna in colunas_horario:
        if coluna in df_limpo.columns:
            df_limpo[coluna] = df_limpo[coluna].apply(limpar_horario)
    
    return df_limpo


def transformar_ponto(df: pd.DataFrame, lista_gestores: Optional[List[str]] = None) -> pd.DataFrame:
    df_transformed = df.copy()
    
    
    novas_colunas = ["AUSENCIA", "ENTRADA", "SAIDA INTERVALO", "VOLTA INTERVALO", "SAIDA", "ALERTA"]
    for col in novas_colunas:
        if col not in df_transformed.columns:
            df_transformed[col] = ""
    
    def is_empty(val: Any) -> bool:
        return pd.isna(val) or val == "" or val == " "
    
    for idx, row in df_transformed.iterrows():
        dia_semana = row.get('Dia', '')
        primeira_entrada = row.get('1a E.', '')
        primeira_saida = row.get('1a S.', '')
        segunda_entrada = row.get('2a E.', '')
        segunda_saida = row.get('2a S.', '')
        observacao = row.get('Observação', '')
        colaborador = row.get('COLABORADOR', '')  
        situacao_primeira = identificar_situacoes_especiais(primeira_entrada)
        
        # Gestores sempre sem alerta
        if colaborador in lista_gestores:
            df_transformed.at[idx, 'ALERTA'] = ''
            continue
        
        # Com observação, sem alerta
        if not is_empty(observacao):
            df_transformed.at[idx, 'ALERTA'] = ''
            continue

        elif dia_semana == 'Domingo':
            df_transformed.at[idx, 'ALERTA'] = ''
            continue
        
        elif situacao_primeira['tipo'] == 'ausente':
            df_transformed.at[idx, 'AUSENCIA'] = 'SIM'
            df_transformed.at[idx, 'ALERTA'] = 'S'
            continue
        
        elif situacao_primeira['tipo'] in ['isento', 'ferias', 'registro_positron']:
            df_transformed.at[idx, 'ALERTA'] = ''
            continue
        
        elif situacao_primeira['tipo'] == 'dsr':
            df_transformed.at[idx, 'ALERTA'] = ''
            continue

        # Sábado: verifica 2 marcações
        elif dia_semana == 'Sabado':
            count = 0
            if not is_empty(primeira_entrada): 
                df_transformed.at[idx, 'ENTRADA'] = 'OK'
                count += 1
            if not is_empty(primeira_saida):
                df_transformed.at[idx, 'SAIDA'] = 'OK'
                count += 1
            
            df_transformed.at[idx, 'ALERTA'] = '' if count >= 2 else 'S'
            continue
        
        # Outros dias da semana
        else:
            marcacoes_vazias = (is_empty(primeira_entrada) and 
                              is_empty(primeira_saida) and 
                              is_empty(segunda_entrada) and 
                              is_empty(segunda_saida))
            
            if marcacoes_vazias:
                if not is_empty(observacao):
                    df_transformed.at[idx, 'ALERTA'] = ''
                else:
                    df_transformed.at[idx, 'AUSENCIA'] = 'SIM'
                    df_transformed.at[idx, 'ALERTA'] = 'S'
            else:
                marcacoes = [
                    ('ENTRADA', primeira_entrada),
                    ('SAIDA INTERVALO', primeira_saida),
                    ('VOLTA INTERVALO', segunda_entrada),
                    ('SAIDA', segunda_saida)
                ]

                qtd_marcacoes = 0
                for nome_campo, valor in marcacoes:
                    if not is_empty(valor):
                        df_transformed.at[idx, nome_campo] = 'OK'
                        qtd_marcacoes += 1
                
                df_transformed.at[idx, 'ALERTA'] = '' if qtd_marcacoes >= 4 else 'S'
            continue               
    
    return df_transformed


def salvar_tabelas_concatenadas(tabelas: List[pd.DataFrame]) -> pd.DataFrame:
    if not tabelas:
        return pd.DataFrame()

    tabelas_com_origem = []
    for i, tabela in enumerate(tabelas):
        tabela_copy = tabela.copy()
        tabelas_com_origem.append(tabela_copy)

    tabela_consolidada = pd.concat(tabelas_com_origem, ignore_index=True, sort=False)
    return tabela_consolidada

def exec_parte1(caminho_pdf: str, lista_gestores: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
    try:
        tabelas = extrair_tabelas_espelho_ponto(caminho_pdf)

        if not tabelas:
            return None

        tabelas_processadas = []
        for i, tabela in enumerate(tabelas):
            tabela_processada = processar_celulas_mescladas(tabela)
            tabelas_processadas.append(tabela_processada)
            
        tabelas_limpas = []
        for tabela in tabelas_processadas:
            tabela_limpa = limpar_e_converter_horarios(tabela)
            tabelas_limpas.append(tabela_limpa)

        tabelas_transformadas = []
        for i, tabela in enumerate(tabelas_limpas):
            tabela_transformada = transformar_ponto(tabela, lista_gestores)
            tabelas_transformadas.append(tabela_transformada)

        tabela_final = salvar_tabelas_concatenadas(tabelas_transformadas)
        
        # FILTRAR APENAS LINHAS COM FUNÇÃO VAZIA OU NaN
        tabela_final = tabela_final[
            (tabela_final['FUNCAO'].isna()) | 
            (tabela_final['FUNCAO'] == '') | 
            (tabela_final['FUNCAO'] == 'Funcao não identificada')
        ]

        # Selecionar colunas
        tabela_final = tabela_final[['Dia', '1a E.', '1a S.', '2a E.',
                    '2a S.', '3a E.', '3a S.', 'Abono', 'Observação', 'Data', 'COLABORADOR', 
                    'AUSENCIA', 'ENTRADA', 'SAIDA INTERVALO', 'VOLTA INTERVALO', 'SAIDA', 'ALERTA']]
        
        tabela_final['COLABORADOR'] = tabela_final['COLABORADOR'].str.removesuffix(' C')

        return tabela_final

    except FileNotFoundError:
        print(f"Erro: Arquivo {caminho_pdf} não encontrado.")
        return None
    except Exception as e:
        print(f"Erro durante o processamento: {str(e)}")
        import traceback
        traceback.print_exc()
        return None
    

def exec_parte2(tabela_ponto: pd.DataFrame, lista_gestores: List[str] = nomes_colaboradores.GESTORES) -> pd.DataFrame:
    horarios = import_horarios()
    tabela_ponto = tabela_ponto.copy()
    
    for idx, row in tabela_ponto.iterrows():
        
        nome = row.get('COLABORADOR', '')
        dia_semana = row.get('Dia', '')
        ausencia = row.get('AUSENCIA', '')
        
        observacao = row.get('Observação', '')

        if nome in lista_gestores or observacao != '' or dia_semana == 'Domingo':
            tabela_ponto.at[idx, 'ALERTA'] = ''
            continue
        
        if ausencia == 'SIM':
            continue
        # Processamento para horário normal
       
        entrada_prog, saida_prog, sab2t = obter_horario_programado(nome, dia_semana, horarios)
        
        entrada = converter_para_time(row.get('1a E.', ''))
        
        if dia_semana == 'Sabado' and sab2t == 'N':
            saida = converter_para_time(row.get('1a S.', ''))

        elif dia_semana == 'Sabado' and sab2t == 'S':
            saida = converter_para_time(row.get('2a S.', ''))
            saida_almoco = converter_para_time(row.get('1a S.', ''))
            volta_almoco = converter_para_time(row.get('2a E.', ''))            
        else:
            saida = converter_para_time(row.get('2a S.', ''))
            saida_almoco = converter_para_time(row.get('1a S.', ''))
            volta_almoco = converter_para_time(row.get('2a E.', ''))


        if entrada_prog is None or saida_prog is None:
            tabela_ponto.at[idx, 'ALERTA'] = 'S/ ENTRADA PROGRAMADA'
            continue
    
        if entrada is None:
            tabela_ponto.at[idx, 'ENTRADA'] = 'SEM MARCAÇÃO'
            tabela_ponto.at[idx, 'ALERTA'] = 'S'
        else:
            if entrada > entrada_prog:
                tabela_ponto.at[idx, 'ENTRADA'] = 'ATRASO'
                tabela_ponto.at[idx, 'ALERTA'] = 'S'


        if saida is None:
            tabela_ponto.at[idx, 'SAIDA'] = 'SEM MARCAÇÃO'
            tabela_ponto.at[idx, 'ALERTA'] = 'S'
        else:
            if saida < saida_prog:
                tabela_ponto.at[idx, 'ALERTA'] = 'S'
                tabela_ponto.at[idx, 'SAIDA'] = 'SAIDA ANTECIPADA'
             
        if dia_semana != 'Sabado':
            if saida_almoco is None:
                tabela_ponto.at[idx, 'SAIDA INTERVALO'] = 'SEM MARCAÇÃO'
                tabela_ponto.at[idx, 'ALERTA'] = 'S'        
            if volta_almoco is None:
                tabela_ponto.at[idx, 'VOLTA INTERVALO'] = 'SEM MARCAÇÃO'
                tabela_ponto.at[idx, 'ALERTA'] = 'S'
        else:
            if sab2t == 'S':
                if saida_almoco is None:
                    tabela_ponto.at[idx, 'SAIDA INTERVALO'] = 'SEM MARCAÇÃO'
                    tabela_ponto.at[idx, 'ALERTA'] = 'S'        
                if volta_almoco is None:
                    tabela_ponto.at[idx, 'VOLTA INTERVALO'] = 'SEM MARCAÇÃO'
                    tabela_ponto.at[idx, 'ALERTA'] = 'S'
                    

        
        continue

    return tabela_ponto





def save(tabela_consolidada: pd.DataFrame, nome_arquivo: str) -> pd.DataFrame:
    with pd.ExcelWriter(nome_arquivo, engine='openpyxl') as writer:
        tabela_consolidada.to_excel(writer, sheet_name='Dados_Consolidados', index=False)
    return tabela_consolidada



def main(caminhopdf):
    resultado = exec_parte1(caminhopdf, lista_gestores= nomes_colaboradores.GESTORES)

    resultado = exec_parte2(resultado, lista_gestores= nomes_colaboradores.GESTORES)

    return resultado[['Dia','3a E.', '3a S.', 'Abono','Observação', '1a E.', '1a S.', '2a E.', '2a S.',
                'Data', 'COLABORADOR', 'AUSENCIA', 'ENTRADA',
                'SAIDA INTERVALO', 'VOLTA INTERVALO', 'SAIDA', 'ALERTA']]
