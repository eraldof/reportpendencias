import pandas as pd
import pdfplumber
from typing import List, Tuple, Optional, Dict, Any
import nomes_colaboradores
from datetime import datetime, timedelta, time

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
        return None, None
    
    # Sábado usa colunas diferentes (ENTRADA.1, SAIDA.1)
    if dia_semana.lower() in horario_colab['PERIODO.1'].iloc[0]:
        entrada_prog = horario_colab['ENTRADA.1'].iloc[0]
        saida_prog = horario_colab['SAIDA.1'].iloc[0]
    elif dia_semana.lower() in horario_colab['PERIODO'].iloc[0]:
        entrada_prog = horario_colab['ENTRADA'].iloc[0]
        saida_prog = horario_colab['SAIDA'].iloc[0]
    else:
        return None, None
    
    
    entrada_prog = converter_para_time(entrada_prog)
    saida_prog = converter_para_time(saida_prog)
    
    return entrada_prog, saida_prog


def calcular_diferenca_minutos(horario1: Optional[time], horario2: Optional[time]) -> Optional[float]:
    if horario1 is None or horario2 is None:
        return None
    
    today = datetime.today().date()
    dt1 = datetime.combine(today, horario1)
    dt2 = datetime.combine(today, horario2)
    
    diferenca = (dt1 - dt2).total_seconds() / 60
    return diferenca


def extrair_tabelas_espelho_ponto(caminho_pdf: str) -> List[pd.DataFrame]:
    tabelas_encontradas = []

    with pdfplumber.open(caminho_pdf) as pdf:
        for num_pagina, pagina in enumerate(pdf.pages):
            tabelas = pagina.extract_tables()

            for idx_tabela, tabela in enumerate(tabelas):
                if not tabela or len(tabela) < 2:
                    continue

                df = pd.DataFrame(tabela[1:], columns=tabela[0])
                df = df.fillna('')
                df = df.map(lambda x: str(x).strip() if x is not None else '')

                # Verifica se tabela tem colunas necessárias
                colunas = [col.strip().lower() for col in df.columns if col]
                tem_data = any('data' in col for col in colunas)
                tem_observacao = any('observação' in col or 'observacao' in col for col in colunas)

                if tem_data and tem_observacao:
                    df.attrs['pagina'] = num_pagina + 1
                    df.attrs['tabela_index'] = idx_tabela
                    df = df.loc[~(df == '').all(axis=1)]
                    df = df.reset_index(drop=True)
                    tabelas_encontradas.append(df)

    return tabelas_encontradas


def processar_celulas_mescladas(df: pd.DataFrame) -> pd.DataFrame:
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
                    'BANCO' in valor_str.upper()
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
    
    if '**' in valor_str and 'AUSENT' in valor_str:
        return {'tipo': 'ausente', 'valor_original': valor}
    
    if 'ISENTO' in valor_str and 'MARCAÇÃO' in valor_str:
        return {'tipo': 'isento', 'valor_original': valor}
    
    if 'FÉRIAS' in valor_str or 'FERIAS' in valor_str:
        return {'tipo': 'ferias', 'valor_original': valor}
    
    if 'D.S.R' in valor_str or 'DSR' in valor_str:
        return {'tipo': 'dsr', 'valor_original': valor}
    
    if 'REGISTRO NO POSITRON' in valor_str:
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


def transformar_ponto(df: pd.DataFrame, nome_colaborador: Optional[str] = None, lista_gestores: Optional[List[str]] = None) -> pd.DataFrame:
    df_transformed = df.copy()
    
    eh_gestor = False
    if lista_gestores and nome_colaborador:
        eh_gestor = nome_colaborador in lista_gestores
    
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
        
        situacao_primeira = identificar_situacoes_especiais(primeira_entrada)
        
        # Gestores sempre sem alerta
        if eh_gestor:
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


def salvar_tabelas_concatenadas(tabelas: List[pd.DataFrame], lista_colaboradores: List[str]) -> pd.DataFrame:
    if not tabelas:
        return pd.DataFrame()

    tabelas_com_origem = []
    if len(tabelas) != len(lista_colaboradores):
        raise ValueError(f"O número de tabelas ({len(tabelas)}) não corresponde ao número de colaboradores ({len(lista_colaboradores)}).")

    for i, tabela in enumerate(tabelas):
        tabela_copy = tabela.copy()
        tabela_copy.insert(0, 'COLABORADOR', lista_colaboradores[i])
        tabela_copy.insert(1, 'Pagina_PDF', tabela.attrs.get('pagina', 'N/A'))
        tabelas_com_origem.append(tabela_copy)

    tabela_consolidada = pd.concat(tabelas_com_origem, ignore_index=True, sort=False)
    return tabela_consolidada


def exec_parte1(caminho_pdf: str, colaboradores: List[str], lista_gestores: Optional[List[str]] = None) -> Optional[pd.DataFrame]:
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
            nome_colaborador = colaboradores[i] if i < len(colaboradores) else None
            tabela_transformada = transformar_ponto(tabela, nome_colaborador, lista_gestores)
            tabelas_transformadas.append(tabela_transformada)

        tabela_final = salvar_tabelas_concatenadas(tabelas_transformadas, colaboradores)

        return tabela_final[['Pagina_PDF', 'Dia', '1a E.', '1a S.', '2a E.',
                    '2a S.', '3a E.', '3a S.', 'Abono', 'Observação', 'Data', 'COLABORADOR', 
                    'AUSENCIA', 'ENTRADA', 'SAIDA INTERVALO', 'VOLTA INTERVALO', 'SAIDA', 'ALERTA']]

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
        entrada = converter_para_time(row.get('1a E.', ''))
        
        if dia_semana == 'Sabado':
            saida = converter_para_time(row.get('1a S.', ''))
        else:
            saida = converter_para_time(row.get('2a S.', ''))
            saida_almoco = converter_para_time(row.get('1a S.', ''))
            volta_almoco = converter_para_time(row.get('2a E.', ''))
        
        observacao = row.get('Observação', '')

        if nome in lista_gestores or observacao != '' or dia_semana == 'Domingo':
            tabela_ponto.at[idx, 'ALERTA'] = 'N'
            continue
        
        # Processamento para horário normal
       
        entrada_prog, saida_prog = obter_horario_programado(nome, dia_semana, horarios)
        
        if entrada_prog is None or saida_prog is None:
            tabela_ponto.at[idx, 'ALERTA'] = 'S/ ENTRADA PROGRAMADA'
            continue
    
        if entrada is None:
            tabela_ponto.at[idx, 'ENTRADA'] = 'SEM MARCAÇÃO'
            tabela_ponto.at[idx, 'ALERTA'] = 'S'
            continue

        if saida is None:
            tabela_ponto.at[idx, 'SAIDA'] = 'SEM MARCAÇÃO'
            tabela_ponto.at[idx, 'ALERTA'] = 'S'
            continue 

        if saida_almoco is None:
            tabela_ponto.at[idx, 'SAIDA INTERVALO'] = 'SEM MARCAÇÃO'
            tabela_ponto.at[idx, 'ALERTA'] = 'S'
            continue

        if volta_almoco is None:
            tabela_ponto.at[idx, 'VOLTA INTERVALO'] = 'SEM MARCAÇÃO'
            tabela_ponto.at[idx, 'ALERTA'] = 'S'
            continue

        if entrada > entrada_prog:
            tabela_ponto.at[idx, 'ALERTA'] = 'ATRASO'

        if saida < saida_prog:
            tabela_ponto.at[idx, 'ALERTA'] = 'S'
            tabela_ponto.at[idx, 'SAIDA'] = 'SAIDA ANTECIPADA'
        
        continue

    return tabela_ponto


def save(tabela_consolidada: pd.DataFrame, nome_arquivo: str) -> pd.DataFrame:
    with pd.ExcelWriter(nome_arquivo, engine='openpyxl') as writer:
        tabela_consolidada.to_excel(writer, sheet_name='Dados_Consolidados', index=False)
    return tabela_consolidada



def main(caminhopdf, nomes_colaboradores, gestores):
    resultado = exec_parte1(caminhopdf, nomes_colaboradores, gestores)
    if resultado is not None:
        resultado = exec_parte2(resultado, gestores)
    
    return resultado[['Pagina_PDF', 'Dia', '1a E.', '1a S.', '2a E.',
                    '2a S.', '3a E.', '3a S.', 'Abono', 'Observação', 'Data', 'COLABORADOR', 
                    'AUSENCIA', 'ENTRADA', 'SAIDA INTERVALO', 'VOLTA INTERVALO', 'SAIDA', 'ALERTA']]
