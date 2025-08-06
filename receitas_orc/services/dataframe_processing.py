"""
dataframe_processing.py

Este módulo contém funções utilitárias para processamento e manipulação
de DataFrames Pandas, incluindo filtragem baseada em mês, renomeação
de colunas, cálculo de percentuais e normalização de texto.
"""

import os
import pandas as pd
import unicodedata
import logging

# Configuração do logger para este módulo
logger = logging.getLogger(__name__)


def filtrar_df(df, mes_input=None):
    """
    Filtra o DataFrame com base no mês informado, considerando a presença das colunas
    'FotografiaPPA' e a coluna de valor 'Receita_Apropriada'.

    Args:
        df (pd.DataFrame): DataFrame original.
        mes_input (int, opcional): Número do mês de 0 a 12.
            Se None ou inválido, o filtro não será aplicado e um DataFrame vazio será retornado.

    Returns:
        pd.DataFrame: DataFrame filtrado com percentuais, ou um DataFrame vazio
                      se o mês for inválido ou colunas obrigatórias não forem encontradas.
    """
    df = df.copy().reset_index(drop=True)

    meses = {
        0: 'Elb', 1: 'Jan', 2: 'Fev', 3: 'Mar', 4: 'Abr', 5: 'Mai', 6: 'Jun',
        7: 'Jul', 8: 'Ago', 9: 'Set', 10: 'Out', 11: 'Nov', 12: 'Dez'
    }

    if mes_input is None:
        logger.warning(
            "Nenhum mês foi fornecido para filtragem em 'filtrar_df'. "
            "Retornando DataFrame vazio."
        )
        return df.head(0)

    if not isinstance(mes_input, int) or mes_input not in meses:
        logger.error(
            f"Mês inválido fornecido para 'filtrar_df': {mes_input}. "
            "Esperado um número inteiro entre 0 e 12."
        )
        return df.head(0)

    mes_str = meses[mes_input]
    logger.info(f"Filtrando dados para o mês: {mes_str} (Receitas Orçadas SME)")

    # ALTERAÇÃO: Removemos o loop e definimos explicitamente a coluna de valor.
    # A lógica de negócio agora depende desta coluna criada pela consulta SQL.
    col_valor = 'Receita_Apropriada'

    # A verificação agora é mais simples e direta.
    if 'FotografiaPPA' in df.columns and col_valor in df.columns:
        df['FotografiaPPA'] = df['FotografiaPPA'].astype(str)
        df[col_valor] = pd.to_numeric(df[col_valor], errors='coerce').fillna(0)

        # O resto da lógica de filtro funciona da mesma forma, mas agora com a coluna correta.
        filtro = df['FotografiaPPA'].str.contains(f"/{mes_str}", case=False, na=False) & (df[col_valor] > 0)
        df_filtrado = df[filtro].copy().reset_index(drop=True)
        
        # A função calcular_percentual_por_iniciativa receberá a coluna correta ('Receita_Apropriada')
        # e calculará os percentuais com base nela.
        df_resultado = calcular_percentual_por_iniciativa(df_filtrado, col_valor)
        
        return df_resultado
    else:
        logger.warning(
            f"Colunas obrigatórias ('FotografiaPPA' ou '{col_valor}') não encontradas. "
            "Nenhum filtro aplicado."
        )
        return df.head(0)



def renomear_colunas_padrao(df):
    """
    Renomeia colunas técnicas conhecidas para nomes mais amigáveis, se presentes.

    Args:
        df (pd.DataFrame): DataFrame original.

    Returns:
        pd.DataFrame: DataFrame com colunas renomeadas (quando aplicável).
    """
    colunas_renomeio = {
        '[PPA].[PPA com Fotografia].[Descrição de PPA com Fotografia].[MEMBER_CAPTION]': 'FotografiaPPA',
        '[Iniciativa].[Iniciativas].[Iniciativa].[MEMBER_CAPTION]': 'INICIATIVA',
        '[Ação].[Ação].[Nome de Ação].[MEMBER_CAPTION]': 'ACAO',
        '[Natureza Orçamentária].[Código Estruturado 4 nível].[Código Estruturado 4 nível].[MEMBER_CAPTION]': 'CDGNVL4',
        '[Natureza Orçamentária].[Descrição de Natureza 4 nível].[Descrição de Natureza 4 nível].[MEMBER_CAPTION]': 'DESCNVL4',
        '[Measures].[ReceitaAjustado]': 'VALOR_RECEITA',
        '[Measures].[DespesaAjustado]': 'VALOR_DESPESA'
    }
    return df.rename(columns={k: v for k, v in colunas_renomeio.items() if k in df.columns})


def calcular_percentual_por_iniciativa(df, coluna_valor):
    """
    Agrupa o DataFrame por INICIATIVA e DESCNVL4, somando os valores
    e calculando o percentual, preservando as outras colunas.

    Args:
        df (pd.DataFrame): DataFrame filtrado.
        coluna_valor (str): Nome da coluna de valor a ser usada.

    Returns:
        pd.DataFrame: DataFrame agregado com uma linha por grupo e a coluna 'PERCENTUAL'.
    """
    if df.empty:
        logger.warning("DataFrame vazio passado para calcular_percentual_por_iniciativa. Retornando vazio.")
        return df

    # 1. Definir o dicionário de agregação.
    #    - Somamos a coluna de valor.
    #    - Para todas as outras colunas, pegamos o 'first' valor, pois são idênticos dentro do grupo.
    agg_dict = {
        coluna_valor: 'sum'
    }
    colunas_para_manter = [col for col in df.columns if col not in ['INICIATIVA', 'DESCNVL4', coluna_valor]]
    for col in colunas_para_manter:
        agg_dict[col] = 'first'

    # 2. Agrupar e agregar. Isso criará um DataFrame com uma linha por grupo.
    df_agregado = df.groupby(['INICIATIVA', 'DESCNVL4']).agg(agg_dict).reset_index()

    # 3. Calcular o percentual no DataFrame já agregado.
    #    Isso garante que o cálculo é feito nos valores somados.
    total_por_iniciativa = df_agregado.groupby('INICIATIVA')[coluna_valor].transform('sum')

    # Evita divisão por zero
    df_agregado['PERCENTUAL'] = (df_agregado[coluna_valor] / total_por_iniciativa.replace(0, pd.NA)).fillna(0) * 100

    return df_agregado




def inserir_cc(df):
    """
    Insere a coluna 'CC' no DataFrame, preenchendo com 'CC' para todas as linhas.

    Args:
        df (pd.DataFrame): DataFrame original.

    Returns:
        pd.DataFrame: DataFrame com a coluna 'CC' adicionada.
    """
    df['CC'] = 'CC'
    return df


def normalizar_texto(texto):
    """
    Normaliza o texto removendo acentos, convertendo para minúsculas e removendo espaços/pontuação.

    Args:
        texto (str): Texto original.

    Returns:
        str: Texto normalizado.
    """
    if pd.isna(texto):
        return ""
    texto = str(texto).lower().strip()
    texto = unicodedata.normalize('NFKD', texto)
    texto = ''.join(c for c in texto if not unicodedata.combining(c))
    texto = texto.replace(',', '').replace('.', '')
    return texto


def selecionar_um_cc_por_projeto(df):
    """
    Seleciona apenas um centro de custo (linha) por INICIATIVA, descartando repetições.

    Args:
        df (pd.DataFrame): DataFrame com colunas ['INICIATIVA', ...]

    Returns:
        pd.DataFrame: DataFrame com uma linha por INICIATIVA.
    """
    return df.drop_duplicates(subset=["INICIATIVA"])
