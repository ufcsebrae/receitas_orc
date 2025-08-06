"""
dataframe_processing.py

Este módulo contém funções utilitárias para processamento e manipulação
de DataFrames Pandas, como a renomeação padrão de colunas e a
normalização de texto.
"""

import pandas as pd
import unicodedata
import logging

# Configuração do logger para este módulo
logger = logging.getLogger(__name__)


def renomear_colunas_padrao(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renomeia colunas técnicas conhecidas de consultas MDX/SQL para nomes
    mais amigáveis e fáceis de usar no pandas.

    Args:
        df (pd.DataFrame): DataFrame original.

    Returns:
        pd.DataFrame: DataFrame com colunas renomeadas (quando aplicável).
    """
    if df is None or df.empty:
        logger.warning("DataFrame fornecido para renomeação está vazio ou nulo.")
        return df

    colunas_renomeio = {
        '[PPA].[PPA com Fotografia].[Descrição de PPA com Fotografia].[MEMBER_CAPTION]': 'FotografiaPPA',
        '[Iniciativa].[Iniciativas].[Iniciativa].[MEMBER_CAPTION]': 'INICIATIVA',
        '[Ação].[Ação].[Nome de Ação].[MEMBER_CAPTION]': 'ACAO',
        '[Natureza Orçamentária].[Código Estruturado 4 nível].[Código Estruturado 4 nível].[MEMBER_CAPTION]': 'CDGNVL4',
        '[Natureza Orçamentária].[Descrição de Natureza 4 nível].[Descrição de Natureza 4 nível].[MEMBER_CAPTION]': 'DESCNVL4',
        '[Measures].[ReceitaAjustado]': 'VALOR_RECEITA',
        '[Measures].[DespesaAjustado]': 'VALOR_DESPESA'
    }
    
    # Cria um dicionário apenas com as colunas que realmente existem no DataFrame
    colunas_para_renomear = {k: v for k, v in colunas_renomeio.items() if k in df.columns}
    
    if colunas_para_renomear:
        logger.debug(f"Renomeando colunas: {list(colunas_para_renomear.keys())}")
        return df.rename(columns=colunas_para_renomear)
    
    return df


def normalizar_texto(texto: str) -> str:
    """
    Normaliza uma string removendo acentos, convertendo para minúsculas e
    removendo espaços extras e algumas pontuações.

    Args:
        texto (str): Texto original.

    Returns:
        str: Texto normalizado.
    """
    if pd.isna(texto):
        return ""
    
    texto_str = str(texto).lower().strip()
    
    # Remove acentos
    texto_normalizado = unicodedata.normalize('NFKD', texto_str)
    texto_sem_acentos = ''.join(c for c in texto_normalizado if not unicodedata.combining(c))
    
    # Remove pontuações específicas (pode ser expandido se necessário)
    texto_limpo = texto_sem_acentos.replace(',', '').replace('.', '')
    
    return texto_limpo

