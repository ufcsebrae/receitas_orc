"""
dataframe_processing.py

Este módulo contém funções utilitárias para processamento e manipulação
de DataFrames Pandas, como a renomeação padrão de colunas e a
normalização de texto.
"""

import pandas as pd
import unicodedata
import logging
import os 

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
        '[Iniciativa].[Iniciativas].[Iniciativa].[MEMBER_CAPTION]': 'PROJETO',
        '[Ação].[Ação].[Nome de Ação].[MEMBER_CAPTION]': 'ACAO',
        '[Natureza Orçamentária].[Código Estruturado 4 nível].[Código Estruturado 4 nível].[MEMBER_CAPTION]': 'CDGNVL4',
        '[Natureza Orçamentária].[Descrição de Natureza 4 nível].[Descrição de Natureza 4 nível].[MEMBER_CAPTION]': 'DESCNVL4',
        '[Measures].[ReceitaAjustado]': 'VALOR_RECEITA_AJUSTADO',
        '[Measures].[DespesaAjustado]': 'VALOR_DESPESA_AJUSTADO',
        
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



def classificar_projetos_em_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enriquece o DataFrame de projetos com uma classificação baseada em
    regras definidas em um arquivo de configuração externo (CSV).

    Args:
        df (pd.DataFrame): O DataFrame de entrada com a coluna 'PROJETO'.

    Returns:
        pd.DataFrame: O DataFrame original com a nova coluna 'TipoRegra'.
    """
    # --- Etapa 1: Carregar as regras do arquivo de configuração ---
    caminho_regras = os.path.join(
        os.path.dirname(__file__), '..', 'config', 'regras_classificacao.csv'
    )
    try:
        regras_df = pd.read_csv(caminho_regras)
    except FileNotFoundError:
        logger.error(f"Arquivo de regras não encontrado em: {caminho_regras}")
        df['TipoRegra'] = 'Erro: Arquivo de Regras Não Encontrado'
        return df
        
    # --- Etapa 2: Aplicar as regras usando um merge ---
    # pd.merge é a forma mais eficiente e "pandônica" de fazer essa junção.
    # Usamos um 'left' join para manter todos os projetos originais.
    df_classificado = pd.merge(df, regras_df, on='PROJETO', how='left')
    
    # --- Etapa 3: Definir um valor padrão para projetos não mapeados ---
    df_classificado['TipoRegra'] = df_classificado['TipoRegra'].fillna('Outra Regra')
    
    return df_classificado
