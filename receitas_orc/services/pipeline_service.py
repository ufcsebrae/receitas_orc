"""
pipeline_service.py

Este módulo contém as funções de serviço que representam os passos lógicos
do pipeline de processamento de receitas e despesas.
"""

import pandas as pd
from pandasql import sqldf
import numpy as np
import logging
import os

logger = logging.getLogger(__name__)


def obter_mes_do_usuario() -> int | None:
    """
    Obtém o mês a ser processado a partir de variável de ambiente ou input do usuário.
    """
    try:
        mes_input_str = os.getenv("MES_INPUT")
        if mes_input_str is None:
            mes_input_str = input("🔸 Digite o número do mês desejado (0–12): ")
        return int(mes_input_str)
    except (ValueError, TypeError):
        logger.warning("Entrada de mês inválida.")
        return None


def filtrar_dataframe_por_mes(df: pd.DataFrame, mes_input: int, nome_df: str) -> pd.DataFrame:
    """
    Filtra um DataFrame pela coluna 'FotografiaPPA' e um mês (formato string).
    """
    meses = {
        0: 'Elb', 1: 'Jan', 2: 'Fev', 3: 'Mar', 4: 'Abr', 5: 'Mai', 6: 'Jun',
        7: 'Jul', 8: 'Ago', 9: 'Set', 10: 'Out', 11: 'Nov', 12: 'Dez'
    }
    mes_str = meses.get(mes_input)
    
    if not mes_str:
        logger.warning(f"Mês {mes_input} é inválido para filtrar {nome_df}.")
        return pd.DataFrame(columns=df.columns)

    if 'FotografiaPPA' not in df.columns:
        logger.error(f"Coluna 'FotografiaPPA' não encontrada em '{nome_df}'. Não é possível filtrar por mês.")
        return df # Retorna o DF original se a coluna não existir

    logger.info(f"Filtrando '{nome_df}' pela coluna 'FotografiaPPA' para o mês: {mes_str}")
    
    filtro = df['FotografiaPPA'].str.contains(f"/{mes_str}", case=False, na=False)
    df_filtrado = df[filtro]
    
    if df_filtrado.empty:
        logger.warning(f"Nenhum dado encontrado para o mês '{mes_str}' em '{nome_df}'.")
        
    return df_filtrado


def agregar_despesas_por_acao(df_despesas_do_mes: pd.DataFrame) -> pd.DataFrame:
    """
    Agrupa as despesas JÁ FILTRADAS POR MÊS para ter uma linha por ação.
    Lida com DataFrames vazios de forma segura.
    """
    if df_despesas_do_mes.empty:
        logger.warning("DataFrame de despesas do mês está vazio. Pulando agregação.")
        # Retorna um DF vazio com as colunas esperadas para evitar erros posteriores
        return pd.DataFrame(columns=['INICIATIVA', 'ACAO', 'VALOR_DESPESA', 'FotografiaPPA'])

    logger.info("Agregando dados de despesas do mês selecionado...")
    df_agg = df_despesas_do_mes.groupby(['INICIATIVA', 'ACAO']).agg(
        VALOR_DESPESA=('VALOR_DESPESA', 'sum'),
        FotografiaPPA=('FotografiaPPA', 'first')
    ).reset_index()
    return df_agg


def ranquear_acoes_e_juntar_cc(df_acoes_agg: pd.DataFrame, df_cc: pd.DataFrame) -> pd.DataFrame:
    """
    Usa SQL para ranquear as ações agregadas e juntá-las com os centros de custo.
    """
    if df_acoes_agg.empty:
        logger.warning("DataFrame de ações agregadas está vazio. Pulando ranqueamento e junção com CC.")
        return df_acoes_agg
        
    logger.info("Preparando e ranqueando as ações agregadas...")
    query_acoes = """
        SELECT
            a.*,
            cc.CC,
            ROW_NUMBER() OVER(PARTITION BY a.INICIATIVA ORDER BY a.ACAO) AS Ordem_Acao
        FROM
            df_acoes_agg AS a
        LEFT JOIN
            df_cc AS cc ON (a.INICIATIVA || a.ACAO) = (cc.PROJETO || cc.ACAO);
    """
    return sqldf(query_acoes, locals())


def juntar_com_receitas(df_acoes_ranqueadas: pd.DataFrame, df_receitas_do_mes: pd.DataFrame) -> pd.DataFrame:
    """
    APENAS junta os dados de ações e receitas (ambos do mesmo mês).
    """
    if df_acoes_ranqueadas.empty:
        logger.warning("Não há ações ranqueadas para juntar com receitas.")
        return df_acoes_ranqueadas

    logger.info("Juntando ações e receitas do mês selecionado...")
    df_com_receita = pd.merge(
        df_acoes_ranqueadas,
        df_receitas_do_mes[['INICIATIVA', 'DESCNVL4', 'VALOR_RECEITA']],
        on='INICIATIVA',
        how='left'
    )
    return df_com_receita


def juntar_com_fato_fechamento(df_principal: pd.DataFrame, df_fechamento_do_mes: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega os dados de FatoFechamento do mês por 'CC' e os junta ao DataFrame principal.
    A normalização da coluna 'CC' já foi feita pela consulta SQL.
    """
    if df_fechamento_do_mes.empty:
        logger.warning("DataFrame 'FatoFechamento' do mês está vazio. Nenhuma informação de fechamento será adicionada.")
        df_principal['VALOR'] = 0 # Adiciona a coluna com 0s para consistência
        return df_principal

    logger.info("Agregando os dados diários de 'FatoFechamento' para o total do mês...")
    df_fechamento_mensal_agg = df_fechamento_do_mes.groupby('CC')['VALOR'].sum().reset_index()

    logger.info("Juntando resultado com dados agregados de 'FatoFechamento'...")
    df_com_fechamento = pd.merge(
        df_principal,
        df_fechamento_mensal_agg,
        on='CC',
        how='left'
    )
    
    df_com_fechamento['VALOR'] = df_com_fechamento['VALOR'].fillna(0)
    return df_com_fechamento

# Em receitas_orc/services/pipeline_service.py

# ... (outras funções) ...

def aplicar_apropriacao_de_receita(df_final: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica a regra de negócio para criar a coluna 'Receita_Apropriada' como etapa final.
    Agora lida com DataFrames vazios de forma segura.
    """
    # --- CORREÇÃO DE ROBUSTEZ ---
    if df_final.empty:
        logger.warning("DataFrame final está vazio. Não é possível aplicar apropriação de receita.")
        # Adiciona a coluna esperada para manter o schema e retorna
        df_final['Receita_Apropriada'] = pd.Series(dtype='float64')
        return df_final
    # --- FIM DA CORREÇÃO ---

    logger.info("Aplicando regra final de apropriação da receita...")
    
    mascara_primeira_acao = ~df_final.duplicated(subset=['INICIATIVA'], keep='first') & (df_final['Ordem_Acao'] == 1)

    df_final['Receita_Apropriada'] = np.where(
        mascara_primeira_acao,
        df_final['VALOR_RECEITA'].fillna(0),
        0
    )
    return df_final

