"""
pipeline_service.py

Contém as funções de serviço que representam os passos lógicos do pipeline.
"""
import pandas as pd
import numpy as np
import logging
import os

logger = logging.getLogger(__name__)

# ... (outras funções do arquivo permanecem iguais) ...
def obter_mes_do_usuario() -> int | None:
    try:
        mes_input_str = os.getenv("MES_INPUT")
        if mes_input_str is None:
            mes_input_str = input("🔸 Digite o número do mês desejado (0–12): ")
        return int(mes_input_str)
    except (ValueError, TypeError):
        logger.warning("Entrada de mês inválida.")
        return None

def filtrar_por_mes_string(df: pd.DataFrame, mes_input: int, nome_df: str, coluna_data: str) -> pd.DataFrame:
    meses_map = {
        0: 'Elb', 1: 'Jan', 2: 'Fev', 3: 'Mar', 4: 'Abr', 5: 'Mai', 6: 'Jun',
        7: 'Jul', 8: 'Ago', 9: 'Set', 10: 'Out', 11: 'Nov', 12: 'Dez'
    }
    mes_str = meses_map.get(mes_input)
    if not mes_str:
        logger.warning(f"Mês {mes_input} é inválido para filtrar {nome_df}.")
        return pd.DataFrame(columns=df.columns)

    logger.info(f"Filtrando '{nome_df}' pela coluna '{coluna_data}' para o mês: {mes_str}")
    filtro = df[coluna_data].astype(str).str.contains(f"/{mes_str}", case=False, na=False)
    df_filtrado = df[filtro]
    
    if df_filtrado.empty:
        logger.warning(f"Nenhum dado encontrado para o mês '{mes_str}' em '{nome_df}'.")
    return df_filtrado

def filtrar_por_mes_datetime(df: pd.DataFrame, mes_input: int, nome_df: str, coluna_data: str) -> pd.DataFrame:
    logger.info(f"Filtrando '{nome_df}' pela coluna '{coluna_data}' para o mês: {mes_input}")
    df_temp = df.copy()
    df_temp[coluna_data] = pd.to_datetime(df_temp[coluna_data], errors='coerce')
    df_temp = df_temp.dropna(subset=[coluna_data])
    filtro = df_temp[coluna_data].dt.month == mes_input
    df_filtrado = df_temp[filtro]
    if df_filtrado.empty:
        logger.warning(f"Nenhum dado encontrado para o mês '{mes_input}' em '{nome_df}'.")
    return df_filtrado


def aplicar_estrategias_de_apropriacao(
    df_receitas_classificadas: pd.DataFrame, 
    df_despesas_classificadas: pd.DataFrame, 
    df_cc: pd.DataFrame, 
    df_fechamento_do_mes: pd.DataFrame,
    df_fechamento_anual: pd.DataFrame
) -> pd.DataFrame:
    """
    Apropria a despesa REALIZADA e retorna um DataFrame com dados NUMÉRICOS,
    com logs detalhados para cada etapa do processo.
    """
    logger.info("Iniciando a função 'aplicar_estrategias_de_apropriacao'...")
    logger.debug(f"Shape df_receitas_classificadas: {df_receitas_classificadas.shape}")
    logger.debug(f"Shape df_despesas_classificadas: {df_despesas_classificadas.shape}")
    logger.debug(f"Shape df_cc: {df_cc.shape}")
    logger.debug(f"Shape df_fechamento_do_mes: {df_fechamento_do_mes.shape}")
    logger.debug(f"Shape df_fechamento_anual: {df_fechamento_anual.shape}")

    if df_despesas_classificadas.empty or df_receitas_classificadas.empty:
        logger.warning("DataFrames de despesas ou receitas estão vazios. Apropriação não pode ser concluída.")
        return pd.DataFrame()

    # --- 1. Calcular Proporções de Receita ---
    logger.info("--- Etapa 1: Calculando proporções de receita com base no orçado ---")
    df_receitas_pivot = pd.pivot_table(
        df_receitas_classificadas, values='VALOR_RECEITA_AJUSTADO', index='PROJETO',
        columns='DESCNVL4', aggfunc='sum', fill_value=0
    )
    logger.debug(f"DataFrame de receitas pivotado (df_receitas_pivot) tem shape: {df_receitas_pivot.shape}")
    
    df_receitas_pivot['Total_Receita_Orcada'] = df_receitas_pivot.sum(axis=1)
    colunas_receita_original = [col for col in df_receitas_pivot.columns if col != 'Total_Receita_Orcada']
    logger.debug(f"Colunas de receita original identificadas: {colunas_receita_original}")
    
    colunas_proporcao = []
    for col in colunas_receita_original:
        nome_coluna_prop = f"Prop_{col.replace(' ', '_')}"
        df_receitas_pivot[nome_coluna_prop] = np.divide(
            df_receitas_pivot[col], df_receitas_pivot['Total_Receita_Orcada'],
            out=np.zeros_like(df_receitas_pivot[col], dtype=float), where=df_receitas_pivot['Total_Receita_Orcada'] != 0
        )
        colunas_proporcao.append(nome_coluna_prop)
    logger.debug(f"Colunas de proporção criadas: {colunas_proporcao}")
    logger.debug(f"Cabeçalho do df_receitas_pivot com proporções:\n{df_receitas_pivot.head()}")
    logger.info("Etapa 1 concluída.")

    # --- 2. Preparar Dados Base ---
    logger.info("--- Etapa 2: Preparando dados base de despesas orçadas ---")
    df_despesas_agg = df_despesas_classificadas.groupby(['PROJETO', 'ACAO', 'TipoRegra']).agg(
        VALOR_DESPESA_AJUSTADO=('VALOR_DESPESA_AJUSTADO', 'sum'),
        FotografiaPPA=('FotografiaPPA', 'first')
    ).reset_index()
    logger.debug(f"Despesas agregadas (df_despesas_agg) tem shape: {df_despesas_agg.shape}")

    df_base = pd.merge(df_despesas_agg, df_cc, on=['PROJETO', 'ACAO'], how='left')
    logger.debug(f"Shape após merge com df_cc: {df_base.shape}")

    df_final = pd.merge(df_base, df_receitas_pivot.reset_index(), on='PROJETO', how='left')
    logger.debug(f"Shape após merge com df_receitas_pivot: {df_final.shape}")
    logger.info("Etapa 2 concluída.")

    # --- 3. Juntar Dados Financeiros ---
    logger.info("--- Etapa 3: Juntando dados financeiros realizados ---")
    if not df_fechamento_do_mes.empty:
        df_fechamento_mensal_agg = df_fechamento_do_mes.groupby('CC')['VALOR'].sum().reset_index().rename(columns={'VALOR': 'EXECUTADO/MES'})
        df_final = pd.merge(df_final, df_fechamento_mensal_agg, on='CC', how='left')
        logger.debug(f"Shape após merge com dados de fechamento do mês: {df_final.shape}")
    else:
        logger.warning("DataFrame de fechamento do mês está vazio. Coluna 'EXECUTADO/MES' será preenchida com 0.")
        df_final['EXECUTADO/MES'] = 0

    if not df_fechamento_anual.empty:
        # Renomeei a coluna aqui para evitar confusão.
        df_fechamento_anual_agg = df_fechamento_anual.groupby('CC')['VALOR'].sum().reset_index().rename(columns={'VALOR': 'ACUMULADO'})
        df_final = pd.merge(df_final, df_fechamento_anual_agg, on='CC', how='left')
        logger.debug(f"Shape após merge com dados de fechamento anual: {df_final.shape}")
    else:
        logger.warning("DataFrame de fechamento anual está vazio. Coluna 'ACUMULADO' será preenchida com 0.")
        df_final['ACUMULADO'] = 0
        
    df_final.fillna(0, inplace=True)
    logger.debug("Valores nulos preenchidos com 0 após merges financeiros.")
    logger.info("Etapa 3 concluída.")

    # --- 4. Apropriação ---
    logger.info("--- Etapa 4: Apropriando despesa realizada com base nas proporções ---")
    colunas_apropriadas_mes = []
    colunas_apropriadas_acum = []
    for col_receita, col_prop in zip(colunas_receita_original, colunas_proporcao):
        nome_aprop_mes = f"Aprop_Mes_{col_receita.replace(' ', '_')}"
        df_final[nome_aprop_mes] = df_final['EXECUTADO/MES'] * df_final[col_prop]
        colunas_apropriadas_mes.append(nome_aprop_mes)
        
        nome_aprop_acum = f"Aprop_Acum_{col_receita.replace(' ', '_')}"
        df_final[nome_aprop_acum] = df_final['ACUMULADO'] * df_final[col_prop]
        colunas_apropriadas_acum.append(nome_aprop_acum)
    logger.debug(f"Colunas de apropriação mensal criadas: {colunas_apropriadas_mes}")
    logger.debug(f"Colunas de apropriação acumulada criadas: {colunas_apropriadas_acum}")

    df_final['Total_Apropriado_Mes'] = df_final[colunas_apropriadas_mes].sum(axis=1)
    df_final['Total_Apropriado_Acumulado'] = df_final[colunas_apropriadas_acum].sum(axis=1)
    logger.debug("Colunas de totais de apropriação (mês e acumulado) calculadas.")
    logger.info("Etapa 4 concluída.")

    # --- 5. Cálculo dos Percentuais ---
    logger.info("--- Etapa 5: Calculando percentuais de composição da receita orçada ---")
    is_first_action = ~df_final.duplicated(subset=['PROJETO'], keep='first')
    for col in colunas_receita_original:
        df_final[col] = np.where(is_first_action, df_final[col], 0)
    
    total_receitas_por_linha_para_perc = df_final[colunas_receita_original].sum(axis=1)
    colunas_percentuais = []
    for col in colunas_receita_original:
        nome_coluna_percentual = f"{col} (%)"
        df_final[nome_coluna_percentual] = np.divide(
            df_final[col], total_receitas_por_linha_para_perc,
            out=np.zeros_like(df_final[col], dtype=float), where=total_receitas_por_linha_para_perc != 0
        )
        colunas_percentuais.append(nome_coluna_percentual)
    logger.debug(f"Colunas de percentual criadas: {colunas_percentuais}")
    logger.info("Etapa 5 concluída.")

    # --- 6. Organização Final ---
    logger.info("--- Etapa 6: Organizando e limpando o DataFrame final ---")
    logger.debug(f"Shape antes de filtrar 'Outra Regra': {df_final.shape}")
    df_final = df_final[df_final['TipoRegra'] != 'Outra Regra'].copy()
    logger.debug(f"Shape após filtrar 'Outra Regra': {df_final.shape}")
    
    colunas_finais_ordenadas = [
        'PROJETO', 'ACAO', 'CC', 'FotografiaPPA', 'TipoRegra', 'VALOR_DESPESA_AJUSTADO',
        *colunas_receita_original, *colunas_percentuais, 'EXECUTADO/MES', 'Total_Apropriado_Mes',
        *colunas_apropriadas_mes, 'ACUMULADO', 'Total_Apropriado_Acumulado', *colunas_apropriadas_acum,
    ]
    
    colunas_para_remover = colunas_proporcao + ['Total_Receita_Orcada']
    df_final = df_final.drop(columns=colunas_para_remover, errors='ignore')
    logger.debug(f"Removidas colunas intermediárias: {colunas_para_remover}")
    
    colunas_existentes = [col for col in colunas_finais_ordenadas if col in df_final.columns]
    
    logger.info(f"Processo finalizado. Retornando DataFrame com shape: {df_final[colunas_existentes].shape}")
    logger.debug(f"Cabeçalho do DataFrame final antes de retornar:\n{df_final[colunas_existentes].head()}")
    
    return df_final[colunas_existentes]

