"""
pipeline_service.py

Contém as funções de serviço que representam os passos lógicos do pipeline de
apropriação de receitas e despesas.
"""
import logging
import os
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

# Supondo que as classes de estratégia estejam neste caminho
from receitas_orc.strategies.base_strategy import BaseApropriacaoStrategy
from receitas_orc.strategies.csn_strategy import CSNStrategy
from receitas_orc.strategies.convenio_strategy import ConvenioStrategy
from receitas_orc.strategies.padrao_strategy import PadraoStrategy

# Configuração do Logger
logger = logging.getLogger(__name__)

# --- Constantes de Configuração ---
STRATEGY_MAP: Dict[str, BaseApropriacaoStrategy] = {
    "CI Único": CSNStrategy(),
    "CCTF": ConvenioStrategy(),
    "100% CSN": CSNStrategy(),
    "100% CONV": ConvenioStrategy(),
    "100% EB": PadraoStrategy()
}
DEFAULT_STRATEGY: BaseApropriacaoStrategy = PadraoStrategy()

FINAL_COLUMN_ORDER: List[str] = [
    'PROJETO', 'ACAO', 'CC', 'FotografiaPPA', 'TipoRegra', 'Despesa_Orcada',
    'Total_Receita_Orcada', 'Despesa_Realizada_Mes', 'Total_Apropriado_Mes',
    'Despesa_Acumulada_Ano', 'Total_Apropriado_Acumulado',
]

# --- Funções Utilitárias de Input e Filtro ---

def obter_mes_do_usuario() -> Optional[int]:
    """Obtém o mês de referência do usuário."""
    try:
        mes_input_str = os.getenv("MES_INPUT")
        if mes_input_str is None:
            mes_input_str = input("🔸 Digite o número do mês desejado (1–12): ")
        return int(mes_input_str)
    except (ValueError, TypeError):
        logger.warning("Entrada de mês inválida. Deve ser um número inteiro.")
        return None

def filtrar_por_mes_string(df: pd.DataFrame, mes_input: int, nome_df: str, coluna_data: str) -> pd.DataFrame:
    """Filtra um DataFrame cuja data está em formato de string (ex: 'dd/Mmm')."""
    meses_map = {
        1: 'Jan', 2: 'Fev', 3: 'Mar', 4: 'Abr', 5: 'Mai', 6: 'Jun',
        7: 'Jul', 8: 'Ago', 9: 'Set', 10: 'Out', 11: 'Nov', 12: 'Dez'
    }
    mes_str = meses_map.get(mes_input)
    if not mes_str:
        logger.warning(f"Mês {mes_input} é inválido para filtrar {nome_df}.")
        return pd.DataFrame(columns=df.columns)
    
    logger.info(f"Filtrando '{nome_df}' pela coluna '{coluna_data}' para o mês: {mes_str}")
    filtro = df[coluna_data].astype(str).str.contains(f"/{mes_str}", case=False, na=False)
    df_filtrado = df[filtro].copy()
    if df_filtrado.empty:
        logger.warning(f"Nenhum dado encontrado para o mês '{mes_str}' em '{nome_df}'.")
    return df_filtrado

def filtrar_por_mes_datetime(df: pd.DataFrame, mes_input: int, nome_df: str, coluna_data: str) -> pd.DataFrame:
    """Filtra um DataFrame convertendo a coluna de data para datetime."""
    logger.info(f"Filtrando '{nome_df}' pela coluna '{coluna_data}' para o mês: {mes_input}")
    df_temp = df.copy()
    df_temp[coluna_data] = pd.to_datetime(df_temp[coluna_data], errors='coerce')
    df_temp.dropna(subset=[coluna_data], inplace=True)
    
    filtro = df_temp[coluna_data].dt.month == mes_input
    df_filtrado = df_temp[filtro]
    if df_filtrado.empty:
        logger.warning(f"Nenhum dado encontrado para o mês '{mes_input}' em '{nome_df}'.")
    return df_filtrado

# --- Funções Auxiliares da Orquestração ---

def _preparar_dados_base(
    df_receitas_classificadas: pd.DataFrame,
    df_despesas_classificadas: pd.DataFrame,
    df_cc: pd.DataFrame,
    df_fechamento_do_mes: pd.DataFrame,
    df_fechamento_anual: pd.DataFrame
) -> tuple[pd.DataFrame, list[str]]:
    """Prepara, agrega e une todos os DataFrames de entrada em uma base única para análise."""
    logger.info("--- Etapa 1: Preparando dados comuns para todas as estratégias ---")

    # CORREÇÃO: Usar o DataFrame de receitas ('df_receitas_classificadas') para o pivot.
    # Ele contém os detalhes necessários por 'DESCNVL4'.
    logger.info("1. Pivotando receitas para obter totais por fonte...")
    df_receitas_pivot = pd.pivot_table(
        df_receitas_classificadas,
        values='VALOR_RECEITA_AJUSTADO', 
        index='PROJETO',
        columns='DESCNVL4', 
        aggfunc='sum', 
        fill_value=0
    )
    colunas_para_somar = df_receitas_pivot.columns.drop('Total_Receita_Orcada', errors='ignore') # 'errors=ignore' evita erro se a coluna não existir
    
    df_receitas_pivot['Soma_Total'] = df_receitas_pivot[colunas_para_somar].sum(axis=1)
    print(df_receitas_pivot.info)
    df_receitas_pivot.to_excel('resultado_final_v1.xlsx', sheet_name='Resultado', index=False)


    logger.info("2. Agregando despesas...")
    df_despesas_agg = df_despesas_classificadas.groupby(['PROJETO', 'ACAO', 'TipoRegra']).agg(
        VALOR_DESPESA_AJUSTADO=('VALOR_DESPESA_AJUSTADO', 'sum'),
        FotografiaPPA=('FotografiaPPA', 'first')
    ).reset_index()

    df_despesas_agg['TOTAL_DESPESA_PROJETO'] = df_despesas_agg.groupby('PROJETO')['VALOR_DESPESA_AJUSTADO'].transform('sum')

    df_base = pd.merge(df_despesas_agg, df_cc, on=['PROJETO', 'ACAO'], how='left')
    df_final = pd.merge(df_base, df_receitas_pivot.reset_index(), on='PROJETO', how='left')

    df_final['Coeficiente_DespesaReceita'] = df_final['Soma_Total']/df_final['TOTAL_DESPESA_PROJETO']

    df_despesas_mensais  = df_fechamento_do_mes.groupby('CC').agg(
        VALOR_DESPESA_AJUSTADO=('VALOR', 'sum')
    ).reset_index()

    df_despesas_anual  = df_fechamento_anual.groupby('CC').agg(
        VALOR_DESPESA_AJUSTADO=('VALOR', 'sum')
    ).reset_index()

    df_final['TOTAL_DESPESA_EXECUTADO_MES'] = df_final['CC'].map(df_despesas_mensais.set_index('CC')['VALOR_DESPESA_AJUSTADO']).fillna(0)
    df_final['TOTAL_DESPESA_EXECUTADO_ANO'] = df_final['CC'].map(df_despesas_anual.set_index('CC')['VALOR_DESPESA_AJUSTADO']).fillna(0)

    df_final['TOTAL_DESPESA_EXECUTADO_MES_PROJETO'] = df_final.groupby('PROJETO')['TOTAL_DESPESA_EXECUTADO_MES'].transform('sum')

    df_final['TOTAL_DESPESA_EXECUTADO_ANO_PROJETO'] =df_final.groupby('PROJETO')['TOTAL_DESPESA_EXECUTADO_ANO'].transform('sum')
    
        

    logger.info("3. Juntando DataFrames base...")

    colunas_de_receita_a_mascarar = ['CSN Programas e Projetos Nacionais']+['Convênios, Subvenções e Auxílios']+['Empresas Beneficiadas']+['Soma_Total']+['TOTAL_DESPESA_PROJETO']+['TOTAL_DESPESA_EXECUTADO_MES_PROJETO']+['TOTAL_DESPESA_EXECUTADO_ANO_PROJETO']
    is_first_in_project = ~df_final.duplicated(subset=['PROJETO'], keep='first')

    #Definir a Regra Especial: identificar a linha específica para o projeto especial.
    is_special_project_target_row = (df_final['PROJETO'] == 'Suporte a Negócios - Remuneração de Recursos Humanos Relacionado a Negócios') & \
                                (df_final['UNIDADE'] == 'SP - Atendimento ao Cliente')
    
    #criando condicional final que combina as duas regras
    final_target_mask = np.where(
    df_final['PROJETO'] == 'Suporte a Negócios - Remuneração de Recursos Humanos Relacionado a Negócios', # Se for o projeto especial...
    is_special_project_target_row,                                        # ... use a máscara da regra especial.
    is_first_in_project                                                   # ... senão, use a máscara da regra geral.
)

    for col in colunas_de_receita_a_mascarar:
    # Verifica se a coluna existe em df_final para evitar erros
        if col in df_final.columns:
            df_final[col] = np.where(
                final_target_mask ,  # Condição: a linha é o alvo?
                df_final[col],        # Sim: mantém o valor original
                0                     # Não: substitui por 0
        )


    return df_final

def _get_strategy(tipo_regra: str) -> BaseApropriacaoStrategy:
    """Retorna a instância da estratégia com base no TipoRegra."""
    strategy = STRATEGY_MAP.get(tipo_regra, DEFAULT_STRATEGY)
    logger.debug(f"Para TipoRegra '{tipo_regra}', estratégia selecionada: {strategy.__class__.__name__}")
    return strategy

def _executar_estrategias(df_preparado: pd.DataFrame) -> pd.DataFrame:
    """Aplica a estratégia de apropriação correta para cada grupo de projeto."""
    logger.info("--- Etapa 2: Mapeando e executando a estratégia correta para cada projeto ---")
    if df_preparado.empty:
        logger.warning("DataFrame preparado está vazio. Pulando execução das estratégias.")
        return pd.DataFrame()
        
    resultados = []

    # 1. Itere sobre cada 'TipoRegra' única presente no DataFrame.
    for tipo_regra in df_preparado['TipoRegra'].unique():
    
    # 2. Obtenha a estratégia correspondente para esta regra.
        strategy = _get_strategy(tipo_regra)
    
    # 3. Crie um sub-DataFrame contendo todas as linhas (de múltiplos projetos) que usam esta regra.
        df_subset_por_regra = df_preparado[df_preparado['TipoRegra'] == tipo_regra]
    
    # 4. Aplique a estratégia a este sub-DataFrame de uma só vez.
    #    
        resultado_subset = strategy.apropriar(df_subset_por_regra.copy())
    
    # 5. Adicione o resultado processado à lista.
        resultados.append(resultado_subset)

    # O restante do código, que concatena os resultados, permanece o mesmo.
    # df_com_resultados = pd.concat(resultados, ignore_index=True)
        
    if not resultados:
        logger.warning("Nenhum resultado gerado após a aplicação das estratégias.")
        return pd.DataFrame()

    df_com_resultados = pd.concat(resultados, ignore_index=True)
    logger.info("Etapa 2 concluída: Estratégias aplicadas.")
    return df_com_resultados

def _finalizar_e_formatar_dataframe(df_processado: pd.DataFrame) -> pd.DataFrame:
    """Realiza cálculos finais, limpeza visual e formatação do DataFrame de saída."""
    if df_processado.empty:
        return df_processado
    pass

    return df_processado


# --- Função Principal de Orquestração ---
def aplicar_estrategias_de_apropriacao(
    df_receitas_classificadas: pd.DataFrame,
    df_despesas_classificadas: pd.DataFrame,
    df_cc: pd.DataFrame,
    df_fechamento_do_mes: pd.DataFrame,
    df_fechamento_anual: pd.DataFrame
) -> pd.DataFrame:
    """Orquestra o pipeline completo de apropriação de despesas."""
    logger.info("Iniciando a orquestração da apropriação com padrão Strategy...")
    
    # 1. Preparar dados
    df_preparado = _preparar_dados_base(
        df_receitas_classificadas, df_despesas_classificadas, df_cc,
        df_fechamento_do_mes, df_fechamento_anual
    )
    
    # 2. Executar estratégias
    df_com_resultados = _executar_estrategias(df_preparado)
    
    # 3. Finalizar e formatar
    df_final = _finalizar_e_formatar_dataframe(df_com_resultados)
    
    logger.info("Orquestração da apropriação concluída com sucesso.")
    return df_final
