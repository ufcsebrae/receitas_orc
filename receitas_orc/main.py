"""
main.py

Este é o ponto de entrada principal para o pipeline de processamento de
receitas orçamentárias. Ele orquestra a execução do pipeline chamando
os serviços apropriados em uma sequência lógica.
"""
import logging
import pandas as pd
import numpy as np

# Importações do seu projeto
from receitas_orc.config.mdx_setup import setup_mdx_environment
from receitas_orc.services.global_services import selecionar_consulta_por_nome
from receitas_orc.services.dataframe_processing import renomear_colunas_padrao, classificar_projetos_em_dataframe
from receitas_orc.services import pipeline_service

# --- Configurações Globais ---
# ATENÇÃO: Substitua pelo caminho EXATO e REAL da sua DLL.

DLL_PATH = r"C:\Microsoft.AnalysisServices.AdomdClient.dll"
RESULT_FILE_NAME = "resultado_pipeline.xlsx"

# --- Configuração de Logging ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def executar_pipeline():
    """Orquestra a execução do pipeline de dados."""
    logger.info("🚀 Iniciando pipeline de execução...")
    
    try:
        setup_mdx_environment(DLL_PATH)
        logger.info("Ambiente MDX inicializado com sucesso.")
    except Exception as e:
        logger.error(f"❌ Falha crítica ao inicializar ambiente MDX: {e}", exc_info=True)
        return

    # Etapa 1: Carregar e preparar dados brutos
    logger.info("--- Etapa 1: Carregando dados brutos ---")
    resultados = selecionar_consulta_por_nome("RECEITAS_ORCADAS_2025, cc, acoes, FatoFechamento")
    df_orcadas = resultados.get("RECEITAS_ORCADAS_2025")
    df_acoes = resultados.get("acoes")
    df_cc = resultados.get("cc")
    df_FatoFechamento = resultados.get("FatoFechamento")
  
    # Verifica se os DataFrames essenciais foram carregados corretamente
    logger.info(f"DataFrames carregados: {list(resultados.keys())}")
    if any(df is None or df.empty for df in [df_orcadas, df_acoes, df_cc]):
        logger.error("Falha ao carregar DataFrames essenciais (orcadas, acoes, cc). Encerrando.")
        return

    logger.info("Renomeando colunas...")
    df_orcadas = renomear_colunas_padrao(df_orcadas)
    df_acoes = renomear_colunas_padrao(df_acoes)
    df_cc = renomear_colunas_padrao(df_cc)
    
    # Etapa 2: Obter o mês do usuário
    logger.info("--- Etapa 2: Obtendo mês de referência ---")
    mes_selecionado = pipeline_service.obter_mes_do_usuario()
    if mes_selecionado is None:
        logger.warning("Nenhum mês válido selecionado. Encerrando o pipeline.")
        return

    # Etapa 3: Filtrar os dados de origem pelo mês selecionado
    logger.info(f"--- Etapa 3: Filtrando dados para o mês {mes_selecionado} ---")
    df_despesas_do_mes = pipeline_service.filtrar_dataframe_por_mes(df_acoes, mes_selecionado, "Despesas","FotografiaPPA")
    df_receitas_do_mes = pipeline_service.filtrar_dataframe_por_mes(df_orcadas, mes_selecionado, "Receitas","FotografiaPPA")
    df_fatofechamento_do_mes = pipeline_service.filtrar_dataframe_por_mes(df_FatoFechamento, mes_selecionado, "Fechamento","DATA")

    #Etapa extra: Classificar projetos
    df_receitas_do_mes = classificar_projetos_em_dataframe(df_receitas_do_mes)
    print(df_receitas_do_mes.to_markdown(index=False))
    
    print(df_fatofechamento_do_mes.to_markdown(index=False))
    # Etapa 4: Processar os dados já filtrados
    logger.info("--- Etapa 4: Processando e juntando dados ---")
    df_acoes_agg = pipeline_service.agregar_despesas_por_acao(df_despesas_do_mes)
    df_acoes_ranqueadas = pipeline_service.ranquear_acoes_e_juntar_cc(df_acoes_agg, df_cc)
    
    # Junta com receitas e fechamento ANTES de apropriar a receita
    df_com_receitas = pipeline_service.juntar_com_receitas(df_acoes_ranqueadas, df_receitas_do_mes)
    df_intermediario = pipeline_service.juntar_com_fato_fechamento(df_com_receitas, df_fechamento_do_mes)

    # Etapa 5: Aplicar regra de negócio final
    logger.info("--- Etapa 5: Aplicando regra de apropriação de receita ---")
    df_resultado_final = pipeline_service.aplicar_apropriacao_de_receita(df_intermediario)
    
    # Etapa 6: Apresentar e salvar o resultado
    logger.info("--- Etapa 6: Gerando saída ---")
    logger.info("✅ Pipeline executado com sucesso.")
    print("\n📊 Resultado final:\n")
    print(df_resultado_final)
    df_resultado_final.to_excel(RESULT_FILE_NAME, index=False)
    logger.info(f"📁 Resultado exportado para '{RESULT_FILE_NAME}' com sucesso.")

def main():
    """Função principal para iniciar a execução do pipeline."""
    executar_pipeline()

if __name__ == "__main__":
    main()
