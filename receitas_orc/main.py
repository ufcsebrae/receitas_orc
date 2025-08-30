"""
main.py

Ponto de entrada principal para o pipeline de processamento de
receitas orçamentárias. Orquestra a execução do pipeline.
"""
import logging
import pandas as pd

# Importações do projeto
from receitas_orc.config.mdx_setup import setup_mdx_environment
from receitas_orc.services.global_services import selecionar_consulta_por_nome
from receitas_orc.services.dataframe_processing import renomear_colunas_padrao, classificar_projetos_em_dataframe
from receitas_orc.services import pipeline_service

# --- Configurações Globais ---
DLL_PATH = r"C:\Microsoft.AnalysisServices.AdomdClient.dll"
RESULT_FILE_NAME = "resultado_pipeline.xlsx"

# Configuração de exibição do Pandas
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 20)
pd.set_option('display.width', 1200)

# --- Configuração de Logging ---
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
logger = logging.getLogger(__name__)


def executar_pipeline():
    """Orquestra a execução do pipeline e exporta o resultado formatado para Excel."""
    logger.info("🚀 Iniciando pipeline de execução...")
    
    try:
        setup_mdx_environment(DLL_PATH)
        logger.info("Ambiente MDX inicializado com sucesso.")
    except Exception as e:
        logger.error(f"❌ Falha crítica ao inicializar ambiente MDX: {e}", exc_info=True)
        return
    
    logger.info("--- Etapa 1: Carregando dados brutos ---")
    resultados = selecionar_consulta_por_nome("RECEITAS_ORCADAS_2025, cc, acoes, FatoFechamento")
    df_orcadas = resultados.get("RECEITAS_ORCADAS_2025")
    df_acoes = resultados.get("acoes")
    df_cc = resultados.get("cc")
    df_FatoFechamento_original = resultados.get("FatoFechamento")
    if any(df is None or df.empty for df in [df_orcadas, df_acoes, df_cc]):
        logger.error("Falha ao carregar DataFrames essenciais (orcadas, acoes, cc). Encerrando.")
        return

    logger.info("Renomeando colunas...")
    df_orcadas = renomear_colunas_padrao(df_orcadas)
    df_acoes = renomear_colunas_padrao(df_acoes)

    logger.info("--- Etapa 2: Obtendo mês de referência ---")
    mes_selecionado = pipeline_service.obter_mes_do_usuario()
    if mes_selecionado is None:
        return

    logger.info(f"--- Etapa 3: Filtrando dados para o mês {mes_selecionado} ---")
    df_despesas_do_mes = pipeline_service.filtrar_por_mes_string(df_acoes, mes_selecionado, "Despesas", "FotografiaPPA")
    df_receitas_do_mes = pipeline_service.filtrar_por_mes_string(df_orcadas, mes_selecionado, "Receitas", "FotografiaPPA")
    
    df_fechamento_do_mes = pd.DataFrame()
    df_fechamento_anual = pd.DataFrame()
    if df_FatoFechamento_original is not None and not df_FatoFechamento_original.empty:
        df_FatoFechamento_original['DATA'] = pd.to_datetime(df_FatoFechamento_original['DATA'])
        df_fechamento_do_mes = pipeline_service.filtrar_por_mes_datetime(df_FatoFechamento_original, mes_selecionado, "FatoFechamento", "DATA")
        df_fechamento_anual = df_FatoFechamento_original[df_FatoFechamento_original['DATA'].dt.month <= mes_selecionado]

    logger.info("--- Etapa 4: Classificando projetos ---")
    df_receitas_classificadas = classificar_projetos_em_dataframe(df_receitas_do_mes)
    df_despesas_classificadas = classificar_projetos_em_dataframe(df_despesas_do_mes)

    logger.info("--- Etapa 5: Aplicando lógica de negócio ---")
    df_resultado_final = pipeline_service.aplicar_estrategias_de_apropriacao(
        df_receitas_classificadas,
        df_despesas_classificadas,
        df_cc,
        df_fechamento_do_mes,
        df_fechamento_anual
    )
    
    #Filtrar o resultado final para manter apenas as linhas '100% CSN'
 
    if not df_resultado_final.empty and 'TipoRegra' in df_resultado_final.columns:
        logger.info("Filtrando o resultado final para manter apenas as regras '100% CSN'...")
        condicao_tipo_regra = (df_resultado_final['TipoRegra'] != 'Outra Regra')

        # 2. Defina a segunda condição (usando '!=' para "diferente de")
        condicao_despesa_anual = (df_resultado_final['CSN_APROPRIAR_ANUAL'] != 0)

        # 3. Aplique ambas as condições usando o operador '&'
        #    Cada condição precisa estar entre parênteses.
        df_resultado_final = df_resultado_final[condicao_tipo_regra & condicao_despesa_anual].copy()


    logger.info("--- Etapa 6: Gerando saída formatada para Excel ---")
    if df_resultado_final.empty:
        logger.warning("O resultado final está vazio (ou não contém regras '100% CSN'). Nenhum arquivo será gerado.")
    else:
        try:
            with pd.ExcelWriter(RESULT_FILE_NAME, engine='xlsxwriter') as writer:
                df_resultado_final.to_excel(writer, sheet_name='Resultado', index=False)
                
                workbook = writer.book
                worksheet = writer.sheets['Resultado']
                
                format_brl = workbook.add_format({'num_format': '#,##0.00'}) # Adicionado .00 para centavos
                format_pct = workbook.add_format({'num_format': '0.00%'})
                
                for col_idx, col_name in enumerate(df_resultado_final.columns):
                    if '(%)' in col_name:
                        worksheet.set_column(col_idx, col_idx, 15, format_pct) # Aumentado para 15
                    # Corrigido para não formatar colunas de texto
                    elif col_name not in ['PROJETO', 'ACAO', 'CC', 'FotografiaPPA_despesas', 'TipoRegra']:
                        worksheet.set_column(col_idx, col_idx, 18, format_brl)
            
            logger.info(f"✅ Pipeline executado e exportado com sucesso para '{RESULT_FILE_NAME}'")
            logger.info("O arquivo Excel contém os dados filtrados e formatados.")
        except Exception as e:
            logger.error(f"❌ Falha ao exportar para Excel: {e}", exc_info=True)




def main():
    """Função principal para iniciar a execução do pipeline."""
    executar_pipeline()

if __name__ == "__main__":
    main()

