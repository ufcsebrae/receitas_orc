import os
from receitas_orc.config.startup import AdomdConnection, Pyadomd
from receitas_orc.config.mdx_setup import setup_mdx_environment
from receitas_orc.config.config_connections import CONEXOES
from receitas_orc.services.global_services import selecionar_consulta_por_nome, salvar_no_financa
from receitas_orc.services.dataframe_processing import filtrar_df


def carregar_dados():
    """Faz a seleção das consultas necessárias."""
    return selecionar_consulta_por_nome("RECEITAS_ORCADAS_2025, cc")


def tratar_dados(df_receitas):
    """
    Trata os dados de receitas, aplicando filtro por mês.

    Se a variável de ambiente MES_INPUT estiver definida, ela será usada.
    Caso contrário, solicita via input.
    """
    try:
        mes_input = int(os.getenv("MES_INPUT", input("\n🔸 Digite o número do mês desejado (0–12): ")))
    except (ValueError, EOFError):
        print("[ERRO] Mês inválido ou entrada não fornecida.")
        return df_receitas  # ou retorne None para abortar

    return filtrar_df(df_receitas, mes_input=mes_input)


def executar_pipeline():
    """Executa o pipeline principal."""
    resultados = carregar_dados()

    try:
        input("\n[INFO] Pressione ENTER para continuar com o filtro e o tratamento de dados...\n")
    except EOFError:
        print("[INFO] Entrada ignorada (modo não interativo)")

    df_receitas = resultados.get("RECEITAS_ORCADAS_2025")
    if df_receitas is not None:
        df_filtrado = tratar_dados(df_receitas)
        # salvar_no_financa(df_filtrado, "RECEITAS")  # Descomente se necessário
    else:
        print("[ERRO] Consulta 'RECEITAS_ORCADAS_2025' não retornou dados.")


def main():
    executar_pipeline()


if __name__ == "__main__":
    main()
