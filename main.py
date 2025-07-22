from conexao.configura_mdx import *
from conexao.funcoes_globais import selecionar_consulta_por_nome, salvar_no_financa
from dados.funcoes_tratamento import filtrar_df

def main():
    # Múltiplas consultas em uma única chamada
    resultados = selecionar_consulta_por_nome("RECEITAS_ORCADAS_2025, cc")
    # Espera o usuário confirmar para continuar o restante da execução
    input("\n🟡 Pressione ENTER para continuar com o filtro e o tratamento de dados...\n")


    # Continua com o tratamento do DataFrame de RECEITAS
    dfRECEITAS = resultados.get("RECEITAS_ORCADAS_2025")
    if dfRECEITAS is not None:
        dfRECEITAS = filtrar_df(dfRECEITAS)
        # salvar_no_financa(dfRECEITAS, "RECEITAS")  # descomente se quiser salvar no banco

if __name__ == "__main__":
    main()
