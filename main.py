from conexao.configura_mdx import *
from conexao.funcoes_globais import selecionar_consulta_por_nome,salvar_no_financa

def main():
   
    query = "RECEITAS_ORCADAS_2025"
    dfRECEITAS = selecionar_consulta_por_nome(query)
    print(dfRECEITAS.head())
    ##salvar_no_financa(dfRECEITAS, "RECEITAS") 
    ##use -- salvar_no_financa(dfReceitas, "RECEITAS") para salvar no banco de dados FINANCA
    
if __name__ == "__main__":
    main()
