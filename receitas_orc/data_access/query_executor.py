import pandas as pd
from pyadomd import Pyadomd
import sqlalchemy

class CriadorDataFrame:
    def __init__(self, funcao_conexao, conexao: str, consulta: str, tipo: str = "sql"):
        self.funcao_conexao = funcao_conexao
        self.conexao = conexao
        self.consulta = consulta
        self.tipo = tipo.lower()

    def executar(self) -> pd.DataFrame:
        try:
            info_conexao = self.funcao_conexao(self.conexao)

            if self.tipo in ("sql", "azure_sql"):
                # Execute a consulta inteira, sem split, para garantir que DECLARE @DT funcione
                return pd.read_sql_query(self.consulta, info_conexao)

            elif self.tipo == "mdx":
                with Pyadomd(info_conexao) as conexao:
                    with conexao.cursor() as cursor:
                        cursor.execute(self.consulta)
                        dados = cursor.fetchall()
                        colunas = [col.name for col in cursor.description]
                        return pd.DataFrame(dados, columns=colunas)

            else:
                raise ValueError(f"Tipo de consulta '{self.tipo}' não suportado.")

        except Exception as erro:
            print(f"Erro ao executar a consulta ({self.tipo}): {erro}")
            return pd.DataFrame()

    def salva_no_Financa(self, df, table_name):
        """
        Salva um DataFrame no SQL Server no servidor SPSVSQL39, banco FINANCA.
        """
        try:
            # Usa sempre a conexão SPSVSQL39 para salvar
            from receitas_orc.services.global_services import funcao_conexao
            engine = funcao_conexao("SPSVSQL39")
            df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
            print(f"Dados salvos com sucesso na tabela {table_name}.")
        except Exception as e:
            print(f"Erro ao salvar no SQL: {e}")
