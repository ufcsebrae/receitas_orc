import os
from typing import Dict
from utils.sql_utils import carregar_sql
from config.config_connections import CONEXOES

SQL_DIR = os.path.join(os.path.dirname(__file__), '..', 'config', 'sql')

class Consulta:
    def __init__(self, titulo: str, sql_filename: str, tipo: str, conexao: str):
        self.titulo = titulo
        self.tipo = tipo
        self.sql_filename = sql_filename
        self.conexao = conexao  # ex: "FINANCA" ou "OLAP_SME"

        if conexao not in CONEXOES:
            raise ValueError(f"Conexão '{conexao}' não está definida em CONEXOES.py")

        self.info_conexao = CONEXOES[conexao]  # carrega automaticamente a configuração da conexão

    @property
    def sql(self):
        return carregar_sql(os.path.join(SQL_DIR, self.sql_filename))

# Dicionário contendo consultas SQL e MDX pré-definidas para uso
consultas: Dict[str, Consulta] = {
  
    "RECEITAS_ORCADAS_2025": Consulta(
        titulo="RECEITAS ORCADAS 2025 - SME - NA",
        tipo="mdx",
        sql_filename="receitas_orc_25.mdx",
        conexao="OLAP_SME"
        ),

    "cc": Consulta(
        titulo="Centro de Custo",
        tipo="sql",
        sql_filename="cc.sql",
        conexao="SPSVSQL39_HubDados"
        )
}