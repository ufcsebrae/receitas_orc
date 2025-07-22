from typing import Dict
from conexao.utils import carregar_sql
from conexao.conexoes import CONEXOES

class Consulta:
    def __init__(self, titulo: str, sql: str, tipo: str, conexao: str):
        self.titulo = titulo
        self.tipo = tipo
        self.sql = sql
        self.conexao = conexao  # ex: "FINANCA" ou "OLAP_SME"

        if conexao not in CONEXOES:
            raise ValueError(f"Conexão '{conexao}' não está definida em CONEXOES.py")

        self.info_conexao = CONEXOES[conexao]  # carrega automaticamente a configuração da conexão

# Dicionário contendo consultas SQL e MDX pré-definidas para uso
consultas: Dict[str, Consulta] = {
  
    "RECEITAS_ORCADAS_2025": Consulta(
        titulo="RECEITAS ORCADAS 2025 - SME - NA",
        tipo="mdx",
        sql=carregar_sql("conexao/consultas/receitas_orc_25.mdx"),
        conexao="OLAP_SME"
        )
}