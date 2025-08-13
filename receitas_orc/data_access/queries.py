"""
queries.py

Este módulo define a estrutura de dados para as consultas (SQL e MDX)
utilizadas no projeto e organiza um dicionário de consultas pré-definidas.
Ele facilita o acesso centralizado e padronizado às definições de consulta.
"""

import os
from typing import Dict
from receitas_orc.utils.sql_utils import carregar_sql
from receitas_orc.config.config_connections import CONEXOES

SQL_DIR = os.path.join(os.path.dirname(__file__), '..', 'config', 'sql')


class Consulta:
    """
    Representa uma definição de consulta para um banco de dados SQL ou MDX.
    Encapsula informações como título, tipo, nome do arquivo SQL/MDX e conexão.
    """
    def __init__(self, titulo: str, sql_filename: str, tipo: str, conexao: str):
        """
        Inicializa uma nova instância de Consulta.

        Args:
            titulo (str): O título ou nome lógico da consulta.
            sql_filename (str): O nome do arquivo contendo a consulta SQL ou MDX.
            tipo (str): O tipo da consulta ('sql' ou 'mdx').
            conexao (str): O nome da conexão a ser usada, conforme definida em CONEXOES.

        Raises:
            ValueError: Se o nome da conexão não estiver definido em CONEXOES.
        """
        self.titulo = titulo
        self.tipo = tipo
        self.sql_filename = sql_filename
        self.conexao = conexao  # ex: "FINANCA" ou "OLAP_SME"

        if conexao not in CONEXOES:
            raise ValueError(f"Conexão '{conexao}' não está definida em CONEXOES.py")

        self.info_conexao = CONEXOES[conexao]  # carrega automaticamente a configuração da conexão

    @property
    def sql(self) -> str:
        """
        Propriedade que carrega o conteúdo SQL/MDX do arquivo sob demanda.

        Returns:
            str: O conteúdo da consulta SQL ou MDX.
        """
        return carregar_sql(os.path.join(SQL_DIR, self.sql_filename))

# Dicionário contendo consultas SQL e MDX pré-definidas para uso

consultas: Dict[str, Consulta] = {
    "RECEITAS_ORCADAS_2025": Consulta(
        titulo="RECEITAS ORCADAS 2025 - SME - NA",
        tipo="mdx",
        sql_filename="receitas_orc_25.mdx",
        conexao="OLAP_SME"
    ),
    "acoes": Consulta(
        titulo="ACOES - SME - NA",
        tipo="mdx",
        sql_filename="acoes.mdx",
        conexao="OLAP_SME"
    ),
    "cc": Consulta(
        titulo="Centro de Custo",
        tipo="sql",
        sql_filename="cc.sql",
        conexao="SPSVSQL39_HubDados"
    ),
    "FatoFechamento": Consulta(
        titulo="Fato Fechamento",
        tipo="sql",
        sql_filename="fatofechamento.sql",
        conexao="SPSVSQL39_FINANCA"
    )
}


