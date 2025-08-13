"""
query_executor.py

Este módulo contém a classe CriadorDataFrame, responsável por executar
consultas SQL ou MDX e retornar os resultados como um DataFrame do pandas.
Ele lida com a lógica específica de execução para cada tipo de consulta.
"""

import pandas as pd
import logging
from typing import Callable

    
        # ... (resto do código)


# Adiciona uma instância do logger para este módulo
logger = logging.getLogger(__name__)


class CriadorDataFrame:
    """
    Executa uma consulta em um banco de dados e retorna um DataFrame do pandas.
    
    A classe abstrai a lógica de execução para diferentes tipos de
    bancos de dados (SQL Server, Azure SQL, MDX/Analysis Services).
    """

    def __init__(self, funcao_conexao: Callable, conexao: str, consulta: str, tipo: str = "sql"):
        """
        Inicializa o executor de consultas.

        Args:
            funcao_conexao (function): A função a ser chamada para obter um objeto de conexão.
            conexao (str): O nome da configuração de conexão a ser usada (ex: "FINANCA").
            consulta (str): A string da consulta SQL ou MDX a ser executada.
            tipo (str, optional): O tipo de consulta ('sql', 'azure_sql', 'mdx'). Default é "sql".
        """
        self.funcao_conexao = funcao_conexao
        self.conexao = conexao
        self.consulta = consulta
        self.tipo = tipo.lower()

    def executar(self) -> pd.DataFrame:
        """
        Executa a consulta e retorna o resultado como um DataFrame.

        Returns:
            pd.DataFrame: Um DataFrame contendo os resultados da consulta, ou um
                          DataFrame vazio em caso de erro.
        """
        try:
            info_conexao = self.funcao_conexao(self.conexao)

            if self.tipo in ("sql", "azure_sql"):
                # Execute a consulta inteira, sem split, para garantir que DECLARE @DT funcione
                return pd.read_sql_query(self.consulta, info_conexao)

            elif self.tipo == "mdx":
                # --- Importação Tardia (Lazy Import) ---
                # Importamos Pyadomd apenas no momento do uso. Isso é crucial
                # porque garante que a função setup_mdx_environment() em main.py
                # já tenha sido executada e preparado o ambiente .NET (CLR).
                # Se importássemos no topo do arquivo, o Python tentaria carregar
                # Pyadomd antes que o ambiente estivesse pronto, causando um NameError.
                from pyadomd import Pyadomd

                with Pyadomd(info_conexao) as conexao:
                    with conexao.cursor() as cursor:
                        cursor.execute(self.consulta)
                        dados = cursor.fetchall()
                        colunas = [col.name for col in cursor.description]
                        return pd.DataFrame(dados, columns=colunas)

            else:
                raise ValueError(f"Tipo de consulta '{self.tipo}' não suportado.")

        except Exception as erro:
          
            # O argumento exc_info=True inclui o traceback completo no log
            logger.error(f"Erro ao executar a consulta ({self.tipo}): {erro}", exc_info=True)
            return pd.DataFrame()

