"""
global_services.py

Este módulo contém serviços globais para o projeto 'receitas_orc',
incluindo a configuração do logger, funções para estabelecer conexões
com diferentes tipos de bancos de dados (SQL, Azure SQL, MDX) e a
execução centralizada de consultas, além de funções para salvar dados.
"""

import logging
import time
import pandas as pd
import os
import sqlalchemy


from typing import Union, List, Dict
from urllib.parse import quote_plus



# Importações relativas para o projeto
# O setup_mdx_environment não será mais chamado aqui diretamente no nível do módulo
# from receitas_orc.config.mdx_setup import setup_mdx_environment
from receitas_orc.data_access.queries import CONEXOES, Consulta, consultas
from receitas_orc.data_access.query_executor import CriadorDataFrame

# A configuração do logger (basicConfig) foi movida para main.py.
# Aqui, apenas obtemos uma instância do logger.
logger = logging.getLogger("logger_financa")

# O caminho da DLL não deve ser hardcoded aqui, será gerenciado por main.py ou uma configuração centralizada
# dll_path = r"C:\Microsoft.AnalysisServices.AdomdClient.dll"
# setup_mdx_environment(dll_path) # Esta chamada é removida daqui

def funcao_conexao(nome_conexao: str) -> Union[sqlalchemy.engine.base.Connection, str]:
    """
    Retorna uma conexão SQLAlchemy com base nas informações da conexão especificada.
    Suporta conexões do tipo: 'sql', 'azure_sql' e 'mdx'.

    Args:
        nome_conexao (str): O nome da conexão conforme definido em CONEXOES.

    Returns:
        sqlalchemy.engine.base.Connection ou str: Objeto de conexão ou string de conexão para MDX.

    Raises:
        ValueError: Se o tipo de conexão não for suportado.
    """
    info = CONEXOES[nome_conexao]

    if info["tipo"] == "sql":
        servidor = info["servidor"]
        banco = info["banco"]
        driver = info["driver"]
        trusted = info.get("trusted_connection", False)
        trusted_str = "Trusted_Connection=yes;" if trusted else ""

        odbc_str = (
            f"DRIVER={driver};"
            f"SERVER={servidor};"
            f"DATABASE={banco};"
            f"{trusted_str}"
        )

        string_conexao = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}"
        return sqlalchemy.create_engine(string_conexao).connect()

    elif info["tipo"] == "azure_sql":
        servidor = info["servidor"]
        banco = info.get("banco", "")
        driver = info["driver"]
        authentication = info["authentication"]
        usuario = info.get("usuario")
        senha = info.get("senha")

        odbc_str = (
            f"DRIVER={driver};"
            f"SERVER={servidor},1433;"
            f"DATABASE={banco};"
            f"Authentication={authentication};"
        )

        if usuario and senha:
            odbc_str += f"UID={usuario};PWD={senha};"

        string_conexao = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}"
        return sqlalchemy.create_engine(string_conexao).connect()

    elif info["tipo"] == "mdx":
        return info["str_conexao"]

    else:
        raise ValueError("Tipo de conexão não suportado.")


def selecionar_consulta_por_nome(titulo: Union[str, List[str]]) -> Dict[str, pd.DataFrame]:
    """
    Executa uma ou mais consultas pelo nome lógico definido no dicionário `consultas`.
    Aceita:
        - String com nomes separados por vírgula
        - Lista de strings

    Args:
        titulo (str ou list): Nome(s) da(s) consulta(s) a ser(em) executada(s).

    Returns:
        Dict[str, DataFrame]: Dicionário com as chaves originais (nomes das consultas)
                              e os DataFrames resultantes.
    """
    if isinstance(titulo, str):
        nomes = [t.strip() for t in titulo.split(",")]
    elif isinstance(titulo, list):
        nomes = [t.strip() for t in titulo]
    else:
        raise ValueError("O parâmetro 'titulo' deve ser uma string ou uma lista de strings.")

    resultados = {}

    for nome in nomes:
        nome_original = nome.strip()

        inicio = time.perf_counter()
        logger.info(f"⛔️ Iniciando execução da consulta: '{nome_original}'")

        try:
            if nome_original in consultas:
                consulta = consultas[nome_original]
            elif nome_original.lower() in consultas:
                consulta = consultas[nome_original.lower()]
            elif nome_original.upper() in consultas:
                consulta = consultas[nome_original.upper()]
            else:
                raise ValueError(f"Consulta '{nome_original}' não reconhecida.")

            logger.debug(f"Conexão usada: {consulta.conexao} | Tipo: {consulta.tipo}")

            df = CriadorDataFrame(
                funcao_conexao, consulta.conexao, consulta.sql, consulta.tipo
            ).executar()

            fim = time.perf_counter()
            tempo = fim - inicio
            linhas, colunas = df.shape
            memoria_mb = df.memory_usage(deep=True).sum() / 1024**2

            logger.info(f"✅ Consulta '{nome_original}' finalizada em {tempo:.2f} segundos.")
            logger.info(f"📊 Linhas: {linhas} | Colunas: {colunas} | Memória: {memoria_mb:.2f} MB")

            # Substituído print() por logger.info()
            logger.info(f"Resultado da consulta '{nome_original}':\n{df.head()}")

            resultados[nome_original] = df

        except Exception as e:
            logger.error(f"❌ Erro na consulta '{nome_original}': {str(e)}")
            
            resultados[nome_original] = pd.DataFrame()

    return resultados


def salvar_no_financa(df: pd.DataFrame, table_name: str):
    """
    Salva um DataFrame no SQL Server 'SPSVSQL39', banco 'FINANCA'.

    Args:
        df (pd.DataFrame): O DataFrame a ser salvo.
        table_name (str): O nome da tabela no banco de dados.
    """
    if df.empty:
        logger.warning(f"⚠️ DataFrame está vazio. Nada será salvo na tabela '{table_name}'.")
        return

    try:
        logger.info(f"📀 Iniciando salvamento na tabela '{table_name}'...")
        inicio = time.perf_counter()

        # A função funcao_conexao já é local a este módulo
        engine = funcao_conexao("SPSVSQL39_FINANCA") # Usando o nome completo para clareza
        df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)

        fim = time.perf_counter()
        tempo = fim - inicio

        logger.info(f"✅ Salvamento concluído na tabela '{table_name}' em {tempo:.2f} segundos.")
    except Exception as e:
        logger.error(f"❌ Erro ao salvar no SQL: {str(e)}")
        

