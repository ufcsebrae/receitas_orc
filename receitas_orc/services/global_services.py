import logging
import time
import pandas as pd
import traceback
from urllib.parse import quote_plus
from datetime import datetime
from conexao.conexoes import CONEXOES
from receitas_orc.data_access.queries import Consulta, consultas
from receitas_orc.data_access.query_executor import CriadorDataFrame
import sqlalchemy
import os

# Configuração do logger
log_folder = "logs"
data_str = datetime.now().strftime("%Y-%m-%d")
log_file = f"{log_folder}/execucao_{data_str}.log"

# Cria pasta de logs se não existir
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

# Configura logger principal
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler()],
)
logger = logging.getLogger("logger_financa")


def funcao_conexao(nome_conexao: str):
    """
    Retorna uma conexão SQLAlchemy com base nas informações da conexão especificada.
    Suporta conexões do tipo: 'sql', 'azure_sql' e 'olap'.
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


def selecionar_consulta_por_nome(titulo):
    """
    Executa uma ou mais consultas pelo nome.
    Aceita:
        - String com nomes separados por vírgula
        - Lista de strings
    Sempre retorna:
        - Dict[str, DataFrame] com os resultados
    Também imprime o head() de cada DataFrame retornado.
    """
    # Normaliza para lista de nomes
    if isinstance(titulo, str):
        nomes = [t.strip().upper() for t in titulo.split(",")]
    elif isinstance(titulo, list):
        nomes = [t.strip().upper() for t in titulo]
    else:
        raise ValueError(
            "O parâmetro 'titulo' deve ser uma string ou uma lista de strings."
        )

    resultados = {}

    for nome in nomes:
        inicio = time.perf_counter()
        logger.info(f"⛔️ Iniciando execução da consulta: '{nome}'")

        try:
            if nome in consultas:
                consulta = consultas[nome]
            elif nome.lower() in consultas:
                consulta = consultas[nome.lower()]
            else:
                raise ValueError(f"Consulta '{nome}' não reconhecida.")

            logger.info(
                f"[DEBUG] Conexão usada: {consulta.conexao} | Tipo: {consulta.tipo}"
            )

            df = CriadorDataFrame(
                funcao_conexao, consulta.conexao, consulta.sql, consulta.tipo
            ).executar()

            fim = time.perf_counter()
            tempo = fim - inicio
            linhas, colunas = df.shape
            memoria_mb = df.memory_usage(deep=True).sum() / 1024**2

            logger.info(f"✅ Consulta '{nome}' finalizada em {tempo:.2f} segundos.")
            logger.info(
                f"📊 Linhas: {linhas} | Colunas: {colunas} | Memória: {memoria_mb:.2f} MB"
            )

            print(f"\n📄 Resultado da consulta '{nome}':")
            print(df.head())

            resultados[nome] = df

        except Exception as e:
            logger.error(f"❌ Erro na consulta '{nome}': {str(e)}")
            logger.error(traceback.format_exc())
            resultados[nome] = pd.DataFrame()

    return resultados


def salvar_no_financa(df: pd.DataFrame, table_name: str):
    """
    Salva DataFrame no SQL Server. Loga tempo, tamanho e falhas.
    """
    from receitas_orc.services.global_services import funcao_conexao

    if df.empty:
        logger.warning(
            f"⚠️ DataFrame está vazio. Nada será salvo na tabela '{table_name}'."
        )
        return

    try:
        logger.info(f"📀 Iniciando salvamento na tabela '{table_name}'...")
        inicio = time.perf_counter()

        engine = funcao_conexao("SPSVSQL39")
        df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)

        fim = time.perf_counter()
        tempo = fim - inicio

        logger.info(
            f"✅ Salvamento concluído na tabela '{table_name}' em {tempo:.2f} segundos."
        )
    except Exception as e:
        logger.error(f"❌ Erro ao salvar no SQL: {str(e)}")
        logger.error(traceback.format_exc())
