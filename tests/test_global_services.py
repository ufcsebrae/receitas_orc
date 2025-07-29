import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from receitas_orc.services import global_services


# ----------------------------- Testes de selecionar_consulta_por_nome ----------------------------- #

@patch.dict("receitas_orc.services.global_services.CONEXOES", {
    "SPSVSQL39": {
        "tipo": "sql",
        "servidor": "localhost",
        "banco": "testdb",
        "driver": "{ODBC Driver 17 for SQL Server}",
        "trusted_connection": True
    }
})
@patch("receitas_orc.services.global_services.CriadorDataFrame")
def test_selecionar_consulta_por_nome_retorna_dataframe(mock_criador_df):
    mock_instance = MagicMock()
    mock_instance.executar.return_value = pd.DataFrame({"col": [1, 2]})
    mock_criador_df.return_value = mock_instance

    resultado = global_services.selecionar_consulta_por_nome("RECEITAS_ORCADAS_2025")
    assert "RECEITAS_ORCADAS_2025" in resultado
    assert not resultado["RECEITAS_ORCADAS_2025"].empty


def test_selecionar_consulta_por_nome_parametros_invalidos():
    with pytest.raises(ValueError):
        global_services.selecionar_consulta_por_nome(123)


def test_selecionar_consulta_por_nome_nao_encontrada():
    resultado = global_services.selecionar_consulta_por_nome("CONSULTA_INEXISTENTE")
    assert "CONSULTA_INEXISTENTE" in resultado
    assert resultado["CONSULTA_INEXISTENTE"].empty


# ----------------------------- Testes de salvar_no_financa ----------------------------- #

@patch("receitas_orc.services.global_services.funcao_conexao")
@patch("pandas.DataFrame.to_sql")
def test_salvar_no_financa_executa_to_sql(mock_to_sql, mock_funcao_conexao):
    mock_engine = MagicMock()
    mock_funcao_conexao.return_value = mock_engine
    df = pd.DataFrame({"col": [1, 2]})
    global_services.salvar_no_financa(df, "tabela_teste")
    mock_to_sql.assert_called_once()


@patch("receitas_orc.services.global_services.funcao_conexao")
@patch("pandas.DataFrame.to_sql")
def test_salvar_no_financa_com_df_vazio(mock_to_sql, mock_funcao_conexao):
    df_vazio = pd.DataFrame()
    global_services.salvar_no_financa(df_vazio, "tabela_teste")
    mock_to_sql.assert_not_called()


# ----------------------------- Testes de funcao_conexao ----------------------------- #

@patch("sqlalchemy.create_engine")
@patch.dict("receitas_orc.services.global_services.CONEXOES", {
    "SPSVSQL39": {
        "tipo": "sql",
        "servidor": "localhost",
        "banco": "testdb",
        "driver": "{ODBC Driver 17 for SQL Server}",
        "trusted_connection": True
    }
})
def test_funcao_conexao_sql(mock_create_engine):
    mock_conn = MagicMock()
    mock_create_engine.return_value.connect.return_value = mock_conn

    conn = global_services.funcao_conexao("SPSVSQL39")
    assert conn is mock_conn


@patch.dict("receitas_orc.services.global_services.CONEXOES", {
    "SPSVSQL_AZURE": {
        "tipo": "azure_sql",
        "servidor": "azure.test.server",
        "banco": "azuredb",
        "driver": "{ODBC Driver 17 for SQL Server}",
        "authentication": "ActiveDirectoryPassword",
        "usuario": "user",
        "senha": "pass"
    }
})
@patch("sqlalchemy.create_engine")
def test_funcao_conexao_azure_sql(mock_create_engine):
    mock_conn = MagicMock()
    mock_create_engine.return_value.connect.return_value = mock_conn
    conn = global_services.funcao_conexao("SPSVSQL_AZURE")
    assert conn is mock_conn


@patch.dict("receitas_orc.services.global_services.CONEXOES", {
    "CONEX_INVALIDA": {
        "tipo": "nao_suportado"
    }
})
def test_funcao_conexao_tipo_invalido():
    with pytest.raises(ValueError):
        global_services.funcao_conexao("CONEX_INVALIDA")
