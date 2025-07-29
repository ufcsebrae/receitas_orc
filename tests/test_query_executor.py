import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from receitas_orc.data_access.query_executor import CriadorDataFrame


def dummy_conexao(nome_conexao):
    # Retorna uma engine falsa ou string de conexão mockada
    return "Provider=MSOLAP;Data Source=servidor_falso;"


def test_sql_query_retorna_dataframe():
    # Simula pd.read_sql_query para retornar um DataFrame
    with patch("pandas.read_sql_query") as mock_read:
        df_esperado = pd.DataFrame({"col1": [1, 2], "col2": ["A", "B"]})
        mock_read.return_value = df_esperado

        criador = CriadorDataFrame(funcao_conexao=dummy_conexao, conexao="dummy", consulta="SELECT *", tipo="sql")
        resultado = criador.executar()

        assert isinstance(resultado, pd.DataFrame)
        assert not resultado.empty
        pd.testing.assert_frame_equal(resultado, df_esperado)




def test_mdx_query_retorna_dataframe():
    dados_falsos = [(1, "A"), (2, "B")]
    colunas_falsas = [MagicMock(), MagicMock()]
    colunas_falsas[0].name = "col1"
    colunas_falsas[1].name = "col2"

    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = dados_falsos
    mock_cursor.description = colunas_falsas

    mock_conexao = MagicMock()
    mock_conexao.__enter__.return_value.cursor.return_value.__enter__.return_value = mock_cursor

    with patch("receitas_orc.data_access.query_executor.Pyadomd", return_value=mock_conexao):
        criador = CriadorDataFrame(funcao_conexao=lambda _: "string_de_conexao_fake", conexao="dummy", consulta="MDX QUERY", tipo="mdx")
        resultado = criador.executar()

        assert isinstance(resultado, pd.DataFrame)
        assert list(resultado.columns) == ["col1", "col2"]
        assert len(resultado) == 2


def test_tipo_nao_suportado_gera_erro():
    criador = CriadorDataFrame(funcao_conexao=dummy_conexao, conexao="dummy", consulta="XXX", tipo="graphql")
    resultado = criador.executar()
    assert resultado.empty
