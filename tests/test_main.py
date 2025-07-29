from unittest.mock import patch
from receitas_orc.main import executar_pipeline


@patch("receitas_orc.main.tratar_dados")
@patch("receitas_orc.main.carregar_dados")
@patch("builtins.input", lambda _: "1")
def test_executar_pipeline_com_mock(mock_carregar, mock_tratar):
    # Mocka retorno do carregar_dados
    mock_carregar.return_value = {
        "RECEITAS_ORCADAS_2025": "fake_df"
    }

    # Mocka retorno do tratamento
    mock_tratar.return_value = "df_processado"

    executar_pipeline()

    mock_carregar.assert_called_once()
    mock_tratar.assert_called_once_with("fake_df")
