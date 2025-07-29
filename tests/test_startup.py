# tests/test_startup.py
from unittest.mock import MagicMock, patch
import importlib


def test_startup_import_sucesso():
    with patch("receitas_orc.config.startup.setup_mdx_environment") as mock_setup:
        mock_conn = MagicMock(name="AdomdConnection")
        mock_pyadomd = MagicMock(name="Pyadomd")
        mock_setup.return_value = (mock_conn, mock_pyadomd)

        # Importa e reinicializa
        import receitas_orc.config.startup as startup_mod
        startup_mod.inicializar_adomd()

        assert startup_mod.AdomdConnection == mock_conn
        assert startup_mod.Pyadomd == mock_pyadomd


def test_startup_falha_carregamento():
    with patch("receitas_orc.config.startup.setup_mdx_environment", side_effect=Exception("Erro")):
        import receitas_orc.config.startup as startup_mod
        startup_mod.inicializar_adomd()

        assert startup_mod.AdomdConnection is None
        assert startup_mod.Pyadomd is None
