import subprocess
import sys
import os
from pathlib import Path
import pytest
from unittest.mock import patch
import pandas as pd

# Verifica se o módulo 'clr' está instalado
try:
    import clr  # noqa: F401
    HAS_CLR = True
except ImportError:
    HAS_CLR = False


@pytest.mark.skipif(not HAS_CLR, reason="Ignorado: 'clr' (pythonnet) não está disponível")
def test_main_execucao_completa():
    """
    Executa o script main.py como subprocesso com variável de ambiente simulando input.
    """
    projeto_root = Path(__file__).resolve().parents[1]
    main_path = projeto_root / "receitas_orc" / "main.py"

    env = os.environ.copy()
    env["PYTHONPATH"] = str(projeto_root)
    env["MES_INPUT"] = "1"  # Simula escolha do mês Janeiro
    env["PYTHONIOENCODING"] = "utf-8"  # Evita erros de codificação com emojis

    result = subprocess.run(
        [sys.executable, str(main_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )

    # Em caso de falha, exibe saída para diagnóstico
    if result.returncode != 0:
        print("\n--- STDOUT ---")
        print(result.stdout)
        print("\n--- STDERR ---")
        print(result.stderr)

    assert result.returncode == 0


def test_main_com_mocks(monkeypatch):
    """
    Testa a função main() diretamente com dependências mockadas.
    Não depende de 'clr' nem subprocess.
    """
    from receitas_orc.main import main

    # Simula input de ENTER e define MES_INPUT via ambiente
    monkeypatch.setattr("builtins.input", lambda _: "1")
    os.environ["MES_INPUT"] = "1"  # Garante uso durante teste

    with patch("receitas_orc.main.selecionar_consulta_por_nome") as mock_sel, \
         patch("receitas_orc.main.filtrar_df") as mock_filt:

        df_fake = pd.DataFrame({
            "FotografiaPPA": ["01/Jan", "15/Jan"],
            "VALOR": [100, 200],
            "INICIATIVA": ["X", "Y"],
            "DESCNVL4": ["desc1", "desc2"]
        })

        mock_sel.return_value = {"RECEITAS_ORCADAS_2025": df_fake}
        mock_filt.return_value = df_fake

        main()

        mock_sel.assert_called_once()
        mock_filt.assert_called_once()

    # Limpa variável de ambiente após o teste
    os.environ.pop("MES_INPUT", None)
