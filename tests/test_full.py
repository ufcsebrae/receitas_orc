from receitas_orc.main import carregar_dados, tratar_dados
from receitas_orc.services.global_services import salvar_no_financa


def test_fluxo_completo(monkeypatch):
    # Simula input do mês
    monkeypatch.setattr("builtins.input", lambda _: "1")

    # Etapas separadas
    resultados = carregar_dados()
    df = resultados["RECEITAS_ORCADAS_2025"]
    df_filtrado = tratar_dados(df)

    assert not df_filtrado.empty
    # salvar_no_financa(df_filtrado, "RECEITAS")  # opcional
