import pandas as pd
from receitas_orc.services.dataframe_processing import filtrar_df, calcular_percentual_por_iniciativa


def test_filtrar_df_mes_valido():
    df = pd.DataFrame({
        '[PPA].[PPA com Fotografia].[Descrição de PPA com Fotografia].[MEMBER_CAPTION]': ["2025/Jan", "2025/Jan", "2025/Fev"],
        '[Measures].[ReceitaAjustado]': [100, 0, 200],
        '[Iniciativa].[Iniciativas].[Iniciativa].[MEMBER_CAPTION]': ["A", "A", "B"],
        '[Natureza Orçamentária].[Descrição de Natureza 4 nível].[Descrição de Natureza 4 nível].[MEMBER_CAPTION]': ["Categoria 1", "Categoria 2", "Categoria 3"]
    })

    resultado = filtrar_df(df.copy(), mes_input=1)
    assert not resultado.empty
    assert "PERCENTUAL" in resultado.columns
    assert resultado["VALOR"].iloc[0] == 100


def test_filtrar_df_mes_invalido():
    df = pd.DataFrame({
        '[PPA].[PPA com Fotografia].[Descrição de PPA com Fotografia].[MEMBER_CAPTION]': ["2025/Jan", "2025/Fev"],
        '[Measures].[ReceitaAjustado]': [100, 200],
        '[Iniciativa].[Iniciativas].[Iniciativa].[MEMBER_CAPTION]': ["A", "B"],
        '[Natureza Orçamentária].[Descrição de Natureza 4 nível].[Descrição de Natureza 4 nível].[MEMBER_CAPTION]': ["Cat1", "Cat2"]
    })

    resultado = filtrar_df(df.copy(), mes_input=99)
    assert resultado.empty


def test_filtrar_df_valor_zero():
    df = pd.DataFrame({
        '[PPA].[PPA com Fotografia].[Descrição de PPA com Fotografia].[MEMBER_CAPTION]': ["2025/Jan", "2025/Jan"],
        '[Measures].[ReceitaAjustado]': [0, 0],
        '[Iniciativa].[Iniciativas].[Iniciativa].[MEMBER_CAPTION]': ["A", "B"],
        '[Natureza Orçamentária].[Descrição de Natureza 4 nível].[Descrição de Natureza 4 nível].[MEMBER_CAPTION]': ["Cat1", "Cat2"]
    })

    resultado = filtrar_df(df.copy(), mes_input=1)
    assert resultado.empty


def test_calcular_percentual_por_iniciativa():
    df = pd.DataFrame({
        "INICIATIVA": ["A", "A", "B"],
        "DESCNVL4": ["Cat1", "Cat2", "Cat3"],
        "VALOR": [100, 100, 200]
    })

    resultado = calcular_percentual_por_iniciativa(df.copy())
    assert round(resultado.query("INICIATIVA == 'A'")["PERCENTUAL"].sum()) == 100
    assert round(resultado.query("INICIATIVA == 'B'")["PERCENTUAL"].sum()) == 100
