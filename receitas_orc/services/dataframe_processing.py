import os


def filtrar_df(df, mes_input=None):
    """
    Filtra o DataFrame com base no mês informado.

    Args:
        df (pd.DataFrame): DataFrame original.
        mes_input (int, opcional): Número do mês de 0 a 12. Se não for fornecido, solicita via input.

    Returns:
        pd.DataFrame: DataFrame filtrado e com percentuais calculados.
    """
    os.system("cls" if os.name == "nt" else "clear")

    print("[INFO] Colunas do DataFrame original:", df.columns.tolist())

    df = df.rename(columns={
        '[PPA].[PPA com Fotografia].[Descrição de PPA com Fotografia].[MEMBER_CAPTION]': 'FotografiaPPA',
        '[Iniciativa].[Iniciativas].[Iniciativa].[MEMBER_CAPTION]': 'INICIATIVA',
        '[Natureza Orçamentária].[Código Estruturado 4 nível].[Código Estruturado 4 nível].[MEMBER_CAPTION]': 'CDGNVL4',
        '[Natureza Orçamentária].[Descrição de Natureza 4 nível].[Descrição de Natureza 4 nível].[MEMBER_CAPTION]': 'DESCNVL4',
        '[Measures].[ReceitaAjustado]': 'VALOR'
    })

    meses = {
        0: 'Elb', 1: 'Jan', 2: 'Fev', 3: 'Mar', 4: 'Abr', 5: 'Mai', 6: 'Jun',
        7: 'Jul', 8: 'Ago', 9: 'Set', 10: 'Out', 11: 'Nov', 12: 'Dez'
    }

    if mes_input is None:
        print("\n" + "=" * 60)
        print("[INFO] SELECIONE O MÊS PARA FILTRAR OS DADOS")
        print("=" * 60)
        for k, v in meses.items():
            print(f"   [{k:>2}] ➤ {v}")
        print("=" * 60)

        try:
            mes_input = int(input("\nDigite o número do mês desejado (0–12): "))
        except (ValueError, EOFError):
            print("[ERRO] Mês inválido ou entrada não fornecida.")
            return df.head(0)

    if mes_input not in meses:
        print("[ERRO] Mês inválido. Digite um número entre 0 e 12.")
        return df.head(0)

    mes_str = meses[mes_input]
    print(f"\n[INFO] Filtrando dados para o mês: {mes_str} (Receitas Orçadas SME)\n")

    df['FotografiaPPA'] = df['FotografiaPPA'].astype(str)

    filtro = df['FotografiaPPA'].str.contains(f"/{mes_str}", case=False, na=False) & (df['VALOR'] > 0)
    df_filtrado = df[filtro]

    df_resultado = calcular_percentual_por_iniciativa(df_filtrado)
    print(df_resultado)

    return df_resultado


def calcular_percentual_por_iniciativa(df):
    """
    Agrupa o DataFrame por INICIATIVA e DESCNVL4, somando os valores
    e calculando o percentual de cada linha com base no total da iniciativa.

    Args:
        df (pd.DataFrame): DataFrame já filtrado por mês.

    Returns:
        pd.DataFrame: Resultado com colunas ['INICIATIVA', 'DESCNVL4', 'VALOR', 'PERCENTUAL'].
    """
    agrupado = df.groupby(['INICIATIVA', 'DESCNVL4'], as_index=False)['VALOR'].sum()
    total_por_iniciativa = agrupado.groupby('INICIATIVA')['VALOR'].transform('sum')
    agrupado['PERCENTUAL'] = (agrupado['VALOR'] / total_por_iniciativa) * 100

    return agrupado
