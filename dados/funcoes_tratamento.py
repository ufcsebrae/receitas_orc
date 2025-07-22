def filtrar_df(df):
    """
    Renomeia as colunas do DataFrame e filtra os dados com base no mês informado pelo usuário.
    """
    import os
    os.system('cls' if os.name == 'nt' else 'clear')  # limpa o terminal (Windows/Linux)

    df.columns = ['FotografiaPPA', 'INICIATIVA', 'CDGNVL4', 'DESCNVL4', 'VALOR']

    meses = {
        0: 'Elb', 1: 'Jan', 2: 'Fev', 3: 'Mar', 4: 'Abr', 5: 'Mai', 6: 'Jun',
        7: 'Jul', 8: 'Ago', 9: 'Set', 10: 'Out', 11: 'Nov', 12: 'Dez'
    }

    print("\n" + "="*60)
    print("📆 SELECIONE O MÊS PARA FILTRAR OS DADOS")
    print("="*60)
    for k, v in meses.items():
        print(f"   [{k:>2}] ➤ {v}")
    print("="*60)

    # Entrada do usuário
    try:
        mes_input = int(input("\n🔸 Digite o número do mês desejado (0–12): "))
        if mes_input not in meses:
            raise ValueError
    except ValueError:
        print("\n❌ Mês inválido. Por favor, digite um número entre 0 e 12.")
        return df.head(0)

    mes_str = meses[mes_input]
    print(f"\n📊 Filtrando dados para o mês: **{mes_str}** (Receitas Orçadas SME)\n")

    filtro = df['FotografiaPPA'].str.contains(f"/{mes_str}", case=False, na=False)
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
    # Soma por iniciativa e categoria
    agrupado = df.groupby(['INICIATIVA', 'DESCNVL4'], as_index=False)['VALOR'].sum()

    # Total por iniciativa
    total_por_iniciativa = agrupado.groupby('INICIATIVA')['VALOR'].transform('sum')

    # Cálculo de percentual
    agrupado['PERCENTUAL'] = (agrupado['VALOR'] / total_por_iniciativa) * 100

    return agrupado
