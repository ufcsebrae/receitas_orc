def carregar_sql(caminho: str) -> str:
    with open(caminho, encoding="utf-8") as f:
        return f.read()