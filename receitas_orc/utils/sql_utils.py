"""
sql_utils.py

Este módulo contém funções utilitárias para manipulação de arquivos SQL,
principalmente para carregar o conteúdo de arquivos .sql em strings.
"""

def carregar_sql(caminho: str) -> str:
    """
    Carrega o conteúdo de um arquivo SQL do caminho especificado.

    Args:
        caminho (str): O caminho completo para o arquivo SQL.

    Returns:
        str: O conteúdo do arquivo SQL como uma string.
    """
    with open(caminho, encoding="utf-8") as f:
        return f.read()

