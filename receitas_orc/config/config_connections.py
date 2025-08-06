"""
config_connections.py

Define as configurações de conexão para os diversos bancos de dados
e serviços utilizados pelo projeto 'receitas_orc'.
Cada entrada no dicionário CONEXOES representa uma configuração de
conexão específica, incluindo tipo (SQL, Azure SQL, MDX), servidor,
banco de dados, driver, e autenticação.
"""

CONEXOES = {
    "SPSVSQL39_FINANCA": {
        "tipo": "sql",
        "servidor": "spsvsql39",
        "banco": "FINANCA",
        "driver": "ODBC+Driver+17+for+SQL+Server",
        "trusted_connection": True
    },
    "SPSVSQL39_HubDados": {
        "tipo": "sql",
        "servidor": "spsvsql39",
        "banco": "HubDados",
        "driver": "ODBC+Driver+17+for+SQL+Server",
        "trusted_connection": True
    },
    "OLAP_SME": {
        "tipo": "mdx",
        "str_conexao": "Provider=MSOLAP;Data Source=NASRVUGESQLPW02;Catalog=SMEDW_V3_SSAS;",
    },
    "AZURE": {
        "tipo": "azure_sql",
        "servidor": "synapsesebraespprod-ondemand.sql.azuresynapse.net",
        "banco": "DatamartMeta",
        "driver": "ODBC+Driver+17+for+SQL+Server",
        "authentication": "ActiveDirectoryInteractive"
    },
    "default": {
        "tipo": "sql",
        "servidor": "localhost",
        "banco": "master",
        "driver": "ODBC Driver 17 for SQL Server",
        "trusted_connection": True
    }
}
