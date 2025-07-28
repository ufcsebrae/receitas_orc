import os
from config.mdx_setup import setup_mdx_environment

dll_path = r"C:\Microsoft.AnalysisServices.AdomdClient.dll"

try:
    AdomdConnection, Pyadomd = setup_mdx_environment(dll_path)
except Exception as e:
    print(f"❌ Falha ao carregar ambiente MDX: {e}")
    AdomdConnection = None
    Pyadomd = None