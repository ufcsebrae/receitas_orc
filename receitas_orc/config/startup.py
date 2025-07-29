# receitas_orc/config/startup.py

from receitas_orc.config.mdx_setup import setup_mdx_environment

dll_path = r"C:\Microsoft.AnalysisServices.AdomdClient.dll"

AdomdConnection = None
Pyadomd = None


def inicializar_adomd():
    """Inicializa as conexões Adomd"""
    global AdomdConnection, Pyadomd
    try:
        AdomdConnection, Pyadomd = setup_mdx_environment(dll_path)
    except Exception as e:
        print(f"❌ Falha ao carregar ambiente MDX: {e}")
        AdomdConnection = None
        Pyadomd = None


# Inicializa automaticamente ao importar (pode ser removido se quiser 100% controle via main)
inicializar_adomd()
