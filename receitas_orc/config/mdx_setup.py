import os
import clr

def setup_mdx_environment(dll_path):
    """
    Configura o ambiente para uso do Pyadomd com a DLL do Analysis Services.
    """
    if not os.path.exists(dll_path):
        raise FileNotFoundError(f"❌ DLL não encontrada em: {dll_path}")

    dll_dir = os.path.dirname(dll_path)

    # Garante que o diretório da DLL está no PATH do sistema
    os.environ["PATH"] = dll_dir + os.pathsep + os.environ.get("PATH", "")
    if dll_dir not in os.sys.path:
        os.sys.path.append(dll_dir)

    # Adiciona referência à DLL e importa os componentes necessários
    clr.AddReference(dll_path)

    from Microsoft.AnalysisServices.AdomdClient import AdomdConnection
    from pyadomd import Pyadomd

    return AdomdConnection, Pyadomd
