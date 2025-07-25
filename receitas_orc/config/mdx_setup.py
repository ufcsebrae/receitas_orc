import os
import sys

def setup_mdx_environment(dll_path=r"C:\Microsoft.AnalysisServices.AdomdClient.dll"):
    """
    Configura o ambiente para uso do Pyadomd com a DLL do Analysis Services.
    """
    dll_dir = os.path.dirname(dll_path)
    os.environ["PATH"] = dll_dir + os.pathsep + os.environ.get("PATH", "")
    if dll_dir not in sys.path:
        sys.path.append(dll_dir)

    import clr
    clr.AddReference(dll_path)
    # Importação do Pyadomd pode ser feita após a configuração
    from pyadomd import Pyadomd
    return Pyadomd
