"""
mdx_setup.py

Este módulo é responsável por configurar o ambiente necessário para a
interação com o Microsoft Analysis Services (MDX) usando o Pyadomd.
Isso inclui a adição do diretório da DLL ao PATH do sistema e o
carregamento das referências .NET.
"""

import os
import clr

def setup_mdx_environment(dll_path):
    """
    Configura o ambiente para uso do Pyadomd com a DLL do Analysis Services.
    Adiciona o diretório da DLL ao PATH do sistema e carrega as referências CLR.

    Args:
        dll_path (str): O caminho completo para o arquivo da DLL
                        Microsoft.AnalysisServices.AdomdClient.dll.

    Returns:
        tuple: Uma tupla contendo as classes AdomdConnection e Pyadomd
               após a configuração bem-sucedida.

    Raises:
        FileNotFoundError: Se a DLL especificada em dll_path não for encontrada.
        Exception: Qualquer outro erro que ocorra durante a configuração do ambiente CLR.
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

