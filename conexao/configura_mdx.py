import os
import sys

# Caminho onde está a DLL válida
dll_path = r"C:\Microsoft.AnalysisServices.AdomdClient.dll"
dll_dir = os.path.dirname(dll_path)

# Adiciona o diretório da DLL ao PATH
os.environ["PATH"] = dll_dir + os.pathsep + os.environ.get("PATH", "")
if dll_dir not in sys.path:
    sys.path.append(dll_dir)

# (opcional) Confirma que a DLL pode ser carregada diretamente
import clr
clr.AddReference(dll_path)

# Agora é seguro importar o Pyadomd
from pyadomd import Pyadomd

# Pronto para usar!
