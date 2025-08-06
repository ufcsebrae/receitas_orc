import os

# Use EXATAMENTE o mesmo caminho que está no seu main.py
DLL_PATH = r"C:\Microsoft.AnalysisServices.AdomdClient.dll"

print(f"Testando o caminho: {DLL_PATH}")

if os.path.exists(DLL_PATH):
    print("\n✅ SUCESSO: Python encontrou o arquivo.")
else:
    print("\n❌ FALHA: Python NÃO encontrou o arquivo neste caminho.")

if os.path.isfile(DLL_PATH):
    print("✅ CONFIRMADO: O caminho aponta para um arquivo.")
else:
    print("❌ AVISO: O caminho não aponta para um arquivo (pode ser uma pasta ou não existir).")

