from receitas_orc.config.mdx_setup import setup_mdx_environment

def test_setup_mdx_environment_afeta_sys():
    from receitas_orc.config.mdx_setup import setup_mdx_environment
    dll_path = r"C:\Microsoft.AnalysisServices.AdomdClient.dll"
    AdomdConnection, Pyadomd = setup_mdx_environment(dll_path)
    assert AdomdConnection is not None
    assert Pyadomd is not None
