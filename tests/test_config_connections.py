from receitas_orc.config.config_connections import CONEXOES

def test_conexoes_possui_chaves_esperadas():
    assert "default" in CONEXOES
    assert isinstance(CONEXOES["default"], dict)
