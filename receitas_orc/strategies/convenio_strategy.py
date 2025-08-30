import pandas as pd
from .base_strategy import BaseApropriacaoStrategy

class ConvenioStrategy(BaseApropriacaoStrategy):
    """
    Estratégia para Convênios.
    REGRA: Apropria 80% da despesa realizada, toda na fonte 'Receita_de_Convenios'.
    """
    def apropriar(self, df_projeto: pd.DataFrame) -> pd.DataFrame:
        pass
        return df_projeto
