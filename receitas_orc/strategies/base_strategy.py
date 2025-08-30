from abc import ABC, abstractmethod
import pandas as pd

class BaseApropriacaoStrategy(ABC):
    """
    Classe base abstrata para todas as estratégias de apropriação.
    Define o contrato que todas as estratégias concretas devem seguir.
    """
    @abstractmethod
    def apropriar(self, df_projeto: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica a lógica de apropriação específica para um único projeto.

        Args:
            df_projeto (pd.DataFrame): DataFrame contendo todas as linhas (ações) de um único projeto.
            colunas_receita (list): Lista com os nomes das colunas de receita (ex: ['Recursos_Propios']).

        Returns:
            pd.DataFrame: O DataFrame do projeto com as novas colunas de apropriação calculadas.
        """
        pass
