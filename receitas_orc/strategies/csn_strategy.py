import pandas as pd
from typing import List

# Supondo que a classe base esteja definida
from .base_strategy import BaseApropriacaoStrategy

class CSNStrategy(BaseApropriacaoStrategy):
    """
    Estratégia que apropria a despesa realizada de forma proporcional
    às receitas orçadas para o projeto.
    """
    # CORREÇÃO 1: Assinatura do método corrigida para ser compatível com o pipeline
    def apropriar(self, df_projeto: pd.DataFrame) -> pd.DataFrame:
        
        # O DataFrame df_projeto já é um subconjunto apenas para um projeto específico.
        # Todos os cálculos devem ser aplicados a ele.
        
        # CORREÇÃO 2: Variáveis usadas sem asteriscos
        df_projeto['CSN_APROPRIAR_MENSAL'] = df_projeto['TOTAL_DESPESA_EXECUTADO_MES_PROJETO'] * df_projeto['Coeficiente_DespesaReceita']
        df_projeto['CSN_APROPRIAR_ANUAL'] = df_projeto['TOTAL_DESPESA_EXECUTADO_ANO_PROJETO'] * df_projeto['Coeficiente_DespesaReceita']
        
        # CORREÇÃO 3: Removida a lógica de filtro.
        # A estratégia deve retornar o DataFrame completo com as novas colunas.
        # O pipeline se encarrega de juntar os resultados.
        
        # Para depuração, você pode imprimir o cabeçalho aqui
        print("DataFrame DENTRO da CSNStrategy (após cálculos):")
        print(df_projeto.head())
        df_projeto.to_excel('resultado_final_v_csn.xlsx', sheet_name='Resultado', index=False)

        
        return df_projeto
