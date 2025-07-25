# receitas_orc

Projeto para gerenciamento de receitas e orçamentos.

## Estrutura

- **receitas_orc/**: Código-fonte principal do projeto.
  - `main.py`: Ponto de entrada da aplicação.
  - `models.py`: Definição dos modelos de dados.
  - `services.py`: Implementação da lógica de negócio.
  - `utils.py`: Funções auxiliares e utilitárias.
  - `config.py`: Configurações globais do projeto.
- **tests/**: Testes unitários para cada módulo.
- `requirements.txt`: Lista de dependências do projeto.
- `README.md`: Documentação e instruções de uso.
- `.gitignore`: Arquivos e pastas ignorados pelo controle de versão.

## Como usar

1. Instale as dependências:
pip install -r requirements.txt

2. Execute o projeto:
python -m receitas_orc.main

3. Execute os testes:
python -m unittest discover tests