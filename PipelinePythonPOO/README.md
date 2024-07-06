# Curso Pipeline de Dados Combinando Python e Orientação a Objetos

Neste curso, exploramos a combinação de dados de duas empresas, uma utilizando o formato JSON e a outra o formato CSV. Abordamos o processo ETL (Extract, Transform, Load) inicialmente utilizando um notebook, em seguida, criamos um script e, por fim, aplicamos os conceitos de Programação Orientada a Objetos (POO) no script, resultando na classe `Dados`.

## Conteúdo do Curso

### 1. Introdução ao ETL
- **Objetivo**: Entender o processo ETL e sua importância no pipeline de dados.
- **Ferramentas**: Jupyter Notebook.
- **Atividades**: Extração de dados de arquivos JSON e CSV, transformação dos dados e carregamento em um formato unificado.

### 2. Criação de um Script ETL
- **Objetivo**: Automatizar o processo ETL utilizando um script Python.
- **Ferramentas**: Python, Bibliotecas `json` e `csv`.
- **Atividades**: Desenvolver um script para realizar a leitura, transformação e combinação dos dados.

### 3. Aplicação de Programação Orientada a Objetos (POO)
- **Objetivo**: Refatorar o script ETL utilizando conceitos de POO.
- **Ferramentas**: Python, Conceitos de POO.
- **Atividades**: Criar a classe `Dados` para encapsular o processo ETL, incluindo métodos para leitura de arquivos, transformação e exportação dos dados combinados.

## Classe `Dados`

A classe `Dados` foi desenvolvida para simplificar e organizar o processo ETL, encapsulando as operações de leitura, transformação e exportação de dados. Aqui estão alguns detalhes sobre a classe:

### Métodos da Classe

- `__init__(self, path, tipo_dados)`: Inicializa a classe com o caminho do arquivo e o tipo de dados (JSON ou CSV).
- `leitura_json(self)`: Lê dados de um arquivo JSON.
- `leitura_csv(self)`: Lê dados de um arquivo CSV.
- `leitura_dados(self)`: Mapeia a leitura de dados de acordo com o tipo.
- `rename_columns(self, key_mapping)`: Renomeia as colunas dos dados de acordo com um mapa de chaves fornecido.
- `join(dados_a, dados_b)`: Junta duas instâncias da classe `Dados`.
- `salvando_dados(self, path)`: Exporta os dados combinados para um arquivo CSV.

### Propriedades

- `dados`: Retorna os dados lidos.
- `nome_colunas`: Retorna os nomes das colunas dos dados.
- `qtd_linhas`: Retorna a quantidade de linhas dos dados.

## Exemplo de Uso

Aqui está um exemplo de como utilizar a classe `Dados` em um script Python:

```python
from processamento_dados import Dados

path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'
path_dados_combinados = 'data_processed/dados_combinados.csv'

# Extract
dados_empresa_a = Dados(path_json, 'json')
print('Empresa A: ', dados_empresa_a.nome_colunas)
print('Empresa A: ', dados_empresa_a.qtd_linhas)

dados_empresa_b = Dados(path_csv, 'csv')
print('Empresa B: ', dados_empresa_b.nome_colunas)
print('Empresa B: ', dados_empresa_b.qtd_linhas)

# Transform
key_mapping = {
    'Nome do Item': 'Nome do Produto',
    'Valor em Reais (R$)': 'Preço do Produto (R$)',
    'Classificação do Produto': 'Categoria do Produto',
    'Quantidade em Estoque': 'Quantidade em Estoque',
    'Nome da Loja': 'Filial',
    'Data da Venda': 'Data da Venda'
}

dados_empresa_b.rename_columns(key_mapping)

dados_fusao = Dados.join(dados_empresa_a, dados_empresa_b)
print('Fusao: ', dados_fusao.nome_colunas)
print('Fusao: ', dados_fusao.qtd_linhas)

# Save
dados_fusao.salvando_dados(path_dados_combinados)
