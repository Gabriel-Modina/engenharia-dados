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
    'Nome do Item' : 'Nome do Produto',
    'Valor em Reais (R$)' : 'Preço do Produto (R$)',
    'Classificação do Produto' : 'Categoria do Produto',
    'Quantidade em Estoque' : 'Quantidade em Estoque',
    'Nome da Loja' : 'Filial',
    'Data da Venda' : 'Data da Venda'
}

dados_empresa_b.rename_columns(key_mapping)

dados_fusao = Dados.join(dados_empresa_a, dados_empresa_b)
print('Fusao: ', dados_fusao.nome_colunas)
print('Fusao: ', dados_fusao.qtd_linhas)

#Load

dados_fusao.salvando_dados(path_dados_combinados)
print('Fusao: ', path_dados_combinados)

''' Sem POO
import csv
import json

path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'
path_dados_combinados = 'data_processed/dados_combinados.csv'



#Abre o arquivo json e faz a leitura
def leitura_json(path_json):
    dados_json = []
    with open(path_json, 'r') as file:
        dados_json = json.load(file)
    return dados_json

#Abre o arquivo csv e faz a leitura
def leitura_csv(path_csv):
    dados_csv = []
    with open(path_csv, 'r') as file:
        spam_reader = csv.DictReader(file, delimiter=',')
        for row in spam_reader:
            dados_csv.append(row)
    return dados_csv

#Faz o mapeamento para leitura de arquivo de acordo com o tipo
def leitura_dados(path, tipo_arquivo):
    dados = []
    if (tipo_arquivo == 'csv'):
        dados = leitura_csv(path)
    elif(tipo_arquivo == 'json'):
        dados = leitura_json(path)
    return dados

#Retorna em lista as chaves da primeira linha do dict
def get_columns(dados):
    return list(dados[0].keys())

#Renomeia o nome das chaves do dict de acordo com o mapa
def rename_columns(dados, key_mapping):
    new_dados_csv = []
    for old_dict in dados:
        dict_temp = {}
        for old_key, value in old_dict.items():
            dict_temp[key_mapping[old_key]] = value
        new_dados_csv.append(dict_temp)
    return new_dados_csv

#Tamanho dos dados
def size_data(dados):
    return len(dados)

#Junta duas listas
def join(dados_a, dados_b):
    combined_list = []
    combined_list.extend(dados_a)
    combined_list.extend(dados_b)
    return combined_list

#transforma de dict para formato de tabela
def transformando_dados_tabela(dados, nome_colunas):
    dados_combinados_tabela = [nome_colunas]
    for row in dados:
        linha = []
        for coluna in nome_colunas:
            linha.append(row.get(coluna, 'Indisponível'))
        dados_combinados_tabela.append(linha)
    return dados_combinados_tabela

#exporta dados para csv
def salvando_dados(dados, path):
    with open(path, 'w') as file:
        writer = csv.writer(file)
        writer.writerows(dados)
#Iniciando leitura
print('Lendo arquivos...\n')
dados_json = leitura_dados(path_json, 'json')
nome_colunas_json = get_columns(dados_json)
tamanho_dados_json = size_data(dados_json)
print('Arquivo '+ path_json +' lido com sucesso.')
print('Total de linhas: '+ str(tamanho_dados_json))

dados_csv = leitura_dados(path_csv, 'csv')
nome_colunas_csv = get_columns(dados_csv)
tamanho_dados_csv = size_data(dados_csv)
print('Arquivo '+ path_csv +' lido com sucesso.')
print('Total de linhas: '+ str(tamanho_dados_csv))

#Iniciando processamento
print('Preparando para fusão...\n')
key_mapping = {
    'Nome do Item' : 'Nome do Produto',
    'Valor em Reais (R$)' : 'Preço do Produto (R$)',
    'Classificação do Produto' : 'Categoria do Produto',
    'Quantidade em Estoque' : 'Quantidade em Estoque',
    'Nome da Loja' : 'Filial',
    'Data da Venda' : 'Data da Venda'
}

print('Mapa para renomear\n'+ str(key_mapping))
print('Primeiro dado antes da transformação: '+ str(dados_csv[1].items()))
dados_csv = rename_columns(dados_csv, key_mapping)
nome_colunas_csv = get_columns(dados_csv)
print('Primeiro dado após transformação: '+ str(dados_csv[1].items()))

print('Realizando fusão...\n')
dados_fusao = join(dados_csv, dados_json)
nome_colunas_fusao = get_columns(dados_fusao)
tamanho_dados_fusao = size_data(dados_fusao)
print('Tamanho após fusão: ', tamanho_dados_fusao)

print('Preparando para salvar dados...\n')

dados_fusao_tabela = transformando_dados_tabela(dados_fusao, nome_colunas_fusao)
print('Transformação em formato de tabela com sucesso!\n')
print('Salvando...\n')
salvando_dados(dados_fusao_tabela, path_dados_combinados)

print('Salvo com sucesso!')
print('Encerrado')
'''