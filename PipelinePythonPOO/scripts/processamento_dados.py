import json
import csv

class Dados:

    def __init__(self, path, tipo_dados):
        self.__path = path
        self.__tipo_dados = tipo_dados
        self.__dados = self.__leitura_dados()
        self.__nome_colunas = self.__get_columns()
        self.__qtd_linhas = self.__size_data()

    # Abre o arquivo json e faz a leitura
    def __leitura_json(self):
        dados_json = []
        with open(self.__path, 'r') as file:
            dados_json = json.load(file)
        return dados_json

    # Abre o arquivo csv e faz a leitura
    def __leitura_csv(self):
        dados_csv = []
        with open(self.__path, 'r') as file:
            spam_reader = csv.DictReader(file, delimiter=',')
            for row in spam_reader:
                dados_csv.append(row)
        return dados_csv

    # Faz o mapeamento para leitura de arquivo de acordo com o tipo
    def __leitura_dados(self):
        dados = []
        if self.__tipo_dados == 'csv':
            dados = self.__leitura_csv()
        elif self.__tipo_dados == 'json':
            dados = self.__leitura_json()
        elif self.__tipo_dados == 'list':
            dados = self.__path
            self.__path = 'lista em memoria'
        return dados
    
    # Retorna em lista as chaves da primeira linha do dict
    def __get_columns(self):
        return list(self.__dados[-1].keys())
    
    # Renomeia o nome das chaves do dict de acordo com o mapa
    def rename_columns(self, key_mapping):
        new_dados = []
        for old_dict in self.__dados:
            dict_temp = {}
            for old_key, value in old_dict.items():
                dict_temp[key_mapping[old_key]] = value
            new_dados.append(dict_temp)

        self.__dados = new_dados
        self.__nome_colunas = self.__get_columns()

    # Tamanho dos dados
    def __size_data(self):
        return len(self.__dados)

    # Junta duas listas
    @staticmethod
    def join(dados_a, dados_b):
        combined_list = []
        combined_list.extend(dados_a.dados)
        combined_list.extend(dados_b.dados)
        return Dados(combined_list, 'list')

    # Transforma de dict para formato de tabela
    def __transformando_dados_tabela(self):
        dados_combinados_tabela = [self.__nome_colunas]
        for row in self.__dados:
            linha = []
            for coluna in self.__nome_colunas:
                linha.append(row.get(coluna, 'Indisponível'))
            dados_combinados_tabela.append(linha)
        return dados_combinados_tabela

    # Exporta dados para csv
    def salvando_dados(self, path):
        dados_combinados_tabela = self.__transformando_dados_tabela()

        with open(path, 'w') as file:
            writer = csv.writer(file)
            writer.writerows(dados_combinados_tabela)

    # Métodos públicos para acessar atributos encapsulados
    @property
    def dados(self):
        return self.__dados

    @property
    def nome_colunas(self):
        return self.__nome_colunas

    @property
    def qtd_linhas(self):
        return self.__qtd_linhas
