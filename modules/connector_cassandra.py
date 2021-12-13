from cassandra.cluster import Cluster
import pandas as pd

cluster = Cluster()

class Interface_cassandra():
    """Conetor com os métodos padrões do CRUD
    """
    database = ""

    def __init__(self, database):
        self.database = database        
        

    def buscar(self, tabela, colunas = '*'):
        """Premite a busca no Keyspace do cassandra

        Args:
            tabela (string): Nome da tabela onde a busca será feita
            colunas (string): [opicional] Por padrão trará todas a colunas da tabela, mas pode ser alterado. Defaults to '*'.

        Returns:
            [Lista]: [Retorna uma lista de Tuplas]
        """
        try:
            session = cluster.connect(self.database)
            query = f"SELECT {colunas} FROM {tabela};"
            dados = session.execute(query)        
            return dados
        except Exception as e:
            print(str(e))

    def inserir(self, values, colunas = "(id, vendedor, total)", tabela = 'vendas'):
        """Permite inserir informações na tabela vendas* (*Por padrão)

        Args:
            values (tupla): [Dados a serem inseridos]
            colunas (str, optional): [Colunas onde os dados serão inseridos]. Defaults to "(id, vendedor, total)".
            tabela (str, optional): [Tabela onde os dados serão inseridos]. Defaults to 'vendas'.
        """
        try:
            session = cluster.connect(self.database)
            query = f"INSERT INTO {tabela} {colunas} VALUES {values};"
            # print(query)
            session.execute(query)
        except Exception as e:
            print(str(e))
        
    def inserir_csv(self, query):
        """Permite inserir em sequencia os dados do CSV, desde que dentro de um for na função principal

        Args:
            query (string): [query deverá ser montada na função antes de ser enviada para o método que executa ela.]
        """
        session = cluster.connect(self.database)
        query = query
        session.execute(query)