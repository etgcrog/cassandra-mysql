from modules.connector_MySQL import Interface_sql
from modules.connector_cassandra import Interface_cassandra
import pandas as pd
import numpy as np

class Functions():

    def importar_csv_sql():
        """Método apra importar um CSV e fazer a inserção em uma Database Mysql.
        """
        oldtech = Interface_sql("root", "root", "172.17.0.4", "oldtech") # Estanciando a conexão com o Mysql
        df_dados = pd.read_csv('Sistema_A_SQL.csv', sep = ',') # Importando o CSV para um DataFrame Pandas
        df_dados = df_dados.dropna() # Método do pandas para limpar linhas com informações nulas
        dados = np.array(df_dados) # convertendo o DataFrame para Array usando Numpy
        lista =[]                                
        for dado in dados: # Laço para organizar os dados em tuplas antes da inserção           
            values = (dado[0], dado[1], dado[2])
            lista.append(values)
        values = str(lista)[1:-1] # Convertendo a lista de tuplas em string e retirando os colchetes para passar na query
        query = f"insert into vendas (nota_fiscal, vendedor, total) values {values}" # Montagem da query de inserção 
        oldtech.action(query) # execução da query dentro do conector
    
    def buscar_sql():
        """Realiza uma busca no Database SQL e converte em dataFrame

        Returns:
            [dataframe]: [Dataframe com os dados da tabela buscada]
        """
        oldtech = Interface_sql("root", "root", "172.17.0.4", "oldtech")
        query = 'select * from vendas'
        dados = oldtech.action(query)
        dados_df = pd.DataFrame(dados, columns=['nota_fiscal', 'vendedor', 'total'])
        dados_df = np.array(dados_df.drop('nota_fiscal', axis = 1))        
        return dados_df

    def sql_2_cassandra():
        """Permite migrar os dados do sql para o cassandra dentro dos parâmetro definidos no método
        """
        con = Interface_cassandra('oldtech')
        values = Functions.buscar_sql()
        for venda in values:            
            valores = f"(uuid(), '{venda[0]}', {float(venda[1])})"
            con.inserir(valores)
    
    def importar_csv_cassandra():
        """Permite inserir um bloco de dados em CSV no keyspace do cassandra
        """
        con = Interface_cassandra('oldtech')
        df_dados = pd.read_csv('Sistema_B_NoSQL.csv', sep = ',')
        df_dados = df_dados.dropna() 
        dados = np.array(df_dados)                                
        for dado in dados:                   
            valores = f"(uuid(), '{dado[1]}', {float(dado[2])})"
            con.inserir(valores)
            

            #O módulo do cassandra para python não aceita execução do comando COPY.
    # def salvar_csv_import():
    #     # oldtech = Interface_sql("user", "user", "127.0.0.1", "oldtech")
    #     con = Interface_cassandra('oldtech')
    #     # query = 'select * from vendas'
    #     # dados = oldtech.action(query)
    #     # dados_df = pd.DataFrame(dados, columns=['nota_fiscal', 'vendedor', 'total'])
    #     # dados_df.drop('nota_fiscal', axis=1, inplace = True)
    #     # dados_df.to_csv('import_SQL_2_Cass.csv', index = False)
    #     query = "COPY oldtech.vendas (id, vendedor, total) FROM 'C:\\Users\\dinho\\Meu Drive\\SoulCode\\Aulas\\VS Code\\import_SQL_2_Cass.csv' WITH DELIMITER=',' AND HEADER=TRUE;"
    #     con.inserir_csv(query)

        
        