from modules.functions import Functions
import datetime

if __name__ == '__main__':
    while True:
        print("[1] - Inserir dados do CSV no MySQL", "[2] - Inserir dados do CSV no Cassandra", "[3] - Migrar Dados do SQL para Cassandra", "[0] - Sair", sep = "\n")
        menu = input("opção: ")

        if menu == "1":
            i = datetime.datetime.now()
            Functions.importar_csv_sql()
            f = datetime.datetime.now()
            r = f-i
            print(r)

        
        elif menu == "2":
            i = datetime.datetime.now()
            Functions.importar_csv_cassandra()
            f = datetime.datetime.now()
            r = f-i
            print(r)
        
        elif menu == "3":
            i = datetime.datetime.now()
            Functions.sql_2_cassandra()
            f = datetime.datetime.now()
            r = f-i
            print(r)
        
        elif menu == "0":
            break

        else:
            print("Opção Inválida!")