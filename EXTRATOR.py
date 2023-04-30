#Vesão demo do script de ETL que utilizei, modifiquei algumas variáveis e parametros para melhor entendimento.
#Versão 1.0
#Sugestões de melhoria:
#1- Uso de funcões nativas do SQLITE3 para inserção dos dados em massa, como sqlite3.cursor.executemany
#2- Redução do número de IF e ELIF na execução das operações do MENU
#3- Criação de uma interface gráfica mais intuitiva.
#4- Diminuição da redundancia da conexão ao SQLITE3 (sqlite3.connect)
#Autoria do Script - https://github.com/jorgecarvalhopi

import pyodbc
import time
import sqlite3
import datetime as dt

#Listando tabelas do Data WareHouse ================================================================================================================
conexao = sqlite3.connect(r'caminho do arquivo .db')
cursor_sqlite3 = conexao.cursor()
cursor_sqlite3.execute("SELECT name FROM sqlite_master WHERE type='table';")
nomes_tabelas = cursor_sqlite3.fetchall()
print('====== ETL - PYTHON ======\n')
print("TEMOS AS SEGUINTES TABELAS DISPONÍVEIS\n")
for tab in nomes_tabelas:
    print(f'{tab[0]}')
print("\n")

#Definindo Funções =================================================================================================================================
#Esse é um exemplo de função que utilizei para atualizar uma tabela do DW
def EXEMPLO_FUNCAO(resultados):
    result_len = len(resultados)
    #Nessa tentativa de conexão, caso o arquivo não exista, o Python irá criá-lo.
    conexao = sqlite3.connect(r'caminho do arquivo .db')
    cursor_sqlite3 = conexao.cursor()
    for row in resultados:
        Coluna1 = int(row[0])
        Coluna2 = row[1]
        Coluna3 = row[2]
        Coluna3 = row[3]
        #Caso a Query retorne um valor Nulo ou algum erro, essa é uma forma de tratar ele e não travar a execução.
        try:
            Coluna4 = float(row[12])
        except:
            Coluna4 = 0
        cursor_sqlite3.execute("INSERT INTO NOME_TABELA" + \
                "(Coluna1, Coluna2, Coluna3, Coluna4)" + \
                "VALUES (?, ?, ?, ?)",
                (Coluna1, Coluna2, Coluna3, Coluna4))
    conexao.commit()
    conexao.close()
    print(f'TABELA NOME_TABELA ATUALIZADA! {result_len} LINHAS INSERIDAS...')

#Essa é a função utilizada para se conectar ao Banco de Produção, extrair seus dados, e gerar uma lista com os resultados
def SQL_SERVER_QUERY(query):
    conexao = pyodbc.connect(
        'Driver={SQL Server};'
        'Server=COLOQUE SEU ID;'
        'Database=COLOQUE SEU BANCO;'
        'UID=Login;'
        'PWD=Senha;'
    )
    cursor_sqlserver = conexao.cursor()
    start = time.time()
    cursor_sqlserver.execute(query)
    end = time.time()

    results = cursor_sqlserver.fetchall()
    print(f'A consulta no Banco de Dados levou {round(end-start,2)} segundos para ser executada!')

    cursor_sqlserver.close()
    conexao.close()  
    return results

#Menu de Opções =================================================================================================================================
print('======OPÇÕES DISPONÍVEIS======\n')
print('''
->> Atualizações\n
1 - Opção
2 - Opção
3 - Opção 
4 - Opção
8 - Opção\n
->> Modificações\n
5 - Opção
6 - Opção
7 - Opção
      ''')
resposta = input('Qual Query executar?\n')
resposta = int(resposta)

#Definindo Respostas =================================================================================================================================
if resposta == 1:
    data_1 = input('DIGITE A DATA INCIAL: ')
    data_2 = input('DIGITE A DATA FINAL: ')
    query ='''
            WITH TABELA2 AS
            (
            SELECT DISTINCT(TABELA4.NomeColuna) from TABELA4
            )
            
            SELECT * FROM TABELA1
            INNER JOIN TABELA2 ON TABELA1.NomeColuna = TABELA_2.NomeColuna
            LEFT JOIN TABELA3 ON TABAELA1.NomeColuna = TABELA3.NomeColuna
            WHERE NOME_TABELA.NomeColuna LIKE '%TEXTO%'
            AND TABELA1.Data BETWEEN ''' + data_1 + ''' AND ''' + data_2 + '''
            '''
    #Ao chamar a função SQL_SERVER_QUERY ela irá retornar um objeto lista, com cada linha retornada pela Query sendo um elemento.
    resultados = SQL_SERVER_QUERY(query=query)
    #Com essa lista em mãos, passamos para a função que irá inserir no DW
    EXEMPLO_FUNCAO(resultados=resultados)

elif resposta == 2:
    "CONSTRUA SUA OPERAÇÃO PERSONALIZADA :)"
