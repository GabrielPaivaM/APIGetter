import os
import sqlite3
from colorama import Fore, init
import csv

init(autoreset=True)

if os.path.exists('database.db'):
    try:
        # Tentando conectar ao banco de dados
        database = sqlite3.connect('database.db')
        cursor = database.cursor()
        print(f"{Fore.LIGHTGREEN_EX}Conexão bem-sucedida ao banco de dados.{Fore.LIGHTWHITE_EX}")

    except sqlite3.DatabaseError as e:
        print(f"{Fore.LIGHTRED_EX}Erro ao conectar ao banco de dados:{Fore.LIGHTWHITE_EX}", e)
        print(f"{Fore.LIGHTCYAN_EX}Criando um novo banco de dados...")
        os.remove('database.db')  # Remove o arquivo existente
        database = sqlite3.connect('database.db')
        cursor = database.cursor()
else:
    print(f"{Fore.LIGHTCYAN_EX}Criando um novo banco de dados...{Fore.LIGHTWHITE_EX}")
    database = sqlite3.connect('database.db')
    cursor = database.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS books (
    id INTEGER PRIMARY KEY AUTOINCREMENT,  -- ID auto-incremental
    search_score REAL,                      -- Para o campo "@search.score"
    partition_key TEXT,                     -- Para "PartitionKey"
    row_key TEXT UNIQUE,                    -- Para "RowKey"
    authors TEXT,                           -- Para "Authors", como texto
    date TEXT,                              -- Para "Date", armazenado como texto
    formatted_key TEXT,                     -- Para "FormattedKey"
    imprint TEXT,                           -- Para "Imprint"
    subject TEXT,                           -- Para "Subject"
    title TEXT,                             -- Para "Title"
    collection TEXT,                        -- Para "Collection" (corrigido de "Colection")
    record_id INTEGER,                      -- Para "RecordId", que pode ser NULL
    countries TEXT,                         -- Para "Countries", como texto
    veiculacao TEXT,                        -- Para "Veiculacao", que pode ser NULL
    subtitle TEXT,                          -- Para "Subtitle", que pode ser NULL
    profissoes TEXT,                        -- Para "Profissoes", como texto
    idiomas_obra TEXT,                      -- Para "IdiomasObra", como texto
    ano INTEGER                             -- Para "Ano", que pode ser NULL
)
''')

cursor.execute('SELECT * FROM books')
books = cursor.fetchall()

with open('books.csv', 'w', encoding='UTF-8') as w:
        c = 0

        for book in books:
            c += 1

            line = f'{book[0]}§{book[1]}§{book[2]}§{book[3]}§{book[4]}§{book[5]}§{book[6]}§{book[7]}§{book[8]}§{book[9]}§{book[10]}§{book[11]}§{book[12]}§{book[13]}§{book[14]}§{book[15]}§{book[16]}§{book[17]}\n'
            w.write(line)

            print(f'{Fore.LIGHTWHITE_EX}Linha {Fore.LIGHTCYAN_EX}{c} {Fore.LIGHTWHITE_EX}escrita')