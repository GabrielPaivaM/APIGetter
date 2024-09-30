import os
import random
import sqlite3
import time
from colorama import Fore, init
import requests
import json

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

url = "https://isbn-search-br.search.windows.net/indexes/isbn-index/docs/search?api-version=2016-09-01"

headers = {
    "Content-Type": "application/json",
    "api-key": "100216A23C5AEE390338BBD19EA86D29"
}

start_page= input(f'{Fore.LIGHTCYAN_EX}Por qual pagina deseja começar? {Fore.LIGHTWHITE_EX}1 a 22823: ')
start_page = int(start_page)
def isbn13_to_isbn10(isbn13):
    # Remover o hífen, se houver
    isbn13 = isbn13.replace("-", "").replace(" ", "")
    # Verificar se o ISBN-13 é válido
    if len(isbn13) != 13 or not isbn13.isdigit() or (isbn13[:3] not in ["978", "979"]):
        print(isbn13)
        raise ValueError("ISBN-13 inválido")

    # Manter os últimos 10 dígitos
    isbn10_candidate = isbn13[3:]

    # Calcular o dígito de controle do ISBN-10
    total = sum(int(isbn10_candidate[i]) * (10 - i) for i in range(9))
    check_digit = total % 11

    # Formatar o dígito de controle
    if check_digit == 10:
        check_digit_str = 'X'
    else:
        check_digit_str = str(check_digit)

    # Retornar o ISBN-10
    return isbn10_candidate + check_digit_str


def isbn10_to_isbn13(isbn10):
    # Remover o hífen, se houver
    isbn10 = isbn10.replace("-", "").replace(" ", "")

    # Verificar se o ISBN-10 é válido
    if len(isbn10) != 10 or not isbn10[:-1].isdigit() or (isbn10[-1] not in "0123456789X"):
        print(isbn10)
        raise ValueError("ISBN-10 inválido")

    # Retirar o dígito de controle
    isbn9 = isbn10[:-1]

    # Adicionar prefixo 978
    isbn13 = "978" + isbn9

    # Calcular o dígito de controle do ISBN-13
    total = sum(int(isbn13[i]) * (1 if i % 2 == 0 else 3) for i in range(12))
    check_digit = (10 - (total % 10)) % 10

    # Retornar o ISBN-13
    return isbn13 + str(check_digit)


def fetch_paginated_data(url, headers, total_records, batch_size=100, start_page=start_page):
    all_results = []
    total_pages = total_records // batch_size

    for page in range (start_page - 1, total_pages + 1):
        time.sleep(random.uniform(0.5, 5))
        skip = page * batch_size

        data = {
            "searchMode": "any",
            "searchFields": "FormattedKey,RowKey,Authors,Title,Imprint",
            "queryType": "full",
            "count": True,  # Solicitar a contagem total
            "facets": ["Imprint,count:50", "Authors,count:50"],
            "filter": "",
            "orderby": None,
            "search": "*",
            "select": "Authors,Profissoes,Colection,Countries,Date,Imprint,Title,Subtitle,RowKey,PartitionKey,RecordId,FormattedKey,Subject,Veiculacao,Ano,IdiomasObra",
            "skip": skip,  # Valor de 'skip' para a paginação
            "top": batch_size  # Número máximo de resultados por página
        }

        # Requisição
        response = requests.post(url, headers=headers, data=json.dumps(data))

        if response.status_code == 200:
            response_json = response.json()
            results = response_json.get('value', [])

            print(
                f"{Fore.LIGHTWHITE_EX}Página {page + 1}/{total_pages + 1} {Fore.LIGHTGREEN_EX}request feito com sucesso.")

            all_results.extend(results)
            i = 1
            for result in results:
                cursor.execute('SELECT row_key FROM books WHERE row_key = ?', (result['RowKey'],))
                resultV = cursor.fetchone()

                if not resultV:

                    autho = result['Authors']
                    if autho:
                        authors = ""
                        for author in autho:
                            authors = authors + ", " + author
                        authors = authors[1:]

                    else:
                        authors = None

                    countr = result['Countries']
                    if countr:
                        countries = ""
                        for country in countr:
                            countries = countries + ", " + country
                        countries = countries[1:]

                    else:
                        countries = None

                    jo = result['Profissoes']
                    if jo:
                        jobs = ""
                        for job in jo:
                            jobs = jobs + ", " + job

                        jobs = jobs[1:]
                    else:
                        jobs = None

                    languag = result['IdiomasObra']
                    if languag:
                        languages = ""
                        for language in languag:
                            languages = languages + ", " + language

                        languages = languages[1:]
                    else:
                        languages = None

                    formatedKey = result['FormattedKey']

                    if len(result['RowKey']) == 10 and not 'null' in result['RowKey']:
                        formatedKey = isbn10_to_isbn13(result['RowKey'])
                    elif len(result['RowKey']) == 13 and not 'null' in result['RowKey']:
                        formatedKey = isbn13_to_isbn10(result['RowKey'])


                    cursor.execute('''
                    INSERT INTO books (
                        search_score, 
                        partition_key, 
                        row_key, 
                        authors, 
                        date, 
                        formatted_key, 
                        imprint, 
                        subject, 
                        title, 
                        collection, 
                        record_id, 
                        countries, 
                        veiculacao, 
                        subtitle, 
                        profissoes, 
                        idiomas_obra, 
                        ano
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        result['@search.score'],  # search_score
                        result['PartitionKey'],  # partition_key
                        result['RowKey'],  # row_key
                        authors,  # authors
                        result['Date'],  # date
                        formatedKey,  # formatted_key
                        result['Imprint'],  # imprint
                        result['Subject'],  # subject
                        result['Title'],  # title
                        result['Colection'],  # collection
                        result['RecordId'],  # record_id
                        countries,  # countries
                        result['Veiculacao'],  # veiculacao
                        result['Subtitle'],  # subtitle
                        jobs,  # profissoes
                        languages,  # idiomas_obra
                        result['Ano']  # ano
                    ))
                    print(
                        f"{Fore.LIGHTGREEN_EX}Livro {Fore.LIGHTWHITE_EX}{i} {Fore.LIGHTGREEN_EX}da pagina {Fore.LIGHTWHITE_EX}{page + 1}/{total_pages + 1} {Fore.LIGHTGREEN_EX}Salvo com sucesso")

                else:
                    print(f"{Fore.LIGHTCYAN_EX}Livro {Fore.LIGHTWHITE_EX}{i} {Fore.LIGHTCYAN_EX}da pagina {Fore.LIGHTWHITE_EX}{page + 1}/{total_pages + 1} {Fore.LIGHTYELLOW_EX}Já Salvo")

                i += 1
            database.commit()

        else:
            print(f"{Fore.LIGHTRED_EX}Erro na requisição da página {Fore.LIGHTWHITE_EX}{page + 1}: {response.status_code}")
            break  # Interrompe se houver erro

    return all_results

total_records = 2282269
all_data = fetch_paginated_data(url, headers, total_records)

print(f"{Fore.LIGHTYELLOW_EX}Total de registros coletados: {Fore.LIGHTWHITE_EX}{len(all_data)}")