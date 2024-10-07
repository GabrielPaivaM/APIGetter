import os
import random
import sqlite3
import time
from colorama import Fore, init
import requests
import json
from functions.convertFunctions import convertFunctions


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

def fetch_paginated_data(url, headers, total_records, batch_size=100, start_page=start_page):
    all_results = []
    total_pages = total_records // batch_size

    for page in range (start_page - 1, total_pages + 1):
        time.sleep(random.uniform(2, 7))
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

                cursor.execute('SELECT * FROM books WHERE row_key = ?', (result['RowKey'],))
                resultV = cursor.fetchone()

                if resultV:

                    autho = result['Authors']
                    if autho:
                        authors = ""
                        for author in autho:
                            authors = authors + ", " + author
                        authors = authors[2:]

                    else:
                        authors = None

                    countr = result['Countries']
                    if countr:
                        countries = ""
                        for country in countr:
                            countries = countries + ", " + country
                        countries = countries[2:]

                    else:
                        countries = None

                    jo = result['Profissoes']
                    if jo:
                        jobs = ""
                        for job in jo:
                            jobs = jobs + ", " + job

                        jobs = jobs[2:]
                    else:
                        jobs = None

                    languag = result['IdiomasObra']
                    if languag:
                        languages = ""
                        for language in languag:
                            languages = languages + ", " + language

                        languages = languages[2:]
                    else:
                        languages = None

                    formatedKey = result['FormattedKey']

                    if len(result['RowKey']) == 10 and not 'null' in result['RowKey']:
                        formatedKey = convertFunctions.isbn10_to_isbn13(result['RowKey'])
                    elif len(result['RowKey']) == 13 and not 'null' in result['RowKey']:
                        formatedKey = convertFunctions.isbn13_to_isbn10(result['RowKey'])

                    updates = list()
                    value1 = resultV[1]
                    value2 = resultV[2]
                    value3 = result['RowKey']
                    value4 = resultV[3]
                    value5 = resultV[4]
                    value6 = resultV[5]
                    value7 = resultV[6]
                    value8 = resultV[7]
                    value9 = resultV[8]
                    value10 = resultV[9]
                    value11 = resultV[10]
                    value12 = resultV[11]
                    value13 = resultV[12]
                    value14 = resultV[13]
                    value15 = resultV[14]
                    value16 = resultV[15]
                    value17 = resultV[16]

                    if not resultV[1] == result['@search.score']:
                        value1 = result['@search.score']
                        updates.append('search score')

                    if not resultV[2] == result['PartitionKey']:
                        value2 = result['PartitionKey']
                        updates.append('PartitionKey')

                    if not resultV[4] == authors:
                        value4 = authors
                        updates.append('authors')

                    if not resultV[5] == result['Date']:
                        value5 = result['Date']
                        updates.append('Date')

                    if not resultV[6] == formatedKey:
                        value6 = formatedKey
                        updates.append('formatedKey')

                    if not resultV[7] == result['Imprint']:
                        value7 = result['Imprint']
                        updates.append('Imprint')

                    if not resultV[8] == result['Subject']:
                        value8 = result['Subject']
                        updates.append('Subject')

                    if not resultV[9] == result['Title']:
                        value9 = result['Title']
                        updates.append('Title')

                    if not resultV[10] == result['Colection']:
                        value10 = result['Colection']
                        updates.append('Colection')

                    if not resultV[11] == result['RecordId']:
                        value11 = result['RecordId']
                        updates.append('RecordId')

                    if not resultV[12] == countries:
                        value12 = countries
                        updates.append('countries')

                    if not resultV[13] == result['Veiculacao']:
                        value13 = result['Veiculacao']
                        updates.append('Veiculacao')

                    if not resultV[14] == result['Subtitle']:
                        value14 = result['Subtitle']
                        updates.append('Subtitle')

                    if not resultV[15] == jobs:
                        value15 = jobs
                        updates.append('jobs')

                    if not resultV[16] == languages:
                        value16 = languages
                        updates.append('languages')

                    if not resultV[17] == result['Ano']:
                        value17 = result['Ano']
                        updates.append('Ano')

                    if updates:
                        cursor.execute('''
                        UPDATE books SET
                            search_score = ?, 
                            partition_key = ?, 
                            authors = ?, 
                            date = ?, 
                            formatted_key = ?, 
                            imprint = ?, 
                            subject = ?, 
                            title = ?, 
                            collection = ?, 
                            record_id = ?, 
                            countries = ?, 
                            veiculacao = ?, 
                            subtitle = ?, 
                            profissoes = ?, 
                            idiomas_obra = ?, 
                            ano = ? 
                            WHERE row_key = ?
                        ''', (
                            value1,  # search_score
                            value2,  # partition_key
                            value4,  # authors
                            value5,  # date
                            value6,  # formatted_key
                            value7,  # imprint
                            value8,  # subject
                            value9,  # title
                            value10,  # collection
                            value11,  # record_id
                            value12,  # countries
                            value13,  # veiculacao
                            value14,  # subtitle
                            value15,  # profissoes
                            value16,  # idiomas_obra
                            value17,  # ano
                            value3
                        ))
                        updatesText = ''

                        for update in updates:
                            updatesText += ", " + update

                        print(
                            f"{Fore.LIGHTGREEN_EX}Livro {Fore.LIGHTWHITE_EX}{i} {Fore.LIGHTGREEN_EX}da pagina {Fore.LIGHTWHITE_EX}{page + 1}/{total_pages + 1} {Fore.LIGHTYELLOW_EX}Atualizado com sucesso: {Fore.LIGHTCYAN_EX}{updatesText[2:]}")

                    else:
                        print(
                            f"{Fore.LIGHTGREEN_EX}Livro {Fore.LIGHTWHITE_EX}{i} {Fore.LIGHTGREEN_EX}da pagina {Fore.LIGHTWHITE_EX}{page + 1}/{total_pages + 1} {Fore.LIGHTMAGENTA_EX}Sem atualizações")
                else:

                    autho = result['Authors']
                    if autho:
                        authors = ""
                        for author in autho:
                            authors = authors + ", " + author
                        authors = authors[2:]

                    else:
                        authors = None

                    countr = result['Countries']
                    if countr:
                        countries = ""
                        for country in countr:
                            countries = countries + ", " + country
                        countries = countries[2:]

                    else:
                        countries = None

                    jo = result['Profissoes']
                    if jo:
                        jobs = ""
                        for job in jo:
                            jobs = jobs + ", " + job

                        jobs = jobs[2:]
                    else:
                        jobs = None

                    languag = result['IdiomasObra']
                    if languag:
                        languages = ""
                        for language in languag:
                            languages = languages + ", " + language

                        languages = languages[2:]
                    else:
                        languages = None

                    formatedKey = result['FormattedKey']

                    if len(result['RowKey']) == 10 and not 'null' in result['RowKey']:
                        formatedKey = convertFunctions.isbn10_to_isbn13(result['RowKey'])
                    elif len(result['RowKey']) == 13 and not 'null' in result['RowKey']:
                        formatedKey = convertFunctions.isbn13_to_isbn10(result['RowKey'])

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


                i += 1
            database.commit()

        else:
            print(f"{Fore.LIGHTRED_EX}Erro na requisição da página {Fore.LIGHTWHITE_EX}{page + 1}: {response.status_code}")
            break  # Interrompe se houver erro

    return all_results

total_records = 2282269
all_data = fetch_paginated_data(url, headers, total_records)

print(f"{Fore.LIGHTYELLOW_EX}Total de registros coletados: {Fore.LIGHTWHITE_EX}{len(all_data)}")