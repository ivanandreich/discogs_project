import json
import pathlib
from pathlib import Path
import os
import psycopg2

# release_id = 1
dir_path = pathlib.Path.cwd()
dir_path = Path(str(dir_path).replace('json_deserializer', 'connector'))
files = os.listdir(path="C:\\Users\\63516\\PycharmProjects\\connector\\venv\\Scripts\\files")
num = len(files)  # кол-во json-файлов в директории
# открыть и закрыть коннекшн 1 раз
for release_id in range(1, num + 1):
    path = Path(dir_path, 'venv', 'Scripts', 'files', str(release_id) + '.json')
    with open(path, 'r') as read_file:
        json_dict = json.load(read_file)
    if 'id' in json_dict:
        con = psycopg2.connect(
            database="postgres",
            user="postgres",
            password="Armadilloso1o",
            host="127.0.0.1",
            port="5432"
        )

        cur = con.cursor()

        d = dict(id=str(json_dict['id']),
                 title=str(json_dict['title']).replace("'", "''"),
                 country=str(json_dict['country']).replace("'", "''"),
                 genres=str(json_dict['genres'])[2:len(str(json_dict['genres'])) - 2].replace("'", "''"),
                 year=str(json_dict['year'])
                 )

        cur.execute(
            "INSERT INTO releases (id,title,country,genres,year) "
            "VALUES (" + d['id'] + ", '" + d['title'] + "', '" + d['country'] + "', '" + d['genres'] + "', " + d[
                'year'] + ")"
        )
        con.commit()
        con.close()
        print("Record " + d['id'] + " inserted successfully")

    else:
        print('File ' + str(release_id) + ' has a problem')
        continue
# print(type(b))

# # con = psycopg2.connect(
#     database="postgres",
#     user="postgres",
#     password="Armadilloso1o",
#     host="127.0.0.1",
#     port="5432"
# )
# print("Database opened successfully")

# cur = con.cursor()

# создание таблицы(код ниже)
# cur.execute('''CREATE TABLE releases
#     (id integer NOT NULL PRIMARY KEY,
#     title text NOT NULL,
#     country text NOT NULL,
#     genres text NOT NULL,
#     year integer NOT NULL);''')
# print("Table created successfully")
# con.commit()
# con.close()
# print("Database opened successfully")

# d = dict(id=str(json_dict['id']),
#          title=str(json_dict['title']),
#          country=str(json_dict['country']),
#          genres=str(json_dict['genres'])[2:len(str(json_dict['genres'])) - 2],
#          year=str(json_dict['year'])
#          )
# # print("INSERT INTO releases (id,title,country,genres,year) "
# #       "VALUES (" + d['id'] + ", '" + d['title'] + "', '" + d['country'] +
# #       "', '" + d['genres'] + "', " + d['year'] + ")")
# cur.execute(
#   "INSERT INTO releases (id,title,country,genres,year) "
#   "VALUES (" + d['id'] + ", '" + d['title'] + "', '" + d['country'] + "', '" + d['genres'] + "', " + d['year'] + ")"
# )
# con.commit()
# con.close()
# print("Record inserted successfully")

# ______________________________________________________________________________________________________________________
# переименовать в loader и закинуть в github
# метод establish connection with database
# метод create table if not exist(?)
# метод для дропа всей бд (и может датасета)
# метод execute
# обработка исключений
# метод prepare sql query, строку кидать в execute
# открыть перед началом действий и закрыть коннекшн в конце
# инсерт батчами (добавление по 100 жсонов разом) - готовим массив данных и инсертаем его одним запросом (надо гуглить)
# агрегировать жсоны в одному файле, каждая строка - 1 жсон
# вместо вывода в консоль закидываем в логи (?)
# настроить pep8
# разобраться с api (?) чтобы было 60 файлов в минуту
# json в таблицу
