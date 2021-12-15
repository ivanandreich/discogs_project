import json
import os
import psycopg2
import configparser
import requests
import time
from pathlib import Path
from user_agent import generate_user_agent


def establish_connection(url, release_id, headers):
    try:
        res = requests.get(url + str(release_id), headers=headers)  # запрос с уникальным юзер-агентом
        res.raise_for_status()
    except requests.exceptions.HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
        return res
    except Exception as err:
        print(f'Other error occurred: {err}')
    else:
        return res


def prepare_directory_for_dataset(dir_path, file_name):
    try:
        path = Path(dir_path, file_name)
    except FileNotFoundError as file_err:
        print(f'File error occurred: {file_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')
    else:
        return path


def open_joined_file(path):
    try:
        joined_file = open(path, 'w')
    except FileNotFoundError as file_err:
        print(f'File error occurred: {file_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')
    else:
        return joined_file


def write_in_joined_file(joined_file, res):
    try:
        json.dump(res, joined_file)
    except AttributeError as attr_err:
        print(f'Attribute error occurred: {attr_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')


def accumulate_joined_data(joined_data, res):
    try:
        joined_data["json"].append(res.json())
    except AttributeError as attr_err:
        print(f'Attribute error occurred: {attr_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')


def make_headers(res):
    try:
        r = res.headers
    except AttributeError as attr_err:
        print(f'Attribute error occurred: {attr_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')
    else:
        return r


def read_config(config, config_name):
    try:
        config.read(config_name)
    except AttributeError as attr_err:
        print(f'Attribute error occurred: {attr_err}')
    except FileNotFoundError as file_err:
        print(f'File error occurred: {file_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')
    else:
        return config


def make_delay():
    time.sleep(3)


def start_execution(config):
    config_1 = configparser.ConfigParser()
    config_connector = read_config(config_1, config)
    ua = generate_user_agent()
    # headers = {'user-agent': 'ConnectorForBigDataScienceStudyingProject/0.1 +https://github.com/ivanandreich/connector'}
    headers = {'user-agent': ua}
    joined_data = {"json": []}
    i = 0
    try:
        pathj = prepare_directory_for_dataset(config_connector['path']['dataset'], '1.json')
        joined_file = open_joined_file(pathj)
        for release_id in range(int(config_connector['nums']['start_id']), int(config_connector['nums']['num_records']) + int(config_connector['nums']['start_id'])):
            res = establish_connection(config_connector['url']['source'], release_id, headers)
            r = make_headers(res)
            if (release_id - int(config_connector['nums']['start_id'])) % int(config_connector['nums']['num_in_file']) == 0 and release_id != int(config_connector['nums']['start_id']):
                write_in_joined_file(joined_file, joined_data)
                i = i + 1
                pathj = prepare_directory_for_dataset(config_connector['path']['dataset'], str(i + 1) + '.json')
                joined_file = open_joined_file(pathj)
                joined_data = {"json": []}
            accumulate_joined_data(joined_data, res)
            make_delay()
        write_in_joined_file(joined_file, joined_data)
    except ValueError as val_err:
        print(f'Value error occurred: {val_err}')
    except KeyError as key_err:
        print(f'Key error occurred: {key_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')


# ___________________________________________________________________


def drop_table_dataset():
    cur.execute('''DROP TABLE IF EXISTS dataset''')


def truncate_table_dataset():
    cur.execute('''TRUNCATE TABLE dataset''')


def create_table_dataset():
    cur.execute('''CREATE TABLE IF NOT EXISTS dataset
            (id integer NOT NULL PRIMARY KEY,
            data json NOT NULL);''')


def insert_query(joined_json, id_in_joined):
    data = joined_json['json'][id_in_joined]
    json_string = json.dumps(data)
    json_string = json_string.replace("'", "''")
    return ('''INSERT INTO dataset (id, data)
            VALUES (\n''' + str(joined_json['json'][id_in_joined]['id']) + ''', \'''' +
            json_string + '''\');''')


def establish_connection_with_db():
    db_config = configparser.ConfigParser()  # создаём объекта парсера
    db_config.read("db_config.ini")  # читаем конфиг для бд
    return psycopg2.connect(
        database=db_config["database"]["database"],
        user=db_config["database"]["user"],
        password=db_config["database"]["password"],
        host=db_config["database"]["host"],
        port=db_config["database"]["port"]
    )


def parse_joined_json(json_dict):
    id_in_joined = 0
    while id_in_joined < len(json_dict['json']):
        # while id_in_joined < 40:
        if 'id' in json_dict['json'][id_in_joined]:
            cur.execute(insert_query(json_dict, id_in_joined))
            id_in_joined = id_in_joined + 1
        else:
            id_in_joined = id_in_joined + 1


def number_of_files_in_directory(path):
    files = os.listdir(path)
    num = len(files)
    return num


def insert_all():
    config = configparser.ConfigParser()  # создаём объекта парсера
    config.read("config_connector.ini")  # читаем конфиг
    filename = 1
    while filename <= number_of_files_in_directory(config["path"]["dataset"]):
        path = config["path"]["dataset"] + str(filename) + '.json'
        with open(path, 'r') as read_file:
            json_dict = json.load(read_file)
        parse_joined_json(json_dict)
        filename = filename + 1


# def clear_directory():

# ___________________________________________________________________


if __name__ == '__main__':
    try:
        # config = sys.argv[1]
        config = 'config_connector.ini'
    except ValueError as val_err:
        print(f'Value error occurred: {val_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')
    else:
        start_execution(config)
        db_config = configparser.ConfigParser()  # создаём объекта парсера
        db_config.read("db_config.ini")  # читаем конфиг для бд

        con = establish_connection_with_db()
        cur = con.cursor()  # курсор

        create_table_dataset()
        insert_all()

        con.commit()
        con.close()
