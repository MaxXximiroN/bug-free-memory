import mysql.connector
import schedule
import time
import logging
import configparser

import datetime as dt
import pandas as pd

from mysql.connector import Error
print('Все модули успешно импортированы')

config = configparser.ConfigParser()                                        # создаём объект конфигуратора
config.read('/Users/maximiron/studying/sql_terminal_practicing/config.ini') # читаем файл с конфигурациями

log_file = config['logging']['log_file']         # - указываем файл для записи логов.
log_level = config['logging']['log_level']       # - указываем уровень логирования.

path_to_littlewins_file = '/Users/maximiron/studying/sql_terminal_practicing/little_daily_wins.txt'
logging.basicConfig(filename=log_file,
                    level=log_level,
                    format='%(asctime)s - %(message)s')
print('Логгирование настроено Ver.2.0')

try:
    host_config = config['database']['host']         # — это адрес сервера, на котором запущена база данных.
    user_config = config['database']['user']         # - имя пользователя для подключения к MySQL.
    password_config = config['database']['password'] # — пароль пользователя MySQL.
    database_config = config['database']['database'] # — имя базы данных, к которой ты подключаешься.
except KeyError as e:
    logging.error(f'Отсутсвует параметр корфигурации {e}')
    raise SistemExit(f'Отсутсвует параметр корфигурации {e}')



# обработка ошибок при подключении к базе c повторными попытками.
def get_db_connection():
    max_retries = 5
    retries = 0
    while retries < max_retries:
        try:
            db_connector = mysql.connector.connect(
                host=host_config,
                user=user_config,         
                password=password_config, 
                database=database_config       
            )
            if db_connector.is_connected():
                logging.info('Подключение прошло успешно')
                return db_connector
        except mysql.connector.Error as e:
            retries += 1
            wait_time = 2 ** 1 
            logging.error(f'Ошибка подключеня, попытка {retries} из {max_retries}. Повторное подключение через {wait_time}')
            time.sleep(wait_time)
    logging.error('Не удалось подкючится к серверу после 5 попыток =Х')
    return None    
            
        
# Читаем файл из 

def read_file(path_to_file: str) -> str:    
    try:
        with open(path_to_file, 'r') as file:                        # 'r' - файл открывается в режиме чтения (read)
            content = file.read().splitlines()                       # Читаем и сразу разделяем строки
            proper_line = ', '.join(i.strip() for i in content if i) # генератор строк без пробелов и пустых элементов.
            logging.info(f' файл {proper_line} успешно прочитан')
            return(content)
    except FileNotFoundError:
        logging.error(f' Файл {path_to_file} не найдет')
    except Exception as e:
        logging.error(f'Ошибка при чтении файла: {e}')




def process_and_adding_date(current_date, comment, cursore, db_connector) -> None:
    subjects = ['sql', 'py', 'bash', 'eng', 'otr', 'db']

    for subject in subjects:
        subject_file = [i.strip().replace(f'_{subject}', '') for i in comment if i.endswith(f'_{subject}')]

        if subject_file:
            query = "INSERT INTO daily_knowledge_reservation (date, subject, comment) VALUES (%s, %s, %s)" 
            cursore.execute(query, (current_date, subject, ', '.join(i for i in subject_file))) # Отпарвляем запрос в БД
            db_connector.commit() # подтверждаем изменения



            
def add_daily_knowlege() -> str:                     
    try:
        db_connector = mysql.connector.connect(
            host=host_config,
            user=user_config,         
            password=password_config, 
            database=database_config       
        )
        if db_connector.is_connected():
            logging.info('Подключение прошло успешно')
            cursore = db_connector.cursor()              # создаём ообъект упраления запросами cursor
            comment = read_file(path_to_littlewins_file) # Получаем данные из файла с выученной информацией
        
            if not comment:
                raise ValueError('Комментарий не может быть пустым!')
    
            process_and_adding_date(dt.date.today(), comment, cursore, db_connector) # сегодняшняя дата, данные из def read_file и запросник.
            logging.info('Kомментарий успешно добавлен')
    
    except mysql.connector.Error as e: 
        logging.error(f'Ошибка mySQL: {e}')
    
    except ValueError as ve:
        logging.error(f'Ошибка: {ve}')
    
    finally:
        cursore.close() # Закрываем запросник 
        db_connector.close() # Закрываем сессию
        logging.info('Соединение с сервером закрыто')

schedule.every().day.at("20:00").do(add_daily_knowlege) # запуск скрипта каждый день в 20:00

while True:
    schedule.run_pending() # Выполнение задач по расписанию
    time.sleep(60) # Задержка на 60 секушнд, запрос отправляется каждую минуту