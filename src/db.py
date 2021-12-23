import sys
import logging
from logging.handlers import RotatingFileHandler

import duckdb
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 1000)


def execute_query(connection, query_string, print_result=False):
    query_result = connection.execute(query_string)
    if print_result:
        print(query_result.fetchdf())
    return query_result


def initialise(connection):
    with open('src/initialise.sql') as f:
        initial_sql = f.read()
    execute_query(connection, initial_sql)

    with open('src/analyse.sql') as f:
        initial_sql = f.read()
    execute_query(connection, initial_sql)


def main():
    logging.info('opening duckdb connection')
    cursor = duckdb.connect()

    logging.info('creating initial resources')
    initialise(connection=cursor)

    show_tables_sql = 'show tables;'
    execute_query(connection=cursor, query_string=show_tables_sql, print_result=True)

    logging.info('opening shell')
    while True:
        query_string = input('\n> ')
        logging.info('executing {}'.format(query_string))

        if query_string.strip(';') == 'exit':
            break
        try:
            execute_query(query_string=query_string, connection=cursor, print_result=True)
        except RuntimeError as e:
            print()
    logging.info('exiting')


if __name__ == '__main__':

    module_name = __file__.split('/')[-1]

    handlers = [RotatingFileHandler('logs/{}.log'.format(module_name), mode='a', maxBytes=1000000, backupCount=10)]

    logging.basicConfig(level=logging.INFO,
                        handlers=handlers,
                        datefmt='%Y-%m-%d %H:%M',
                        format='%(asctime)s %(name)-8s %(module)-12s %(levelname)-8s %(message)s')

    logging.info(' '.join(sys.argv))

    logging.getLogger("pika").setLevel(logging.WARNING)

    main()
