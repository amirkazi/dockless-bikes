'''
24th August 2018

File contains function to append data from dataframe to a Postgres database
    table
'''

import pandas as pd 
from sqlalchemy import create_engine
import psycopg2


username = ''
password = ''
host = ''
port = ''
database = ''
dataframe = df
table_name = ''


def pushing_dataframes_to_postgres(username, password, host, port, database, dataframe, table_name):
    '''
    Given details about a database, and about a dataframe, adds that dataframe
        to the Postgres database table.

    Input:
        username: string
        password: string
        host: string
        port: string
        database: string
        dataframe: string
        table_name: string
    ''' 
    # string: dialect+driver://username:password@host:port/database
    
    string = 'postgresql+psycopg2://' + username + ':' + password + '@' + host + ':' + port + '/' + database
    engine = create_engine(string, echo = False)
    conn = engine.connect()
    
    dataframe.to_sql(name = table_name, con = engine, if_exists = 'append')





