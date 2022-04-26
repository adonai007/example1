from abc import abstractmethod
import sys
import os
import csv
import pyodbc 
import datetime
import time
import logging


# a program to execute sql scripts
class ExecuteScript:
    def __init__(self, file_name, db_name, db_user, db_password):
        self.file_name = file_name
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.logger = logging.getLogger('ExecuteScript')

    def execute_script(self):
        try:
            self.logger.info('Executing script: {}'.format(self.file_name))
            conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE={};UID={};PWD={}'.format(self.db_name, self.db_user, self.db_password))
            cursor = conn.cursor()
            with open(self.file_name, 'r') as f:
                for line in f:
                    cursor.execute(line)
                    conn.commit()
            self.logger.info('Script executed successfully')
        except Exception as e:
            self.logger.error('Error occured while executing script: {}'.format(e))
            sys.exit(1)
        finally:
            cursor.close()
            conn.close()
            
    def execute_script_with_param(self, param):
        try:
            self.logger.info('Executing script: {} with params: {}'.format(self.file_name, param))
            conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE={};UID={};PWD={}'.format(self.db_name, self.db_user, self.db_password))
            cursor = conn.cursor()
            with open(self.file_name, 'r') as f:
                for line in f:
                    cursor.execute(line, param)
                    conn.commit()
            self.logger.info('Script executed successfully')
        except Exception as e:
            self.logger.error('Error occured while executing script: {}'.format(e))
            sys.exit(1)
        finally:
            cursor.close()
            conn.close()

    def execute_script_with_param_and_return(self, param):
        try:
            self.logger.info('Executing script: {} with params: {}'.format(self.file_name, param))
            conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE={};UID={};PWD={}'.format(self.db_name, self.db_user, self.db_password))
            cursor = conn.cursor()
            with open(self.file_name, 'r') as f:
                for line in f:
                    cursor.execute(line, param)
                    conn.commit()
                    rows = cursor.fetchall()
            self.logger.info('Script executed successfully')
            return rows
        except Exception as e:
            self.logger.error('Error occured while executing script: {}'.format(e))
            sys.exit(1)
        finally:
            cursor.close()
            conn.close()




