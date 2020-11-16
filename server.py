#!/usr/bin/env python
"""Metadata upload process

Handle form upload, delete existing data and load the contents of the file.
"""

import os
import sys
from subprocess import call
import web
import psycopg2


web.config.debug = False
URLS = (
    '/(.*)', 'Index'
)
RENDER = web.template.render('./')
DB = web.database(dbn='postgres',
                  host=os.getenv('DB_HOST'),
                  user=os.getenv('DB_USER'),
                  pw=os.getenv('DB_PASS'),
                  db=os.getenv('DB_DB'))
API = os.getenv('API_URL')
try:
    DB._connect(DB.keywords)
except psycopg2.DatabaseError as error:
    print(error, file=sys.stderr)
    sys.exit(1)


class Index:
    """"Handle requests to index"""

    def GET(self, name):
        """Render index page"""
        return RENDER.index()

    def POST(self, name):
        """Execute a book loader based on input parameters"""
        success = error = ''  # later passed to the template

        data = web.input()
        mode = data.mode
        # write csv to filesystem
        csv_content = data.filename.decode('utf-8')
        csv_path = '/tmp/csv.csv'
        csv = open(csv_path, "w")
        csv.write(csv_content)
        csv.close()
        # delete existing data
        if not clear_data(mode):
            error = 'There was a problem deleting previous data'
        # load data from csv
        if load_data(csv_path, mode) and error == '':
            success = 'Import completed successfully'
        else:
            error = 'There was a problem loading the file'
        print(success)
        print(error)
        return RENDER.index(success, error)


def load_data(file_path, mode):
    """Load CSV file"""
    try:
        cmd = './loader.py --mode {0} --file {1} --client-url {2}'.format(
            mode, file_path, API)
        retcode = call(cmd, shell=True)
        if retcode != 0:
            print("Child was terminated by signal", -retcode, file=sys.stderr)
            return False
        return True
    except OSError as error:
        print("Execution failed:", error, file=sys.stderr)


def clear_data(mode):
    """Delete any existing data"""
    try:
        DB.delete('funding', where='1=1')
        DB.delete('funder', where='1=1')
        DB.delete('subject', where='1=1')
        DB.delete('price', where='1=1')
        DB.delete('publication', where='1=1')
        DB.delete('contribution', where='1=1')
        DB.delete('contributor', where='1=1')
        DB.delete('issue', where='1=1')
        DB.delete('series', where='1=1')
        DB.delete('language', where='1=1')
        DB.delete('work', where='1=1')
        DB.delete('imprint', where='1=1')
        DB.delete('publisher', where='1=1')
        return True
    except psycopg2.DatabaseError as error:
        print(error, file=sys.stderr)
        return False


if __name__ == '__main__':
    APP = web.application(URLS, globals())
    APP.run()
