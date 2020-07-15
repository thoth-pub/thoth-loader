#!/usr/bin/env python
"""Metadata upload process

Handle form upload, delete existing data and load the contents of the file.
"""

import sys
from subprocess import call
import web


web.config.debug = False
URLS = (
    '/(.*)', 'Index'
)
RENDER = web.template.render('./')
API = 'http://localhost:8080/graphql'


class Index:
    """"Handle requests to index"""

    def GET(self, name):
        """Render index page"""
        return RENDER.index()

    def POST(self, name):
        """Execute a book loader based on input parameters"""
        data = web.input()
        mode = data.mode
        csv_content = data.filename.decode('utf-8')
        csv_path = '/tmp/csv.csv'
        csv = open(csv_path, "w")
        csv.write(csv_content)
        csv.close()
        print(mode)
        if load_data(csv_path, mode):
            return RENDER.index(success='Import completed successfully')
        return RENDER.index(error='There was a problem loading the file')


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


if __name__ == '__main__':
    APP = web.application(URLS, globals())
    APP.run()
