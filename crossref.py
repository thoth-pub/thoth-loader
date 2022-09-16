"""Expose Crossref API"""
import sys
import requests
import logging


class CrossrefClient:
    """Crossref API client"""
    endpoint = "https://api.crossref.org"
    retry_count = 0

    def get_doi(self, doi):
        try:
            url = "%s/works/%s" % (self.endpoint, doi)
            res = requests.get(url)
            if res.status_code == 404:
                logging.info("%s not found in Crossref" % doi)
                return False
            self.retry_count = 0
            return res.json()['message']
        except requests.exceptions.RequestException as error:
            logging.error('Error (%s) querying %s' % (res.status_code, doi))
            if self.retry_count <= 5:
                self.retry_count += 1
                logging.error('Retrying (%s) querying %s' % (self.retry_count, doi))
                return self.get_doi(doi)
            sys.exit(1)
