"""Load a CSV file containing chapter abstracts into Thoth"""

import sys

import numpy as np
import pandas as pd
import logging

import thothlibrary
from thothlibrary import ThothClient

from crossrefchapterloader import CrossrefChapterLoader


class Deduper:  # pylint: disable=too-few-public-methods
    """Dummy class to rename duplicate columns in a CSV file"""
    headers = dict()

    def __call__(self, header):
        """Append an increasing counter to columns that repeat its header"""
        if header not in self.headers:
            self.headers[header] = 0
            return header
        self.headers[header] += 1
        return "%s %d" % (header, self.headers[header])


class ObpChapterAbstractLoader:
    """Logic to ingest OBP chapter abstracts from CSV into Thoth"""
    publisher_name = "Open Book Publishers"
    encoding = "utf-8"
    header = 0
    separation = ","

    def __init__(self, metadata_file, client_url, email, password):
        self.metadata_file = metadata_file
        self.thoth = ThothClient(client_url)
        self.thoth.login(email, password)

        self.data = self.prepare_file()
        publishers = self.thoth.publishers(search=self.publisher_name)
        try:
            self.publisher_id = publishers[0].publisherId
        except (IndexError, AttributeError):
            logging.error('Publisher not found: %s' % self.publisher_name)
            sys.exit(1)
        try:
            self.imprint_id = publishers[0].imprints[0].imprintId
        except (IndexError, AttributeError):
            logging.error('No imprints associated with publisher: %s' % self.publisher_name)
            sys.exit(1)

    def run(self):
        for row in self.data.index:
            doi = self.data.at[row, "DOI"]
            abstract = self.data.at[row, "Content"]
            if not doi or not abstract:
                continue
            simple_doi = CrossrefChapterLoader.simple_doi(doi).strip()
            full_doi = CrossrefChapterLoader.full_doi(simple_doi)

            try:
                work = self.thoth.work_by_doi(doi=full_doi)
            except thothlibrary.errors.ThothError:
                logging.warning('DOI not in Thoth: %s' % full_doi)
                continue
            if work['workType'] != "BOOK_CHAPTER":
                logging.warning('Not a chapter: %s' % simple_doi)
                continue
            abstract = abstract.strip()
            # Some abstracts contain multiple lines by mistake
            line_count = len(abstract.split("\n"))
            if line_count > 6:
                abstract = abstract.replace("\n", " ").replace("  ", " ")
            if work['longAbstract']:
                logging.info('Abstract already in Thoth: %s' % simple_doi)
                continue
            work['longAbstract'] = abstract
            self.thoth.update_work(work)

    def prepare_file(self):
        """Read CSV, convert empties to None and rename duplicate columns"""
        frame = pd.read_csv(self.metadata_file, encoding=self.encoding,
                            header=self.header, sep=self.separation)
        frame = frame.where(pd.notnull(frame), None)
        frame = frame.replace({np.nan: None})
        frame = frame.rename(columns=Deduper())
        return frame