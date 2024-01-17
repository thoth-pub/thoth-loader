"""Load a CSV file into Thoth"""

import re
import sys

import numpy as np
import pandas as pd
import logging
from thothlibrary import ThothClient


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


class ChapterLoader:
    """Generic logic to ingest chapter metadata from CSV into Thoth"""
    single_imprint = True
    publisher_name = None
    all_contributors = {}
    all_institutions = {}
    all_imprints = {}
    encoding = "utf-8"
    header = 0
    separation = ","
    main_contributions = ["AUTHOR", "EDITOR", "TRANSLATOR"]
    contributors_limit = 99999
    institutions_limit = 99999
    orcid_regex = re.compile(
        r'0000-000(1-[5-9]|2-[0-9]|3-[0-4])\d{3}-\d{3}[\dX]')

    def __init__(self, metadata_file, client_url, email, password):
        # get metadata file from arguments
        self.metadata_file = metadata_file
        self.thoth = ThothClient(client_url)
        # login to (local) thoth using provided credentials
        self.thoth.login(email, password)
        # prepare_file does """Read CSV, convert empties to None and rename duplicate columns"""
        # so we shouldn't need this for the Editus chapter loader, because it's JSON
        self.data = self.prepare_file()
        logging.info(self.publisher_name)
        # can't see what's inside here because it's a Thoth operation, i.e. I'd need to go into Thoth.
        # but it also doesn't matter because in obpchapterloader, they just say that publisher_name is
        # "Open Book Publishers"
        publishers = self.thoth.publishers(search=self.publisher_name)
        try:
            self.publisher_id = publishers[0].publisherId
        except (IndexError, AttributeError):
            logging.error('Publisher not found: %s' % self.publisher_name)
            sys.exit(1)
        try:
            for imprint in publishers[0].imprints:
                self.all_imprints[imprint.imprintName] = imprint.imprintId
            if self.single_imprint:
                self.imprint_id = publishers[0].imprints[0].imprintId
        except (IndexError, AttributeError):
            logging.error('No imprints associated with publisher: %s' % self.publisher_name)
            sys.exit(1)

        # create cache of all existing contributors
        for c in self.thoth.contributors(limit=self.contributors_limit):
            self.all_contributors[c.fullName] = c.contributorId
            if c.orcid:
                self.all_contributors[c.orcid] = c.contributorId
        # create cache of all existing institutions
        for i in self.thoth.institutions(limit=self.institutions_limit):
            self.all_institutions[i.institutionName] = i.institutionId
            if i.ror:
                self.all_institutions[i.ror] = i.institutionId

    def get_book_by_title(self, title):
        """Query Thoth to find a book given its title"""
        try:
            books = self.thoth.books(search=title.replace('"', '\\"'), publishers='"%s"' % self.publisher_id)
            return books[0]
        except (IndexError, AttributeError):
            logging.error('Book not found: \'%s\'' % title)
            sys.exit(1)

    def get_work_by_doi(self, doi):
        """Query Thoth to find a work given its DOI"""
        try:
            return self.thoth.work_by_doi(doi)
        except (IndexError, AttributeError):
            logging.error('Work not found: \'%s\'' % doi)
            sys.exit(1)

    def prepare_file(self):
        """Read CSV, convert empties to None and rename duplicate columns"""
        frame = pd.read_csv(self.metadata_file, encoding=self.encoding,
                            header=self.header, sep=self.separation)
        frame = frame.where(pd.notnull(frame), None)
        frame = frame.replace({np.nan: None})
        frame = frame.rename(columns=Deduper())
        return frame

    def is_main_contribution(self, contribution_type):
        """Return a boolean string ready for ingestion"""
        return "true" \
            if contribution_type in self.main_contributions \
            else "false"

    def create_chapter_relation(self, book_work_id, chapter_work_id, relation_ordinal):
        """Create a work relation of type HAS_CHILD"""
        work_relation = {
            "relatorWorkId": book_work_id,
            "relatedWorkId": chapter_work_id,
            "relationType": "HAS_CHILD",
            "relationOrdinal": relation_ordinal
        }
        return self.thoth.create_work_relation(work_relation)

    @staticmethod
    def sanitise_title(title, subtitle):
        """Return a dictionary that includes the full title"""
        character = " " if title.endswith("?") else ": "
        full_title = character.join([title, subtitle]) \
            if subtitle else title
        return {"title": title, "subtitle": subtitle, "fullTitle": full_title}

    @staticmethod
    def sanitise_date(date):
        """Return a date ready to be ingested"""
        if not date:
            return None
        date = str(int(date))
        if len(date) == len("20200101"):
            return "{}-{}-{}".format(date[:4], date[4:6], date[6:8])
        return date.replace("/", "-").strip()
