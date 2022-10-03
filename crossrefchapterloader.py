"""Load chapter metadata from Crossref into Thoth"""

import re
import sys

import logging
import thothlibrary
from crossref import CrossrefClient


class CrossrefChapterLoader:
    """Generic logic to ingest chapter metadata from CSV into Thoth"""
    single_imprint = True
    publisher_name = None
    all_contributors = {}
    all_institutions = {}
    all_imprints = {}
    encoding = "utf-8"
    header = 0
    separation = ","
    orcid_regex = re.compile(
        r'0000-000(1-[5-9]|2-[0-9]|3-[0-4])\d{3}-\d{3}[\dX]')

    def __init__(self, metadata_file, client_url, email, password):
        self.thoth = thothlibrary.ThothClient(client_url)
        self.thoth.login(email, password)
        self.crossref = CrossrefClient()

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
        for c in self.thoth.contributors(limit=99999):
            self.all_contributors[c.fullName] = c.contributorId
            if c.orcid:
                self.all_contributors[c.orcid] = c.contributorId
        # create cache of all existing institutions
        for i in self.thoth.institutions(limit=99999):
            self.all_institutions[i.institutionName] = i.institutionId
            if i.ror:
                self.all_institutions[i.ror] = i.institutionId

    def get_crossref_metadata(self, doi):
        return self.crossref.get_doi(doi)

    def doi_in_thoth(self, doi):
        try:
            self.thoth.work_by_doi(doi=doi)
            return True
        except thothlibrary.errors.ThothError:
            return False

    def get_book_by_title(self, title):
        """Query Thoth to find a book given its title"""
        try:
            books = self.thoth.books(search=title.replace('"', '\\"'), publishers='"%s"' % self.publisher_id)
            return books[0]
        except (IndexError, AttributeError):
            logging.error('Book not found: \'%s\'' % title)
            sys.exit(1)

    def all_books(self):
        return self.thoth.books(limit=99999, publishers='"%s"' % self.publisher_id, work_status="ACTIVE")

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
    def simple_doi(doi):
        return doi.replace("https://doi.org/", "").replace("http://dx.doi.org/", "")

    @staticmethod
    def full_doi(doi):
        return "https://doi.org/%s" % CrossrefChapterLoader.simple_doi(doi)

    @staticmethod
    def roman_to_decimal(roman):
        def roman_value(val):
            r = val.upper()
            if r == 'I':
                return 1
            if r == 'V':
                return 5
            if r == 'X':
                return 10
            if r == 'L':
                return 50
            if r == 'C':
                return 100
            if r == 'D':
                return 500
            if r == 'M':
                return 1000
            return -1

        try:
            decimal = int(roman)
            return decimal
        except ValueError:
            pass
        res = 0
        i = 0
        while i < len(roman):
            s1 = roman_value(roman[i])
            if i + 1 < len(roman):
                s2 = roman_value(roman[i + 1])
                if s1 >= s2:
                    res = res + s1
                    i = i + 1
                else:
                    res = res + s2 - s1
                    i = i + 2
            else:
                res = res + s1
                i = i + 1
        return res
