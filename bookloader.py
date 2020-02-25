"""Load a CSV file into Thoth"""

import re
import pandas as pd
import isbn_hyphenate
from thothlibrary import ThothClient


class Deduper():  # pylint: disable=too-few-public-methods
    """Dummy class to rename duplicate columns in a CSV file"""

    headers = dict()

    def __call__(self, header):
        """Append an increasing counter to columns that repeat its header"""
        if header not in self.headers:
            self.headers[header] = 0
            return header
        self.headers[header] += 1
        return "%s %d" % (header, self.headers[header])


class BookLoader():
    """Generic logic to ingest metadata from CSV into Thoth"""
    single_imprint = True
    publisher_name = None
    publisher_shortname = None
    publisher_url = None
    all_contributors = {}
    all_series = {}
    encoding = "utf-8"
    header = 0
    separation = ","
    work_types = {
        "Monograph": "MONOGRAPH",
        "Book": "MONOGRAPH",
        "Edited book": "EDITED_BOOK",
        "Journal Issue": "JOURNAL_ISSUE",
        "Journal": "JOURNAL_ISSUE"
    }
    work_statuses = {
        "Active": "ACTIVE",
        "Out of print": "OUT_OF_PRINT"
    }
    contribution_types = {
        "Author": "AUTHOR",
        "Editor": "EDITOR",
        "Translator": "TRANSLATOR",
        "Foreword": "FOREWORD_BY",
        "Introduction": "INTRODUCTION_BY",
        "Preface": "PREFACE_BY",
        "Music editor": "MUSIC_EDITOR"
    }
    main_contributions = ["AUTHOR", "EDITOR", "TRANSLATOR"]
    orcid_regex = re.compile(
        r'0000-000(1-[5-9]|2-[0-9]|3-[0-4])\d{3}-\d{3}[\dX]')

    def __init__(self, metadata_file, client_url):
        self.metadata_file = metadata_file
        self.thoth = ThothClient(client_url)
        self.data = self.prepare_file()
        self.publisher_id = self.create_publisher()
        self.imprint_id = self.create_imprint()

    def prepare_file(self):
        """Read CSV, convert empties to None and rename duplicate columns"""
        frame = pd.read_csv(self.metadata_file, encoding=self.encoding,
                            header=self.header, sep=self.separation)
        frame = frame.where(pd.notnull(frame), None)
        frame = frame.rename(columns=Deduper())
        return frame

    def create_publisher(self):
        """Create a publisher object in Thoth and return its ID"""
        publisher = {
            "publisherName": self.publisher_name,
            "publisherShortname": self.publisher_shortname,
            "publisherUrl": self.publisher_url
        }
        return self.thoth.create_publisher(publisher)

    def create_imprint(self):
        """Creates an imprint equal to the publisher's attributes"""
        imprint = {
            "publisherId": self.publisher_id,
            "imprintName": self.publisher_name,
            "imprintUrl": self.publisher_url
        }
        return self.thoth.create_imprint(imprint)

    @staticmethod
    def sanitise_title(title, subtitle):
        """Return a dictionary that includes the full title"""
        character = " " if title.endswith("?") else ": "
        full_title = character.join([title, subtitle]) \
            if subtitle else title
        return {"title": title, "subtitle": subtitle, "fullTitle": full_title}

    @staticmethod
    def in_to_mm(inches):
        """Return a rounded conversion to milimetres from inches"""
        try:
            return int(round(float(inches)*25.4))
        except TypeError:
            return None

    @staticmethod
    def sanitise_date(date):
        """Return a date ready to be ingested"""
        if not date:
            return None
        if len(date) == len("20200101"):
            return "{}-{}-{}".format(date[:4], date[4:6], date[6:8])
        return date.replace("/", "-").strip()

    @staticmethod
    def sanitise_isbn(isbn):
        """Return a hyphenated ISBN"""
        if not isbn:
            return None
        try:
            if "-" in str(isbn):
                return str(isbn)
            return isbn_hyphenate.hyphenate(str(int(isbn)))
        except isbn_hyphenate.IsbnMalformedError:
            print(isbn)
            raise
