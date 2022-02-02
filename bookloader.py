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
    all_institutions = {}
    all_series = {}
    encoding = "utf-8"
    header = 0
    separation = ","
    work_types = {
        "Monograph": "MONOGRAPH",
        "MONOGRAPH": "MONOGRAPH",
        "Book": "MONOGRAPH",
        "Edited book": "EDITED_BOOK",
        "Edited Book": "EDITED_BOOK",
        "EDITED_BOOK": "EDITED_BOOK",
        "Journal Issue": "JOURNAL_ISSUE",
        "Journal": "JOURNAL_ISSUE"
    }
    work_statuses = {
        "Active": "ACTIVE",
        "Cancelled": "CANCELLED",
        "Forthcoming": "FORTHCOMING",
        "Out of print": "OUT_OF_PRINT",
        "Withdrawn": "WITHDRAWN_FROM_SALE"
    }
    contribution_types = {
        "Author": "AUTHOR",
        "AUTHOR": "AUTHOR",
        "Editor": "EDITOR",
        "EDITOR": "EDITOR",
        "Translator": "TRANSLATOR",
        "Foreword": "FOREWORD_BY",
        "Introduction": "INTRODUCTION_BY",
        "Preface": "PREFACE_BY",
        "Music editor": "MUSIC_EDITOR"
    }
    main_contributions = ["AUTHOR", "EDITOR", "TRANSLATOR"]
    orcid_regex = re.compile(
        r'0000-000(1-[5-9]|2-[0-9]|3-[0-4])\d{3}-\d{3}[\dX]')
    int_regex = re.compile(r'\d+')
    audio_regex = re.compile(r'[0-9]{1,3} \(aud\)')
    video_regex = re.compile(r'[0-9]{1,3} \(vid\)')

    def __init__(self, metadata_file, client_url, email, password):
        self.metadata_file = metadata_file
        self.thoth = ThothClient(client_url, version="0.6.0")
        self.thoth.login(email, password)

        self.data = self.prepare_file()
        publishers = self.thoth.publishers(search=self.publisher_name)
        try:
            self.publisher_id = publishers[0].publisherId
        except (IndexError, AttributeError):
            self.publisher_id = self.create_publisher()
        try:
            self.imprint_id = publishers[0].imprints[0].imprintId
        except (IndexError, AttributeError):
            self.imprint_id = self.create_imprint()

        # create cache of all existing contributors
        for c in self.thoth.contributors():
            self.all_contributors[c.fullName] = c.contributorId
            if c.orcid:
                self.all_contributors[c.orcid] = c.contributorId
        # create cache of all existing institutions
        for i in self.thoth.institutions():
            self.all_institutions[i.institutionName] = i.institutionId
            if i.ror:
                self.all_institutions[i.ror] = i.institutionId

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

    def is_main_contribution(self, contribution_type):
        """Return a boolean string ready for ingestion"""
        return "true" \
            if contribution_type in self.main_contributions \
            else "false"

    @staticmethod
    def sanitise_title(title, subtitle):
        """Return a dictionary that includes the full title"""
        character = " " if title.endswith("?") else ": "
        full_title = character.join([title, subtitle]) \
            if subtitle else title
        return {"title": title, "subtitle": subtitle, "fullTitle": full_title}

    @staticmethod
    def split_title(full_title):
        """Return a dictionary that includes the title and the subtitle"""
        subtitle = None
        try:
            title, subtitle = re.split(':', full_title)
        except ValueError:
            title = full_title
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
        date = str(int(date))
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

    @staticmethod
    def sanitise_price(price):
        """Return a float ready for ingestion"""
        try:
            return float(price.replace("$", "").strip())
        except (TypeError, AttributeError):
            return None

    @staticmethod
    def sanitise_media(media_count):
        """Return both audio and video count as integers"""
        def find_integer():
            return int(BookLoader.int_regex.match(media_count).group(0))
        # case: single integer in cell means audio count
        audio_count = video_count = 0
        try:
            audio_count = int(media_count)
            return audio_count, video_count
        except (TypeError, ValueError):
            if media_count is None:
                return 0, 0
        # case: specific video count in cell, e.g. "2 (vid)"
        try:
            if BookLoader.video_regex.search(media_count).group(0):
                video_count = find_integer()
        except AttributeError:
            video_count = 0
        # case: specific audio count in cell, e.g. "1 (aud)"
        try:
            if BookLoader.audio_regex.search(media_count).group(0):
                audio_count = find_integer()
        except AttributeError:
            audio_count = 0
        return audio_count, video_count
