"""Load a CSV file into Thoth"""
import re
import pandas as pd
import isbn_hyphenate
import json
import pymarc
import roman
import logging
import sys
from onix.book.v3_0.reference.strict import Onixmessage
from xsdata.formats.dataclass.parsers import XmlParser
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


class BookLoader:
    """Generic logic to ingest metadata from CSV, MARCXML, ONIX, or JSON into Thoth"""
    allowed_formats = ["CSV", "MARCXML", "ONIX3", "JSON"]
    import_format = "CSV"
    single_imprint = True
    publisher_name = None
    publisher_shortname = None
    publisher_url = None
    cache_contributors = True
    cache_institutions = True
    cache_series = False
    cache_issues = False
    cache_pagination_size = 20000
    all_contributors = {}
    all_institutions = {}
    all_series = {}
    all_issues = {}
    encoding = "utf-8"
    header = 0
    separation = ","
    work_types = {
        "Monograph": "MONOGRAPH",
        "MONOGRAPH": "MONOGRAPH",
        "monograph": "MONOGRAPH",
        "Book": "MONOGRAPH",
        "Edited book": "EDITED_BOOK",
        "Edited Book": "EDITED_BOOK",
        "EDITED_BOOK": "EDITED_BOOK",
        "edited book": "EDITED_BOOK",
        "Journal Issue": "JOURNAL_ISSUE",
        "Journal": "JOURNAL_ISSUE",
        "textbook": "TEXTBOOK"
    }
    work_statuses = {
        "Active": "ACTIVE",
        "ACTIVE": "ACTIVE",
        "04": "ACTIVE",
        "Cancelled": "CANCELLED",
        "Forthcoming": "FORTHCOMING",
        "02": "FORTHCOMING",
        "Out of print": "OUT_OF_PRINT",
        "07": "OUT_OF_PRINT",
        "Withdrawn": "WITHDRAWN_FROM_SALE",
        "05": "NO_LONGER_OUR_PRODUCT",
        "06": "OUT_OF_STOCK_INDEFINITELY",
        "09": "UNKNOWN",
    }
    contribution_types = {
        "Author": "AUTHOR",
        "author": "AUTHOR",
        "AUTHOR": "AUTHOR",
        "AUHTOR": "AUTHOR",
        "A01": "AUTHOR",
        "individual_author": "AUTHOR",
        # A02 = "With or as told to"
        "A02": "AUTHOR",
        "editor": "EDITOR",
        "Editor": "EDITOR",
        "EDITOR": "EDITOR",
        "B01": "EDITOR",
        "B02": "EDITOR",
        # B09 = "Series edited by"
        "B09": "EDITOR",
        # B12 = "Guest editor"
        "B12": "EDITOR",
        "B13": "EDITOR",
        "C99": "EDITOR",
        "organizer": "EDITOR",
        "Translator": "TRANSLATOR",
        "translator": "TRANSLATOR",
        "Photographer": "PHOTOGRAPHER",
        "Illustrator": "ILLUSTRATOR",
        "B06": "TRANSLATOR",
        "Foreword": "FOREWORD_BY",
        "A24": "INTRODUCTION_BY",
        "Introduction": "INTRODUCTION_BY",
        "writer of introduction": "INTRODUCTION_BY",
        "A15": "PREFACE_BY",
        "Preface": "PREFACE_BY",
        "Music editor": "MUSIC_EDITOR",
        "Research By": "RESEARCH_BY",
        "Contributions By": "CONTRIBUTIONS_BY",
        # B18 = "Prepared for publication by"
        "B18": "CONTRIBUTIONS_BY",
        # A32 = "Contributions by: Author of additional contributions to the text"
        "A32": "CONTRIBUTIONS_BY",
        # A43 = "Interviewer"
        "A43": "CONTRIBUTIONS_BY",
        # A44 = "Interviewee"
        "A44": "CONTRIBUTIONS_BY",
        # A19 = "Afterword by"
        "A19": "CONTRIBUTIONS_BY",
    }
    publication_types = {
        "BB": "HARDBACK",
        "BC": "PAPERBACK",
        "B106": "PAPERBACK",
        "B402": "HARDBACK",
        "E101": "EPUB",
        "E105": "HTML",
        "E107": "PDF",
        "E116": "AZW3",
        "Paperback": "PAPERBACK",
        "Hardback": "HARDBACK",
        "KINDLE": "AZW3"
    }

    language_codes = {
        "pt": "POR",
        "en": "ENG",
        "es": "SPA",
    }

    dimension_types = {
        ("02", "mm"): "widthMm",
        ("02", "cm"): "widthCm",
        ("02", "in"): "widthIn",
        ("01", "mm"): "heightMm",
        ("01", "cm"): "heightCm",
        ("01", "in"): "heightIn",
        ("03", "mm"): "depthMm",
        ("03", "cm"): "depthCm",
        ("03", "in"): "depthIn",
        ("08", "gr"): "weightG",
        ("08", "oz"): "weightOz",
    }

    main_contributions = ["AUTHOR", "EDITOR", "TRANSLATOR"]
    orcid_regex = re.compile(
        r'0000-000(1-[5-9]|2-[0-9]|3-[0-4])\d{3}-\d{3}[\dX]')
    int_regex = re.compile(r'\d+')
    audio_regex = re.compile(r'[0-9]{1,3} \(aud\)')
    video_regex = re.compile(r'[0-9]{1,3} \(vid\)')

    def __init__(self, metadata_file, client_url, email, password):
        if self.import_format not in self.allowed_formats:
            raise
        self.metadata_file = metadata_file
        self.thoth = ThothClient(client_url)
        self.thoth.login(email, password)

        if self.import_format == "CSV":
            self.data = self.prepare_csv_file()
        elif self.import_format == "MARCXML":
            self.data = self.prepare_marcxml_file()
        elif self.import_format == "ONIX3":
            self.data = self.prepare_onix3_file()
        elif self.import_format == "JSON":
            self.data = self.prepare_json_file()

        try:
            self.set_publisher_and_imprint()
        except Exception:
            # Publisher name may not be set at this point, which is OK
            pass

        if self.cache_contributors:
            # create cache of all existing contributors using pagination
            for offset in range(0, self.thoth.contributor_count(), self.cache_pagination_size):
                for c in self.thoth.contributors(limit=self.cache_pagination_size, offset=offset):
                    self.all_contributors[c.fullName] = c.contributorId
                    if c.orcid:
                        self.all_contributors[c.orcid] = c.contributorId
        if self.cache_institutions:
            # create cache of all existing institutions using pagination
            for offset in range(0, self.thoth.institution_count(), self.cache_pagination_size):
                for i in self.thoth.institutions(limit=self.cache_pagination_size, offset=offset):
                    self.all_institutions[i.institutionName] = i.institutionId
                    if i.ror:
                        self.all_institutions[i.ror] = i.institutionId
        if self.cache_series:
            # create cache of all existing series using pagination
            for offset in range(0, self.thoth.series_count(), self.cache_pagination_size):
                for series in self.thoth.serieses(limit=self.cache_pagination_size, offset=offset):
                    self.all_series[series.seriesName] = series.seriesId
        if self.cache_issues:
            # create cache of all existing issues using pagination
            for offset in range(0, self.thoth.issue_count(), self.cache_pagination_size):
                for issue in self.thoth.issues(limit=self.cache_pagination_size, offset=offset):
                    self.all_issues[issue.work.workId] = issue.issueId

    def prepare_csv_file(self):
        """Read CSV, convert empties to None and rename duplicate columns"""
        frame = pd.read_csv(self.metadata_file, encoding=self.encoding,
                            header=self.header, sep=self.separation)
        frame = frame.where(pd.notnull(frame), None)
        frame = frame.rename(columns=Deduper())
        return frame

    def prepare_marcxml_file(self):
        """Read MARC XML"""
        collection = pymarc.marcxml.parse_xml_to_array(self.metadata_file)
        return collection

    def prepare_onix3_file(self):
        """Read ONIX 3.0"""
        parser = XmlParser()
        message = parser.parse(self.metadata_file, Onixmessage)
        return message

    def prepare_json_file(self):
        """Read JSON"""
        with open(self.metadata_file) as raw_json:
            prepared_json = json.load(raw_json)
        return prepared_json

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

    def set_publisher_and_imprint(self):
        if self.publisher_name:
            publishers = self.thoth.publishers(
                        search=self.publisher_name)
            try:
                self.publisher_id = publishers[0].publisherId
            except (IndexError, AttributeError):
                self.publisher_id = self.create_publisher()
            try:
                self.imprint_id = publishers[0].imprints[0].imprintId
            except (IndexError, AttributeError):
                self.imprint_id = self.create_imprint()
        else:
            # Searching on an empty publisher name would return
            # the full set of publishers and select the first one
            raise

    def is_main_contribution(self, contribution_type):
        """Return a boolean string ready for ingestion"""
        return "true" \
            if contribution_type in self.main_contributions \
            else "false"

    def check_update_contributor(self, contributor, contributor_id):
        # find existing contributor in Thoth
        contributor_record = self.thoth.contributor(contributor_id, True)
        thoth_contributor = json.loads(contributor_record)['data']['contributor']
        # remove unnecessary fields for comparison to contributor
        del thoth_contributor['__typename']
        del thoth_contributor['contributions']
        # add contributorId to contributor dictionary so it can be compared to thoth_contributor
        contributor["contributorId"] = contributor_id
        if contributor != thoth_contributor:
            combined_contributor = {}
            # some contributors may have contributed to multiple books and be in the JSON multiple times
            # with profile_link containing different values.
            # Combine the dictionaries and keep the value that is not None.
            for key in set(thoth_contributor) | set(contributor):
                if contributor[key] is not None:
                    combined_contributor[key] = contributor[key]
                else:
                    combined_contributor[key] = thoth_contributor[key]
            # combined contributor now contains the values from both dictionaries
            # however, if all of these values are already in Thoth, there's no need to update
            # so only update if combined_contributor is different from thoth_contributor
            if combined_contributor != thoth_contributor:
                self.thoth.update_contributor(combined_contributor)
                logging.info(f"updated contributor: {contributor_id}")
        else:
            logging.info(f"existing contributor, no changes needed to Thoth: {contributor_id}")
        return contributor

    def get_book_by_title(self, title):
        """Query Thoth to find a book given its title"""
        try:
            books = self.thoth.books(search=title.replace('"', '\\"'), publishers='"%s"' % self.publisher_id)
            return books[0]
        except (IndexError, AttributeError):
            logging.error('Book not found: \'%s\'' % title)
            sys.exit(1)

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
    def get_work_contributions(work):
        work_contributions = {}
        for c in work.contributions:
            work_contributions[c.fullName] = c.contributionId
            if c.contributor.orcid:
                work_contributions[c.contributor.orcid] = c.contributionId
        return work_contributions

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
            title = title.strip()
            subtitle = subtitle.strip()
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
        if len(date) == len("2023"):
            return f"{date}-01-01"
        if len(date) == len("20200101"):
            return f"{date[:4]}-{date[4:6]}-{date[6:8]}"
        return date.replace("/", "-").strip()

    @staticmethod
    def sanitise_isbn(isbn):
        """Return a hyphenated ISBN"""
        if not isbn:
            return None
        try:
            if "-" in str(isbn):
                if not len(str(isbn)) == 17:
                    raise isbn_hyphenate.IsbnMalformedError
                else:
                    return str(isbn)
            return isbn_hyphenate.hyphenate(str(int(isbn)))
        except ValueError:
            return None
        except isbn_hyphenate.IsbnMalformedError:
            print(isbn)
            raise

    @staticmethod
    def sanitise_issn(issn):
        """Return a hyphenated ISSN"""
        if not issn:
            return None
        if "-" in str(issn):
            hyphenated_issn = str(issn)
        else:
            hyphenated_issn = str(issn)[:4] + '-' + str(issn)[4:]
        if not len(hyphenated_issn) == 9:
            raise ValueError("ISSN incorrectly formatted: %s" % issn)
        return hyphenated_issn

    @staticmethod
    def sanitise_url(url):
        """Return a URL beginning https://"""
        if not url:
            return None
        if url.startswith("https://"):
            return url
        else:
            return "https://{}".format(url)

    @staticmethod
    def sanitise_doi(doi):
        """Return a DOI beginning https://doi.org/"""
        return BookLoader.sanitise_identifier(doi, "doi")

    @staticmethod
    def sanitise_orcid(orcid):
        """Return an ORCID beginning https://orcid.org/"""
        return BookLoader.sanitise_identifier(orcid, "orcid")

    @staticmethod
    def sanitise_ror(ror):
        """Return a ROR beginning https://ror.org/"""
        return BookLoader.sanitise_identifier(ror, "ror")

    @staticmethod
    def sanitise_identifier(identifier, domain):
        """Return an identifier beginning https://{domain}.org/"""
        if not identifier:
            return None
        if identifier.startswith("https://{}.org/".format(domain)):
            return identifier
        elif identifier.startswith("http://{}.org/".format(domain)):
            return identifier.replace("http://", "https://")
        elif identifier.startswith("{}.org/".format(domain)):
            return BookLoader.sanitise_url(identifier)
        else:
            return "https://{}.org/{}".format(domain, identifier)

    @staticmethod
    def sanitise_price(price):
        """Return a float ready for ingestion"""
        try:
            return float(price.replace("$", "").strip())
        except (TypeError, AttributeError, ValueError):
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

    @staticmethod
    def sanitise_string(string):
        return string.replace('\n', '').replace('\r', '').strip()

    @staticmethod
    def parse_page_string(page_string):
        """Return the number of pages and page breakdown from MARC 300 fields"""
        matches = re.search(r'\((?:(\w+), )?(\d+) pages\)', page_string)
        if matches:
            roman_numeral = matches.group(1)
            page_number = int(matches.group(2))
            page_breakdown = None

            if roman_numeral:
                decimal_numeral = roman.fromRoman(roman_numeral.upper())
                page_breakdown = f"{roman_numeral}+{page_number - decimal_numeral}"

            return page_number, page_breakdown

        return None, None

