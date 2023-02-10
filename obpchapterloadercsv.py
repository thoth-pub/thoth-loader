#!/usr/bin/env python
"""Load OBP chapter metadata into Thoth"""

import sys
import logging
from chapterloader import ChapterLoader


class ObpChapterLoaderCsv(ChapterLoader):
    """OBP specific logic to ingest chapter metadata from CSV into Thoth"""
    single_imprint = False
    publisher_name = "Open Book Publishers"

    def run(self):
        """Process CSV and call Thoth to insert its data"""
        current_book = {'fullTitle': ''}
        relation_ordinal = 0
        for row in self.data.index:
            imprint_name = self.data.at[row, "Imprint"]
            try:
                imprint_id = self.all_imprints[imprint_name]
            except (IndexError, AttributeError):
                logging.error('Imprint not found: %s' % imprint_name)
                sys.exit(1)

            work = self.get_work(row, imprint_id)
            if self.data.at[row, "Book Title"] != current_book['fullTitle']:
                current_book = self.get_book_by_title(self.data.at[row, "Book Title"])
                relation_ordinal = 1
                logging.info('Book: %s' % current_book['fullTitle'])
            else:
                relation_ordinal += 1
            work_id = self.thoth.create_work(work)
            logging.info('Created chapter %s: %s (%s)' % (relation_ordinal, work['fullTitle'], work_id))
            self.create_languages(row, work_id)
            self.create_subjects(row, work_id)
            self.create_contributors(row, work_id)
            self.create_chapter_relation(current_book['workId'], work_id, relation_ordinal)
            self.create_publications(row, work_id)

    def get_work(self, row, imprint_id):
        """Returns a dictionary with all attributes of a 'work'

        row: current row number

        imprint_id: previously obtained ID of this work's imprint
        """
        try:
            title, subtitle = self.data.at[row, 'Chapter Title'].split(":")
            title = title.strip()
            subtitle = subtitle.strip()
        except ValueError:
            title = self.data.at[row, 'Chapter Title']
            subtitle = None
        title = self.sanitise_title(title, subtitle)
        doi = "https://doi.org/{}".format(self.data.at[row, 'DOI'].strip()) \
            if self.data.at[row, 'DOI'] else None

        publication_date = self.sanitise_date(self.data.at[row, "Date"])

        copyright_text = None
        if self.data.at[row, "Authors"] and self.data.at[row, "Editors"]:
            copyright_text = self.data.at[row, "Authors"]
        elif self.data.at[row, "Authors"] and not self.data.at[row, "Editors"]:
            copyright_text = self.data.at[row, "Authors"]
        elif not self.data.at[row, "Authors"]:
            copyright_text = self.data.at[row, "Editors"]

        page_count = int(self.data.at[row, "Number of Pages"]) \
            if self.data.at[row, "Number of Pages"] else None
        first_page = self.data.at[row, "Start Page"].strip() \
            if self.data.at[row, "Start Page"] else None
        last_page = self.data.at[row, "End Page"].strip() \
            if self.data.at[row, "End Page"] else None
        page_interval = None
        if first_page and last_page:
            page_interval = "{}–{}".format(first_page, last_page)
        abstract = self.data.at[row, "Abstract"].strip() \
            if self.data.at[row, "Abstract"] else None

        work = {
            "workType": "BOOK_CHAPTER",
            "workStatus": str(self.data.at[row, "Work Status"]),
            "fullTitle": title["fullTitle"],
            "title": title["title"],
            "subtitle": title["subtitle"],
            "reference": str(self.data.at[row, "Record Reference"]),
            "edition": None,
            "imprintId": imprint_id,
            "doi": doi,
            "publicationDate": publication_date,
            "place": self.data.at[row, "Place of publication"],
            "width": None,
            "height": None,
            "pageCount": page_count,
            "pageBreakdown": None,
            "imageCount": None,
            "tableCount": None,
            "audioCount": None,
            "videoCount": None,
            "license": self.data.at[row, "License"],
            "copyrightHolder": copyright_text,
            "landingPage": self.data.at[row, "Website"],
            "lccn": None,
            "oclc": None,
            "shortAbstract": None,
            "longAbstract": abstract,
            "generalNote": None,
            "toc": None,
            "coverUrl": None,
            "coverCaption": None,
            "firstPage": first_page,
            "lastPage": last_page,
            "pageInterval": page_interval
        }
        return work

    def create_languages(self, row, work_id):
        """Creates all languages associated with the current work

        row: current row number

        work_id: previously obtained ID of the current work
        """
        lang_data = self.data.at[row, "Language"]
        if lang_data:
            languages = lang_data.upper().split(";")
            for language in languages:
                language = {
                    "workId": work_id,
                    "languageCode": language.strip(),
                    "languageRelation": "ORIGINAL",
                    "mainLanguage": "true"
                }
                self.thoth.create_language(language)

    def create_subjects(self, row, work_id):
        """Creates all subjects associated with the current work

        row: current row number

        work_id: previously obtained ID of the current work
        """
        subjects = {
            "BIC": self.data.at[row, "BIC"],
            "KEYWORD": self.data.at[row, "Keywords"],
        }
        for stype, codes in subjects.items():
            if not codes:
                continue
            for index, code in enumerate(codes.replace(",", ";").split(";")):
                if not code:
                    continue
                subject = {
                    "workId": work_id,
                    "subjectType": stype,
                    "subjectCode": code.strip(),
                    "subjectOrdinal": index + 1
                }
                self.thoth.create_subject(subject)

    def create_contributors(self, row, work_id):
        """Creates all contributions associated with the current work

        row: current row number

        work_id: previously obtained ID of the current work
        """
        contributors = {
            "TRANSLATOR": self.data.at[row, "Translator"],
            "PHOTOGRAPHER": self.data.at[row, "Photographer"],
            "ILLUSTRATOR": self.data.at[row, "Illustrator"],
            "FOREWORD_BY": self.data.at[row, "Foreword by"],
            "AFTERWORD_BY": self.data.at[row, "Afterword by"],
            "INTRODUCTION_BY": self.data.at[row, "Introduction by"],
            "PREFACE_BY": self.data.at[row, "Preface by"],
        }
        # book editors are also included when chapters have authors - ignore them
        # if authors + editors: only authors; else editors
        authors = self.data.at[row, "Authors"]
        editors = self.data.at[row, "Editors"]
        if authors:
            contributors["AUTHOR"] = authors
        elif not authors and editors:
            contributors["EDITOR"] = editors

        contribution_ordinal = 0;
        for contribution_type, people in contributors.items():
            if not people:
                continue
            for contributor in people.strip().split(";"):
                if not contributor:
                    continue
                names = contributor.split(",")
                surname = names[0].strip()
                name = None
                orcid = None
                if len(names) == 1:
                    fullname = surname
                elif len(names) == 2:
                    name = names[1].strip()
                    surname = names[0].strip()
                    try:
                        orcid = self.orcid_regex.search(name).group(0)
                        name = name.replace(" ({})".format(orcid), "")
                    except AttributeError:
                        orcid = None
                    fullname = "{} {}".format(name, surname).strip()
                    if name == "":
                        name = None
                elif len(names) == 3:
                    # most probably an institution
                    fullname = surname = '%s, %s, %s' % (names[2].strip(), names[1].strip(), names[0].strip())
                contributor = {
                    "firstName": name,
                    "lastName": surname,
                    "fullName": fullname,
                    "orcid": orcid,
                    "website": None

                }
                if fullname not in self.all_contributors:
                    contributor_id = self.thoth.create_contributor(contributor)
                    self.all_contributors[fullname] = contributor_id
                else:
                    contributor_id = self.all_contributors[fullname]

                contribution_ordinal += 1
                contribution = {
                    "workId": work_id,
                    "contributorId": contributor_id,
                    "contributionType": contribution_type,
                    "mainContribution": self.is_main_contribution(
                        contribution_type
                    ),
                    "biography": None,
                    "institution": None,
                    "firstName": name,
                    "lastName": surname,
                    "fullName": fullname,
                    "contributionOrdinal": contribution_ordinal
                }
                self.thoth.create_contribution(contribution)

    def create_publications(self, row, work_id):
        """Creates all publications associated with the current work
        row: current row number
        work_id: previously obtained ID of the current work
        """
        for pub_type, csv_heading in [("PDF", "Full-text URL - PDF"),
                                      ("HTML", "Full-text URL - HTML")]:
            url = self.data.at[row, csv_heading]
            if url:
                query_pub = {
                    "workId": work_id,
                    "publicationType": pub_type,
                    "isbn": None,
                    "widthMm": None,
                    "widthIn": None,
                    "heightMm": None,
                    "heightIn": None,
                    "depthMm": None,
                    "depthIn": None,
                    "weightG": None,
                    "weightOz": None
                }
                id = self.thoth.create_publication(query_pub)

                query_loc = {
                    "publicationId": id,
                    "landingPage": url,
                    "fullTextUrl": url,
                    "locationPlatform": "OTHER",
                    "canonical": "true"
                }
                self.thoth.create_location(query_loc)
