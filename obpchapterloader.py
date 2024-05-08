#!/usr/bin/env python
"""Load OBP chapter metadata into Thoth"""

import re
import logging

from bookloader import BookLoader
from chapterloader import ChapterLoader


class ObpChapterLoader(ChapterLoader):
    """OBP specific logic to ingest chapter metadata from CSV into Thoth"""
    publisher_name = "Open Book Publishers"
    contributors_limit = 1  # lookup not needed in this workflow
    institutions_limit = 1

    def run(self):
        """Process CSV and call Thoth to insert its data"""
        book_doi = self.data.at[0, 'doi'].strip().lower()[:-3]  # OBP chapter DOIs are book's + ".00"
        book_id = self.get_work_by_doi(book_doi)['workId']

        relation_ordinal = 1
        for row in self.data.index:
            work = self.get_work(row, self.imprint_id)
            logging.info(work)
            work_id = self.thoth.create_work(work)
            logging.info('Created chapter %s: %s (%s)' % (relation_ordinal, work['fullTitle'], work_id))
            self.create_languages(row, work_id)
            self.create_contributors(row, work_id)
            self.create_chapter_relation(book_id, work_id, relation_ordinal)
            relation_ordinal += 1

    def get_work(self, row, imprint_id):
        """Returns a dictionary with all attributes of a 'work'

        row: current row number

        imprint_id: previously obtained ID of this work's imprint
        """
        title = self.data.at[row, 'title']
        subtitle = self.data.at[row, 'subtitle']
        title = self.sanitise_title(title, subtitle)
        doi = self.data.at[row, 'doi'].strip().lower()

        cc_license = "https://creativecommons.org/licenses/{}/4.0/".format(self.data.at[row, 'license'].lower())

        # Produce landing page based on OBP's convention
        chapter_doi = doi.replace("https://doi.org/", "")
        book_doi = chapter_doi[:-3]
        landing_page = f"https://www.openbookpublishers.com/books/{book_doi}/chapters/{chapter_doi}"

        page_count = int(self.data.at[row, "page_count"]) \
            if self.data.at[row, "page_count"] else None
        first_page = str(self.data.at[row, "first_page"]) \
            if self.data.at[row, "first_page"] else None
        last_page = str(self.data.at[row, "last_page"]) \
            if self.data.at[row, "last_page"] else None
        page_interval = None
        if first_page and last_page:
            page_interval = "{}–{}".format(first_page, last_page)
        abstract = self.data.at[row, "long_abstract"].strip() \
            if self.data.at[row, "long_abstract"] else None

        work = {
            "workType": "BOOK_CHAPTER",
            "workStatus": "FORTHCOMING",
            "fullTitle": title["fullTitle"],
            "title": title["title"],
            "subtitle": title["subtitle"],
            "reference": None,
            "edition": None,
            "imprintId": imprint_id,
            "doi": doi,
            "publicationDate": None,
            "place": "Cambridge, UK",
            "width": None,
            "height": None,
            "pageCount": page_count,
            "pageBreakdown": None,
            "imageCount": None,
            "tableCount": None,
            "audioCount": None,
            "videoCount": None,
            "license": cc_license,
            "copyrightHolder": self.data.at[row, "copyright_holder"],
            "landingPage": landing_page,
            "lccn": None,
            "oclc": None,
            "shortAbstract": None,
            "longAbstract": abstract,
            "generalNote": None,
            "bibliographyNote": None,
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
        for index in range(1, 4):
            try:
                language = {
                    "workId": work_id,
                    "languageCode": self.data.at[row, f"language_code_{index}"].upper(),
                    "languageRelation": self.data.at[row, f"language_relation_{index}"].upper().replace(' ', '_'),
                    "mainLanguage": "true" if self.data.at[row, f"main_language_{index}"] == "Yes" else "false",
                }
                language_id = self.thoth.create_language(language)
                logging.info(f"Language: {language_id}")
            except AttributeError:
                continue

    def create_contributors(self, row, work_id):
        """Creates all contributions associated with the current work

        row: current row number

        work_id: previously obtained ID of the current work
        """
        for index in range(1, 21):
            contributor_id = self.data.at[row, f"contributor_id_{index}"]
            if not contributor_id:
                continue
            contributor_id = contributor_id.strip()
            contribution_type = BookLoader.contribution_types[self.data.at[row, f"contribution_type_{index}"].strip()]
            main_contribution = "true" if self.data.at[row, f"main_contribution_{index}"].strip() == "Yes" else "false"
            biography = self.data.at[row, f"biography_{index}"].strip() if self.data.at[row, f"biography_{index}"] else None

            contributor = self.thoth.contributor(contributor_id)

            contribution = {
                "workId": work_id,
                "contributorId": contributor_id,
                "contributionType": contribution_type,
                "mainContribution": main_contribution,
                "biography": biography,
                "institution": None,
                "firstName": contributor['firstName'],
                "lastName": contributor['lastName'],
                "fullName": contributor['fullName'],
                "contributionOrdinal": index
            }
            contribution_id = self.thoth.create_contribution(contribution)
            logging.info(f"Contribution: {contribution_id}")

            # now create affiliations
            for affiliation_ordinal in range(1, 4):
                institution_id = self.data.at[row, f"institution_id_{index}_{affiliation_ordinal}"]
                if not institution_id:
                    continue
                institution_id = institution_id.strip()
                try:
                    position = self.data.at[row, f"position_{index}_{affiliation_ordinal}"].strip()
                except AttributeError:
                    position = None

                affiliation = {
                    "contributionId": contribution_id,
                    "institutionId": institution_id,
                    "position": position,
                    "affiliationOrdinal": affiliation_ordinal
                }
                affiliation_id = self.thoth.create_affiliation(affiliation)
                logging.info(f"Affiliation {affiliation_id}")
