#!/usr/bin/env python
"""Load WHP metadata into Thoth"""

import logging
from bookloader import BookLoader


class WHPLoader(BookLoader):
    """WHP specific logic to ingest metadata from CSV into Thoth"""
    single_imprint = True
    publisher_name = "The White Horse Press"
    publisher_shortname = "WHP"
    publisher_url = "https://www.whpress.co.uk/"

    def run(self):
        """Process CSV and call Thoth to insert its data"""
        for row in self.data.index:
            work = self.get_work(row, self.imprint_id)
            work_id = self.thoth.create_work(work)
            logging.info('workId: %s' % work_id)
            self.create_publications(row, work_id)
            self.create_languages(row, work_id)
            self.create_subjects(row, work_id)
            self.create_contributors(row, work_id)

    def get_work(self, row, imprint_id):
        """Returns a dictionary with all attributes of a 'work'

        row: current row number

        imprint_id: previously obtained ID of this work's imprint
        """
        try:
            title = self.data.at[row, 'Distinctive Title (required)'].strip()
            subtitle = self.data.at[row, 'Subtitle'].strip()
        except (ValueError, AttributeError):
            title = self.data.at[row, 'Distinctive Title (required)'].strip()
            subtitle = None
        title = self.sanitise_title(title, subtitle)

        publication_date = self.sanitise_date(self.data.at[row, "Publication Date (required)"])

        page_count = int(self.data.at[row, "Number of Pages"]) \
            if self.data.at[row, "Number of Pages"] else None
        cc_license = self.data.at[row, "Creative Commons License URL for Open Access Book"].strip() \
            if self.data.at[row, "Creative Commons License URL for Open Access Book"] else None
        abstract = self.data.at[row, "Publisher Description of item (required)"].strip() \
            if self.data.at[row, "Publisher Description of item (required)"] else None

        main_role = self.contribution_types[self.data.at[row, "Contributor Role 1 (required)"]]
        work_type = "EDITED_BOOK" if main_role == "EDITOR" else "MONOGRAPH"

        work = {
            "workType": work_type,
            "workStatus": "ACTIVE",
            "fullTitle": title["fullTitle"],
            "title": title["title"],
            "subtitle": title["subtitle"],
            "reference": None,
            "edition": 1,
            "imprintId": imprint_id,
            "doi": None,
            "publicationDate": publication_date,
            "place": "Winwick, UK",
            "pageCount": page_count,
            "pageBreakdown": None,
            "imageCount": None,
            "tableCount": None,
            "audioCount": None,
            "videoCount": None,
            "license": cc_license,
            "copyrightHolder": None,
            "landingPage": None,
            "lccn": None,
            "oclc": None,
            "shortAbstract": None,
            "longAbstract": abstract,
            "generalNote": None,
            "bibliographyNote": None,
            "toc": None,
            "coverUrl": None,
            "coverCaption": None,
            "firstPage": None,
            "lastPage": None,
            "pageInterval": None,
        }
        return work

    # pylint: disable=too-many-locals
    def create_publications(self, row, work_id):
        """Creates all publications associated with the current work

        row: current row number

        work_id: previously obtained ID of the current work
        """

        publications = [
            ("PAPERBACK", self.sanitise_isbn(self.data.at[row, "ISBN - PAPERBACK"])),
            ("HARDBACK", self.sanitise_isbn(self.data.at[row, "ISBN - HARDCOVER"])),
            ("PDF", self.sanitise_isbn(self.data.at[row, "ISBN - PDF"])),
        ]

        for ptype, isbn in publications:
            # some books are digital only, others do not have all formats
            if not isbn:
                continue
            publication = {
                "workId": work_id,
                "publicationType": ptype,
                "isbn": isbn,
                "widthMm": None,
                "widthIn": None,
                "heightMm": None,
                "heightIn": None,
                "depthMm": None,
                "depthIn": None,
                "weightG": None,
                "weightOz": None,
            }
            self.thoth.create_publication(publication)

    def create_languages(self, row, work_id):
        """Creates all languages associated with the current work

        row: current row number

        work_id: previously obtained ID of the current work
        """
        work_lang = self.data.at[row, "Language (required)"]
        original_lang = self.data.at[row, "Original Language"]

        if work_lang and original_lang:
            languages = [
                ("TRANSLATED_INTO", work_lang),
                ("TRANSLATED_FROM", original_lang),
            ]
        else:
            languages = [("ORIGINAL", work_lang)]

        for lang_relation, language in languages:
            language = {
                "workId": work_id,
                "languageCode": language.strip().upper(),
                "languageRelation": lang_relation,
                "mainLanguage": "true"
            }
            self.thoth.create_language(language)

    def create_subjects(self, row, work_id):
        """Creates all subjects associated with the current work

        row: current row number

        work_id: previously obtained ID of the current work
        """
        bisac_codes = [self.data.at[row, "BISAC 1 (required)"], self.data.at[row, "BISAC 2"],
                       self.data.at[row, "BISAC 3"], self.data.at[row, "BISAC 4"], self.data.at[row, "BISAC 5"]]

        for index, code in enumerate(bisac_codes):
            if not code:
                continue
            subject = {
                "workId": work_id,
                "subjectType": "BISAC",
                "subjectCode": code.strip(),
                "subjectOrdinal": index + 1
            }
            self.thoth.create_subject(subject)

    def create_contributors(self, row, work_id):
        """Creates all contributions associated with the current work

        row: current row number

        work_id: previously obtained ID of the current work
        """
        contributors = [
            (self.data.at[row, "Contributor 1 (required)"], self.data.at[row, "Contributor Role 1 (required)"]),
            (self.data.at[row, "Contributor 2"], self.data.at[row, "Contributor Role 2"]),
            (self.data.at[row, "Contributor 3"], self.data.at[row, "Contributor Role 3"]),
        ]

        for index, (contributor, contribution_role) in enumerate(contributors):
            if not contributor:
                continue

            names = contributor.split(",")
            surname = names[0].strip()
            if len(names) == 1:
                name = None
                fullname = surname
            elif len(names) == 2:
                name = names[1].strip()
                surname = names[0].strip()
                fullname = "{} {}".format(name, surname)
            contributor = {
                "firstName": name,
                "lastName": surname,
                "fullName": fullname,
                "orcid": None,
                "website": None

            }
            if fullname not in self.all_contributors:
                contributor_id = self.thoth.create_contributor(
                    contributor)
                self.all_contributors[fullname] = contributor_id
            else:
                contributor_id = self.all_contributors[fullname]

            contribution = {
                "workId": work_id,
                "contributorId": contributor_id,
                "contributionType": self.contribution_types[contribution_role],
                "mainContribution": "true",
                "contributionOrdinal": index + 1,
                "biography": None,
                "firstName": name,
                "lastName": surname,
                "fullName": fullname,
            }
            self.thoth.create_contribution(contribution)
