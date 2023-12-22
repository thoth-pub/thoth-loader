#!/usr/bin/env python
"""Load EDITUS metadata into Thoth"""

import logging
import requests
from editusbookloaderfunctions import EditusBookLoaderFunctions
from thothlibrary import ThothError


class EDITUSLoader(EditusBookLoaderFunctions):
    """EDITUS specific logic to ingest metadata from JSON into Thoth"""
    import_format = "JSON"
    single_imprint = True
    publisher_name = "EDITUS"
    publisher_shortname = None
    publisher_url = "http://www.uesc.br/editora/"
    cache_institutions = False

    def run(self):
        """Process JSON and call Thoth to insert its data"""
        logging.info("run function in editusloader.py")
        for record in self.data:
            # logging.info("Running get_work in editusloader")
            work = self.get_work(record, self.imprint_id)
            try:
                work_id = self.thoth.work_by_doi(work['doi']).workId
                existing_work = self.thoth.work_by_id(work_id)
                existing_work.update((k, v) for k, v in work.items() if v is not None)
                self.thoth.update_work(existing_work)
            except (IndexError, AttributeError, ThothError):
                work_id = self.thoth.create_work(work)
            logging.info('workId: %s' % work_id)
            self.create_pdf_publication(record, work_id)
            # self.create_contributors(record, work_id)
            # self.create_languages(record, work_id)
            # self.create_subjects(record, work_id)

    # @staticmethod TODO: ask Javi why this was here as a static method and if that's necessary.
    def get_work(self, record, imprint_id):
        """Returns a dictionary with all attributes of a 'work'

        record: current JSON record

        imprint_id: previously obtained ID of this work's imprint
        """
        title = self.split_title(record["title"])
        publication_date = self.sanitise_date(record["year"])

        editus_work_types = {
        "Monograph": "MONOGRAPH",
        "MONOGRAPH": "MONOGRAPH",
        "Book": "MONOGRAPH",
        "Edited book": "EDITED_BOOK",
        "Edited Book": "EDITED_BOOK",
        "EDITED_BOOK": "EDITED_BOOK",
        "Journal Issue": "JOURNAL_ISSUE",
        "Journal": "JOURNAL_ISSUE"
        }

        work = {
            "workType": editus_work_types[record["TYPE"]], # TODO: refactor to use work_types dictionary from bookloader. Ask Javi about this
            "workStatus": "ACTIVE",
            "fullTitle": title["fullTitle"],
            "title": title["title"],
            "subtitle": title["subtitle"],
            "reference": record["_id"],
            "edition": 1,
            "imprintId": imprint_id,
            "doi": record["doi_number"],
            "publicationDate": publication_date,
            "place": record["city"] + ", " + record["country"],
            "pageCount": record["pages"],
            "pageBreakdown": None,
            "imageCount": None,
            "tableCount": None,
            "audioCount": None,
            "videoCount": None,
            "license": record["use_licence"],
            "copyrightHolder": None,
            "landingPage": record["books_url"],
            "lccn": None,
            "oclc": None,
            "shortAbstract": None,
            "longAbstract": record["synopsis"],
            "generalNote": None,
            "bibliographyNote": None,
            "toc": None,
            "coverUrl": record["cover_url"],
            "coverCaption": None,
            "firstPage": None,
            "lastPage": None,
            "pageInterval": None,
        }
        return work

    def create_pdf_publication(self, record, work_id):
        """Creates PDF publication and location associated with the current work

        record: current JSON record

        work_id: previously obtained ID of the current work
        """

        publication = {
            "workId": work_id,
            "publicationType": "PDF",
            "isbn": None,
            "widthMm": None,
            "widthIn": None,
            "heightMm": None,
            "heightIn": None,
            "depthMm": None,
            "depthIn": None,
            "weightG": None,
            "weightOz": None,
        }
        pdf_publication_id = self.thoth.create_publication(publication)
        logging.info(publication)

        def create_pdf_location():
            location = {
                "publicationId": pdf_publication_id,
                "landingPage": record["books_url"],
                "fullTextUrl": record["pdf_url"],
                "locationPlatform": "OTHER", #TODO: Ask Javi to add SciELO to the list of location platforms
                "canonical": "true",
            }
            self.thoth.create_location(location)
            logging.info(location)
        create_pdf_location()

    def create_contributors(self, record, work_id):
        """Creates all contributions associated with the current work

        record: current JSON record

        work_id: previously obtained ID of the current work
        """
        for contributor in record.contributors():
            full_name_inverted = contributor.choice[1].value.split(',')
            name = full_name_inverted[1]
            surname = full_name_inverted[0]
            fullname = f"{name} {surname}"
            contribution_type = self.contribution_types[contributor.contributor_role[0].value.value]
            contribution_ordinal = int(contributor.sequence_number.value)
            contributor = {
                "firstName": name,
                "lastName": surname,
                "fullName": fullname,
                "orcid": None,
                "website": None
            }
            if fullname not in self.all_contributors:
                contributor_id = self.thoth.create_contributor(contributor)
                self.all_contributors[fullname] = contributor_id
            else:
                contributor_id = self.all_contributors[fullname]

            contribution = {
                "workId": work_id,
                "contributorId": contributor_id,
                "contributionType": contribution_type,
                "mainContribution": "true",
                "contributionOrdinal": contribution_ordinal,
                "biography": None,
                "firstName": name,
                "lastName": surname,
                "fullName": fullname,
            }
            logging.info(contribution)
            self.thoth.create_contribution(contribution)

    def create_languages(self, record, work_id):
        """Creates language associated with the current work

        record: current onix record

        work_id: previously obtained ID of the current work
        """
        language = {
            "workId": work_id,
            "languageCode": record.language_code(),
            "languageRelation": "ORIGINAL",
            "mainLanguage": "true"
        }
        logging.info(language)
        self.thoth.create_language(language)

    def create_subjects(self, record, work_id):
        """Creates all subjects associated with the current work

        record: current onix record

        work_id: previously obtained ID of the current work
        """
        def create_subject(subject_type, subject_code, subject_ordinal):
            subject = {
                "workId": work_id,
                "subjectType": subject_type,
                "subjectCode": subject_code,
                "subjectOrdinal": subject_ordinal
            }
            self.thoth.create_subject(subject)
            logging.info(subject)

        for index, code in enumerate(record.bisac_codes()):
            create_subject("BISAC", code, index + 1)
