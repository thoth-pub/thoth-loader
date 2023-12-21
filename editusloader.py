#!/usr/bin/env python
"""Load EDITUS metadata into Thoth"""

import logging
import requests
from editusbookloaderfunctions import EditusBookLoaderFunctions


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
        # TODO: change
        logging.info("run function in editusloader.py")
        for record in self.data:
        # for product in self.data.no_product_or_product:
            # logging.info(record)
            logging.info("Running get_work in editusloader")
            work = self.get_work(record, self.imprint_id)
            # logging.info(work)
            # TODO: Either ubiquity or Onix3 loader contains logic to overwrite existing records
            work_id = self.thoth.create_work(work)
            logging.info('workId: %s' % work_id)
            # self.create_publications(record, work_id)
            # self.create_contributors(record, work_id)
            # self.create_languages(record, work_id)
            # self.create_subjects(record, work_id)

    @staticmethod
    def get_work(record, imprint_id):
        """Returns a dictionary with all attributes of a 'work'

        record: current JSON record

        imprint_id: previously obtained ID of this work's imprint
        """
        # title = record["title"]
        # logging.info("Title recorded in work as " + title)
        # doi = record.doi()

        # resolve DOI to obtain landing page
        # landing_page = requests.get(doi).url

        work = {
            # TODO: fix this using work_types so it is read from the JSON
            "workType": "MONOGRAPH",
            "workStatus": "ACTIVE",
            "fullTitle": record["title"],
            "title": record["title"], #TODO: if there's a colon, put rest in subtitle field
            "subtitle": None,
            "reference": record["_id"],
            "edition": 1,
            "imprintId": imprint_id,
            "doi": record["doi_number"],
            "publicationDate": record["year"], #TODO: currently just Y, make into M/D/Y
            "place": record["city"], #TODO: concat with "country" from JSON
            "pageCount": record["pages"],
            "pageBreakdown": None,
            "imageCount": None,
            "tableCount": None,
            "audioCount": None,
            "videoCount": None,
            "license": record["use_licence"],
            "copyrightHolder": None,
            "landingPage": record["doi_number"],
            "lccn": None,
            "oclc": None,
            "shortAbstract": None,
            "longAbstract": record["synopsis"],
            "generalNote": None,
            "bibliographyNote": None,
            "toc": None,
            "coverUrl": None, # TODO: write a method to construct cover URL from "cover" "filename" in JSON
            "coverCaption": None,
            "firstPage": None,
            "lastPage": None,
            "pageInterval": None,
        }
        return work

    def create_pdf_publications(self, record, work_id):
        """Creates PDF publication and prices associated with the current work

        record: current JSON record

        work_id: previously obtained ID of the current work
        """
        def create_price():
            price = {
                "publicationId": publication_id,
                "currencyCode": currency_code,
                "unitPrice": unit_price,
            }
            self.thoth.create_price(price)
            logging.info(price)

        publication = {
            "workId": work_id,
            "publicationType": "EPUB",
            "isbn": record.isbn(),
            "widthMm": None,
            "widthIn": None,
            "heightMm": None,
            "heightIn": None,
            "depthMm": None,
            "depthIn": None,
            "weightG": None,
            "weightOz": None,
        }
        publication_id = self.thoth.create_publication(publication)
        logging.info(publication)

        for currency_code, unit_price in record.prices():
            create_price()

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
