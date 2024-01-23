#!/usr/bin/env python
"""Load EDITUS metadata into Thoth"""

import logging
import requests
from bookloader import BookLoader
from onix3 import Onix3Record


class EDITUSLoader(BookLoader):
    """EDITUS specific logic to ingest metadata from ONIX into Thoth"""
    import_format = "ONIX3"
    single_imprint = True
    publisher_name = "EDITUS"
    publisher_shortname = None
    publisher_url = "http://www.uesc.br/editora/"
    cache_institutions = False

    def run(self):
        """Process ONIX and call Thoth to insert its data"""
        for product in self.data.no_product_or_product:
            record = Onix3Record(product)
            work = self.get_work(record, self.imprint_id)
            logging.info(work)
            work_id = self.thoth.create_work(work)
            logging.info('workId: %s' % work_id)
            self.create_publications(record, work_id)
            self.create_contributors(record, work_id)
            self.create_languages(record, work_id)
            self.create_subjects(record, work_id)

    @staticmethod
    def get_work(record, imprint_id):
        """Returns a dictionary with all attributes of a 'work'

        record: current onix record

        imprint_id: previously obtained ID of this work's imprint
        """
        title = record.title()
        doi = record.doi()

        # resolve DOI to obtain landing page
        landing_page = requests.get(doi).url

        work = {
            "workType": record.work_type(),
            "workStatus": "ACTIVE",
            "fullTitle": title["fullTitle"],
            "title": title["title"],
            "subtitle": title["subtitle"],
            "reference": None,
            "edition": 1,
            "imprintId": imprint_id,
            "doi": doi,
            "publicationDate": record.publication_date(),
            "place": None,
            "pageCount": record.page_count(),
            "pageBreakdown": None,
            "imageCount": None,
            "tableCount": None,
            "audioCount": None,
            "videoCount": None,
            "license": record.license(),
            "copyrightHolder": None,
            "landingPage": landing_page,
            "lccn": None,
            "oclc": None,
            "shortAbstract": None,
            "longAbstract": record.long_abstract().replace("\r", ""),
            "generalNote": None,
            "bibliographyNote": None,
            "toc": None,
            "coverUrl": record.cover_url(),
            "coverCaption": None,
            "firstPage": None,
            "lastPage": None,
            "pageInterval": None,
        }
        return work

    def create_publications(self, record, work_id):
        """Creates EPUB publication and prices associated with the current work

        record: current onix record

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

        record: current onix record

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
