#!/usr/bin/env python
"""Load LSE Press metadata into Thoth"""

import logging
import requests
from urllib.parse import urlparse
from bookloader import BookLoader
from onix3 import Onix3Record


class LSELoader(BookLoader):
    """LSE Press specific logic to ingest metadata from ONIX into Thoth"""
    import_format = "ONIX3"
    single_imprint = True
    publisher_name = "LSE Press"
    publisher_shortname = None
    publisher_url = "https://press.lse.ac.uk/"
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
            "reference": record.reference(),
            "edition": 1,
            "imprintId": imprint_id,
            "doi": doi,
            "publicationDate": record.publication_date(),
            "place": record.publication_place(),
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
        """Creates PDF publication and locations associated with the current work

        record: current onix record

        work_id: previously obtained ID of the current work
        """
        def create_location(landing_page, full_text_url, location_platform, canonical):
            location = {
                "publicationId": publication_id,
                "landingPage": landing_page,
                "fullTextUrl": full_text_url,
                "locationPlatform": location_platform,
                "canonical": canonical,
            }
            self.thoth.create_location(location)
            logging.info(location)

        publication = {
            "workId": work_id,
            "publicationType": "PDF",
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

        # OAPEN location
        oapen_full_text_url = record.oapen_url()
        oapen_url_path = urlparse(oapen_full_text_url).path.split('/')
        oapen_landing_page = f"https://library.oapen.org/handle/{oapen_url_path[2]}/{oapen_url_path[3]}"
        create_location(oapen_landing_page, oapen_full_text_url, "OAPEN", "true")

        # DOAB location
        doab_cover = record.cover_url()
        cover_url_path = urlparse(doab_cover).path.split('/')
        doab_landing_page = f"https://directory.doabooks.org/handle/{cover_url_path[3]}/{cover_url_path[4]}"
        create_location(doab_landing_page, None, "DOAB", "false")

    def create_contributors(self, record, work_id):
        """Creates all contributions associated with the current work

        record: current onix record

        work_id: previously obtained ID of the current work
        """
        for contributor in record.contributors():
            name = contributor.choice[0].value
            surname = contributor.choice[1].value
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

        for index, code in enumerate(record.bic_codes()):
            create_subject("BIC", code, index + 1)

        for index, code in enumerate(record.keywords()):
            create_subject("KEYWORD", code, index + 1)
