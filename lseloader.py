#!/usr/bin/env python
"""Load LSE Press metadata into Thoth"""

import logging
import requests
from bookloader import BookLoader
from onix3 import Onix3Record


class LSELoader(BookLoader):
    """LSE Press specific logic to ingest metadata from ONIX into Thoth"""
    import_format = "ONIX3"
    single_imprint = True
    publisher_name = "LSE Press"
    publisher_shortname = None
    publisher_url = "https://press.lse.ac.uk/"

    def run(self):
        """Process ONIX and call Thoth to insert its data"""
        for product in self.data.no_product_or_product:
            record = Onix3Record(product)
            work = self.get_work(record, self.imprint_id)
            logging.info(work)
            break

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
            "pageCount": None,
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
            "longAbstract": record.long_abstract(),
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
