#!/usr/bin/env python
"""Load L'Harmattan OA metadata into Thoth"""

import logging
from bookloader import BookLoader

class LHarmattanLoader(BookLoader):
    """L'Harmattan specific logic to ingest metadata from CSV into Thoth"""
    single_imprint = True
    cache_institutions = False
    publisher_name = "L'Harmattan Open Access"
    publisher_shortname = "L'Harmattan"
    publisher_url = "https://openaccess.hu"
    separation = ";"

    def run(self):
        """Process CSV and call Thoth to insert its data"""
        for index, row in self.data.iterrows():
            work = self.get_work(row, self.imprint_id)
            logging.info("\n\n\n\n**********")
            logging.info(work)


    def get_work(self, row, imprint_id):
        """Returns a dictionary with all attributes of a 'work'

        row: current row number

        imprint_id: previously obtained ID of this work's imprint
        """


        reference = row["uid"]
        doi = self.sanitise_doi(row["scs023_doi"])
        # TODO: need to map L'Harmattan taxonomy types
        # "text edition", "academic notes", "literary translation",
        # to allowed work types in Thoth: Monograph,
        # Edited Book, Textbook, Book Set
        # temporary workaround below
        work_type = row["taxonomy_EN"]
        if work_type in self.work_types:
            work_type = self.work_types[row["taxonomy_EN"]]
        else:
            work_type = "MONOGRAPH"

        # TODO: is this the right way to put in a title of a work in multiple languages?
        # TODO: sometimes English title is blank
        title = row["title"].strip() + " / " + row["scs023_title_en"].strip()
        # date only available as year; add date to Thoth as 01-01-YYYY
        date = self.sanitise_date(row["date"])
        # TODO: Formatting: add multiple places to Thoth separated by ;, e.g. "Budapest; Paris"?
        place = row["scs023_place"]
        # TODO: Formatting: separate original and translation by line break?
        # If English scs023_summary == scs023_summary_en,
        # do not duplicate in Thoth
        # TODO: sometimes English summary is blank
        long_abstract = row["scs023_summary"] +  " / " + row["scs023_summary_en"]
        # TODO: convert "edition in English, e.g. "First edition" to 1, "Second edition" to 2, etc.
        edition = row["edition-info_EN"]
        license = "CC BY-NC-ND 4.0"

        work = {
            "workType": work_type,
            "workStatus": "ACTIVE",
            # TODO: fix fullTitle, title, subtitle
            "fullTitle": title,
            "title": None,
            "subtitle": None,
            "reference": reference,
            "edition": edition,
            "imprintId": imprint_id,
            "doi": doi,
            "publicationDate": date,
            "place": place,
            "pageCount": None,
            "pageBreakdown": None,
            "imageCount": None,
            "tableCount": None,
            "audioCount": None,
            "videoCount": None,
            "license": license,
            "copyrightHolder": None,
            "landingPage": doi,
            "lccn": None,
            "oclc": None,
            "shortAbstract": None,
            "longAbstract": long_abstract,
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
