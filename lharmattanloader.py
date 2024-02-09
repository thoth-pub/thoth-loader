#!/usr/bin/env python
"""Load L'Harmattan OA metadata into Thoth"""

import logging
import sys
from bookloader import BookLoader
from thothlibrary import ThothError

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
            logging.info("\n\n\n\n**********")
            logging.info(f"processing book: {row['title']}")
            work = self.get_work(row, self.imprint_id)
            # try to find the work in Thoth
            try:
                work_id = self.thoth.work_by_doi(work['doi']).workId
                existing_work = self.thoth.work_by_id(work_id)
                # if work is found, try to update it with the new data
                if existing_work:
                    try:
                        existing_work.update((k, v) for k, v in work.items() if v is not None)
                        self.thoth.update_work(existing_work)
                        logging.info(f"updated workId: {work_id}")
                    # if update fails, log the error and exit the import
                    except ThothError as t:
                        logging.error(f"Failed to update work with id {work_id}, exception: {t}")
                        sys.exit(1)
            # if work isn't found, create it
            except (IndexError, AttributeError, ThothError):
                work_id = self.thoth.create_work(work)
                logging.info(f"created workId: {work_id}")
            work = self.thoth.work_by_id(work_id)


    def get_work(self, row, imprint_id):
        """Returns a dictionary with all attributes of a 'work'

        row: current row number

        imprint_id: previously obtained ID of this work's imprint
        """


        reference = row["uid"]
        doi = self.sanitise_doi(row["scs023_doi"])
        # TODO: waiting for Szilvia response to map L'Harmattan taxonomy types
        # "text edition", "academic notes", "literary translation",
        # to allowed work types in Thoth: Monograph,
        # Edited Book, Textbook, Book Set
        # temporary workaround below
        work_type = row["taxonomy_EN"]
        if work_type in self.work_types:
            work_type = self.work_types[row["taxonomy_EN"]]
        else:
            work_type = "MONOGRAPH"
        title = self.split_title(row["title"].strip())
        # date only available as year; add date to Thoth as 01-01-YYYY
        date = self.sanitise_date(row["date"])
        place = (row["scs023_place"]).replace("|", "; ")
        long_abstract = row["scs023_summary"]
        editions_text = {
        "First edition": 1,
        "Second edition": 2,
        }
        edition = row["edition-info_EN"]
        if edition in editions_text:
            edition = editions_text[edition]
        else:
            edition = 1
        license = "https://creativecommons.org/licenses/by-nc-nd/4.0/"

        work = {
            "workType": work_type,
            "workStatus": "ACTIVE",
            "fullTitle": title["fullTitle"],
            "title": title["title"],
            "subtitle": title["subtitle"],
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
