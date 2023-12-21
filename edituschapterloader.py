"""Load Editus chapter metadata into Thoth"""
import logging

from bookloader import BookLoader
from edituschapterloaderfunctions import EDITUSChapterLoaderFunctions

"""read json file is done in bookloader.py"""

class EDITUSChapterLoader(EDITUSChapterLoaderFunctions):
    publisher_name = "Editus - Editora da UESC"
    # since these are chapters, we don't need multiple contributors & institutions
    contributors_limit = 1  # lookup not needed in this workflow
    institutions_limit = 1

    def run(self):
        """Process JSON and call Thoth to insert its data"""
        # TODO: strip chapter info from DOI
        book_doi = self[0]["doi_number"]
        book_id = self.get_work_by_doi(book_doi)['workId']
        # book_id = self.get_work_by_doi(book_doi)
        # logging.info("hello world221")
        # work_id = self.thoth.create_work(work)
        relation_ordinal = 1
        for row in len(self.data):
            work = self.get_work(row, self.imprint_id)
            logging.info(work)
            work_id = self.thoth.create_work(work)
            logging.info('Created chapter %s: %s (%s)' % (relation_ordinal, work['fullTitle'], work_id))
            self.create_languages(row, work_id)
            self.create_contributors(row, work_id)
            self.create_chapter_relation(book_id, work_id, relation_ordinal)
            relation_ordinal += 1
