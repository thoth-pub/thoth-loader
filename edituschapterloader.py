"""Load Editus chapter metadata into Thoth"""
import logging

from bookloader import BookLoader
from chapterloader import ChapterLoader

"""read json file is done in bookloader.py"""

class EDITUSChapterLoader(ChapterLoader):
    publisher_name = "EDITUS"

    def run(self):
        """Process JSON and call Thoth to insert its data"""
        book_doi = self[0]["doi_number"]
        book_id = self.get_work_by_doi(book_doi)
        logging.info("hello world221")
        work_id = self.thoth.create_work(work)
        return book_id
