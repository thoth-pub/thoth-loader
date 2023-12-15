"""Load Editus chapter metadata into Thoth"""
import re
import logging

from bookloader import BookLoader
from chapterloader import ChapterLoader

"""read json file is done in bookloader.py"""

class EDITUSChapterLoader(BookLoader):
    publisher_name = "EDITUS"

    def run(self):
        logging.info("hello world")
