#!/usr/bin/env python
"""Load EDUEPB metadata into Thoth"""

from scieloloader import SciELOChapterLoader


class EDUEPBChapterLoader(SciELOChapterLoader):
    """EDUEPB specific logic to ingest chapter metadata from JSON into Thoth"""
    publisher_name = "EDUEPB"
    publisher_url = "https://books.scielo.org/eduepb/"
