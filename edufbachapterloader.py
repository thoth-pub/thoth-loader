#!/usr/bin/env python
"""Load EDUFBA metadata into Thoth"""

from scieloloader import SciELOChapterLoader


class EDUFBAChapterLoader(SciELOChapterLoader):
    """EDUFBA specific logic to ingest chapter metadata from JSON into Thoth"""
    publisher_name = "EDUFBA"
    publisher_url = "https://books.scielo.org/edufba/"
