#!/usr/bin/env python
"""Load EDUFBA metadata into Thoth"""

from scieloloader import SciELOBookLoader


class EDUFBALoader(SciELOBookLoader):
    """EDUFBA specific logic to ingest book metadata from JSON into Thoth"""
    publisher_name = "EDUFBA"
    publisher_url = "https://books.scielo.org/edufba/"
