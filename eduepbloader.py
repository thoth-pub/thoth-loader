#!/usr/bin/env python
"""Load EDUEPB metadata into Thoth"""

from scieloloader import SciELOBookLoader

class EDUEPBLoader(SciELOBookLoader):
    """EDUEPB specific logic to ingest book metadata from JSON into Thoth"""
    publisher_name = "EDUEPB"
    publisher_url = "https://books.scielo.org/eduepb/"

