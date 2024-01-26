#!/usr/bin/env python
"""Load EDUEPB metadata into Thoth"""

from scieloloader import SciELOLoader

class EDUEPBLoader(SciELOLoader):
    """EDUEPB specific logic to ingest metadata from JSON into Thoth"""
    publisher_name = "EDUEPB"
    publisher_url = "https://books.scielo.org/eduepb/"

