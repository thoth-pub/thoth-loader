#!/usr/bin/env python
"""Load EDUEPB metadata into Thoth"""

from scieloloader import SciELOLoader

class EDUEPBLoader(SciELOLoader):
    """EDUEPB specific logic to ingest metadata from JSON into Thoth"""
    import_format = "JSON"
    single_imprint = True
    publisher_name = "EDUEPB"
    publisher_shortname = None
    publisher_url = "https://books.scielo.org/eduepb/"
    cache_institutions = False

