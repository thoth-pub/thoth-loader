#!/usr/bin/env python
"""Load EDITUS metadata into Thoth"""

from scieloloader import SciELOLoader

class EDITUSLoader(SciELOLoader):
    """EDITUS specific logic to ingest metadata from JSON into Thoth"""
    import_format = "JSON"
    single_imprint = True
    publisher_name = "EDITUS"
    publisher_shortname = None
    publisher_url = "http://www.uesc.br/editora/"
    cache_institutions = False

