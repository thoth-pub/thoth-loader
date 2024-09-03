#!/usr/bin/env python
"""Load EDITUS metadata into Thoth"""

from scieloloader import SciELOBookLoader


class EDITUSLoader(SciELOBookLoader):
    """EDITUS specific logic to ingest book metadata from JSON into Thoth"""
    publisher_name = "EDITUS"
    publisher_url = "http://www.uesc.br/editora/"
