#!/usr/bin/env python
"""Load EDITUS metadata into Thoth"""

from scieloloader import SciELOLoader

class EDITUSLoader(SciELOLoader):
    """EDITUS specific logic to ingest metadata from JSON into Thoth"""
    publisher_name = "EDITUS"
    publisher_url = "http://www.uesc.br/editora/"

