#!/usr/bin/env python
"""Load EDITUS metadata into Thoth"""

from scieloloader import SciELOChapterLoader


class EDITUSChapterLoader(SciELOChapterLoader):
    """EDITUS specific logic to ingest chapter metadata from JSON into Thoth"""
    publisher_name = "EDITUS"
    publisher_url = "http://www.uesc.br/editora/"
