#!/usr/bin/env python
"""Load Editorial Universidad del Rosario metadata into Thoth"""

from scieloloader import SciELOBookLoader


class RosarioLoader(SciELOBookLoader):
    """Rosario specific logic to ingest book metadata from JSON into Thoth"""
    publisher_name = "Editorial Universidad del Rosario"
    publisher_url = "https://editorial.urosario.edu.co/"
