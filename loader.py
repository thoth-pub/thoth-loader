#!/usr/bin/env python
"""Metadata loader

Call custom, business specific, workflows to ingest metadata into Thoth.
"""

import argparse
import logging
from obploader import OBPBookLoader
from obpchapterloader import ObpChapterLoader
from obpchapterabstractloader import ObpChapterAbstractLoader
from punctumloader import PunctumBookLoader
from punctumchapterloader import PunctumChapterLoader
from africanmindsloader import AfricanMindsBookLoader
from whploader import WHPLoader
from whpchapterloader import WHPChapterLoader
from uwploader import UWPLoader
from lseloader import LSELoader
from editusloader import EDITUSLoader
from scieloloader import SciELOLoader
from ubiquityloader import UbiquityPressesLoader

LOADERS = {
    "OBP": OBPBookLoader,
    "OBP-chapters": ObpChapterLoader,
    "OBP-chapter-abstracts": ObpChapterAbstractLoader,
    "punctum": PunctumBookLoader,
    "punctum-chapters": PunctumChapterLoader,
    "AM": AfricanMindsBookLoader,
    "WHP": WHPLoader,
    "WHP-chapters": WHPChapterLoader,
    "UWP": UWPLoader,
    "LSE": LSELoader,
    "EDITUS": EDITUSLoader,
    "SciELO": SciELOLoader,
    "Ubiquity-presses": UbiquityPressesLoader,
}

ARGS = [
    {
        "val": "--file",
        "dest": "file",
        "action": "store",
        "default": "./data/all-book-metadata.csv",
        "help": "Path to metadata CSV file"
    }, {
        "val": "--client-url",
        "dest": "client_url",
        "action": "store",
        "default": "https://api.thoth.pub",
        "help": "Thoth's GraphQL endpoint URL, excluding '/graphql'"
    }, {
        "val": "--email",
        "dest": "email",
        "action": "store",
        "help": "Authentication email address"
    }, {
        "val": "--password",
        "dest": "password",
        "action": "store",
        "help": "Authentication password"
    }, {
        "val": "--mode",
        "dest": "mode",
        "action": "store",
        "default": "OBP",
        "help": "Publisher key, one of: {}".format(
            ', '.join("%s" % (key) for (key, val) in LOADERS.items()))
    }
]


def run(mode, metadata_file, client_url, email, password):
    """Execute a book loader based on input parameters"""
    loader = LOADERS[mode](metadata_file, client_url, email, password)
    loader.run()


def get_arguments():
    """Parse input arguments using ARGS"""
    parser = argparse.ArgumentParser()
    for arg in ARGS:
        if 'default' in arg:
            parser.add_argument(arg["val"], dest=arg["dest"],
                                default=arg["default"], action=arg["action"],
                                help=arg["help"])
        else:
            parser.add_argument(arg["val"], dest=arg["dest"], required=True,
                                action=arg["action"], help=arg["help"])
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(levelname)s:%(asctime)s: %(message)s')
    ARGUMENTS = get_arguments()
    run(ARGUMENTS.mode, ARGUMENTS.file, ARGUMENTS.client_url,
        ARGUMENTS.email, ARGUMENTS.password)
