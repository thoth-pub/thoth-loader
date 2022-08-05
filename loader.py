#!/usr/bin/env python
"""Metadata loader

Call custom, business specific, workflows to ingest metdata into Thoth.
"""

import argparse
import logging
from obploader import OBPBookLoader
from punctumloader import PunctumBookLoader
from punctumchapterloader import PunctumChapterLoader
from africanmindsloader import AfricanMindsBookLoader

LOADERS = {
    "OBP": OBPBookLoader,
    "punctum": PunctumBookLoader,
    "punctum-chapters": PunctumChapterLoader,
    "AM": AfricanMindsBookLoader,
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
