#!/usr/bin/env python
"""Load L'Harmattan OA metadata into Thoth"""

import logging
import sys
from bookloader import BookLoader
from thothlibrary import ThothError

class LHarmattanLoader(BookLoader):
    """L'Harmattan specific logic to ingest metadata from CSV into Thoth"""
    single_imprint = True
    cache_institutions = False
    publisher_name = "L'Harmattan Open Access"
    publisher_shortname = "L'Harmattan"
    publisher_url = "https://openaccess.hu"
    separation = ";"

    def run(self):
        """Process CSV and call Thoth to insert its data"""
        for index, row in self.data.iterrows():
            logging.info("\n\n\n\n**********")
            logging.info(f"processing book: {row['title']}")
            work = self.get_work(row, self.imprint_id)
            # try to find the work in Thoth
            try:
                work_id = self.thoth.work_by_doi(work['doi']).workId
                existing_work = self.thoth.work_by_id(work_id)
                # if work is found, try to update it with the new data
                if existing_work:
                    try:
                        existing_work.update((k, v) for k, v in work.items() if v is not None)
                        self.thoth.update_work(existing_work)
                        logging.info(f"updated workId: {work_id}")
                    # if update fails, log the error and exit the import
                    except ThothError as t:
                        logging.error(f"Failed to update work with id {work_id}, exception: {t}")
                        sys.exit(1)
            # if work isn't found, create it
            except (IndexError, AttributeError, ThothError):
                work_id = self.thoth.create_work(work)
                logging.info(f"created workId: {work_id}")
            work = self.thoth.work_by_id(work_id)
            self.create_contributors(row, work)


    def get_work(self, row, imprint_id):
        """Returns a dictionary with all attributes of a 'work'

        row: current row number

        imprint_id: previously obtained ID of this work's imprint
        """

        reference = row["uid"]
        doi = self.sanitise_doi(row["scs023_doi"])
        # TODO: waiting for Szilvia response to map L'Harmattan taxonomy types
        # "text edition", "academic notes", "literary translation",
        # to allowed work types in Thoth: Monograph,
        # Edited Book, Textbook, Book Set
        # temporary workaround below
        work_type = row["taxonomy_EN"]
        if work_type in self.work_types:
            work_type = self.work_types[row["taxonomy_EN"]]
        else:
            work_type = "MONOGRAPH"
        title = self.split_title(row["title"].strip())
        # date only available as year; add date to Thoth as 01-01-YYYY
        date = self.sanitise_date(row["date"])
        place = (row["scs023_place"]).replace("|", "; ")
        long_abstract = row["scs023_summary"]
        editions_text = {
        "First edition": 1,
        "Second edition": 2,
        }
        edition = row["edition-info_EN"]
        if edition in editions_text:
            edition = editions_text[edition]
        else:
            edition = 1
        license = "https://creativecommons.org/licenses/by-nc-nd/4.0/"

        work = {
            "workType": work_type,
            "workStatus": "ACTIVE",
            "fullTitle": title["fullTitle"],
            "title": title["title"],
            "subtitle": title["subtitle"],
            "reference": reference,
            "edition": edition,
            "imprintId": imprint_id,
            "doi": doi,
            "publicationDate": date,
            "place": place,
            "pageCount": None,
            "pageBreakdown": None,
            "imageCount": None,
            "tableCount": None,
            "audioCount": None,
            "videoCount": None,
            "license": license,
            "copyrightHolder": None,
            "landingPage": doi,
            "lccn": None,
            "oclc": None,
            "shortAbstract": None,
            "longAbstract": long_abstract,
            "generalNote": None,
            "bibliographyNote": None,
            "toc": None,
            "coverUrl": None,
            "coverCaption": None,
            "firstPage": None,
            "lastPage": None,
            "pageInterval": None,
        }
        return work

    def create_contributors(self, row, work):
        """Creates/updates all contributors associated with the current work and their contributions

        row: current CSV row

        work: Work from Thoth
        """

        # we have authors, translators, contributors, and editors
        # read data from each relevant field
        authors = row["scs023_author"] if row["scs023_author"] else None
        translators = row["scs023_translator"] if row["scs023_translator"] else None
        contributors = row["contributor"] if row["contributor"] else None
        editors = row["scs023_editor"] if row["scs023_editor"] else None
        orcid = row["scs023_orcid"] if row["scs023_orcid"] else None
        website = row["scs023_web"] if row["scs023_web"] else None

        all_creators = [[authors, "AUTHOR"], [translators, "TRANSLATOR"], [contributors, "CONTRIBUTIONS_BY"], [editors, "EDITOR"]]


        highest_contribution_ordinal = max((c.contributionOrdinal for c in work.contributions), default=0)
        creator_category_count = 0
        individual_creator_count = 0
        for creators, contribution_type in all_creators:
            if creators:
                creator_category_count += 1
                # names are separated by pipes, so use the pipe as a separator to put each set of names in an array
                creators_array = creators.split("|")
                # iterate over the elements of the array
                for creator in creators_array:
                    individual_creator_count += 1
                    # sanitise names for Thoth formatting
                    split_creator = creator.split()
                    # names with middle initial, e.g. "K. Németh András"
                    if len(split_creator) == 3:
                        name = " ".join([split_creator[2], split_creator[0]])
                        surname = split_creator[1]
                        full_name = " ".join([split_creator[2], split_creator[0], split_creator[1]])
                    elif len(split_creator) == 2:
                        name = split_creator[1]
                        surname = split_creator[0]
                        full_name = " ".join(reversed(split_creator))
                    else:
                        name = surname = full_name = creator
                    contributor = {
                        "firstName": name,
                        "lastName": surname,
                        "fullName": full_name,
                        "orcid": None,
                        "website": None,
                    }
                    if full_name not in self.all_contributors:
                        # if not in Thoth, create a new contributor
                        contributor_id = self.thoth.create_contributor(contributor)
                        logging.info(f"created contributor: {full_name}, {contributor_id}")
                        # cache new contributor
                        self.all_contributors[full_name] = contributor_id
                    else:
                        contributor_id = self.all_contributors[full_name]
                        logging.info(f"contributor {full_name} already in Thoth, skipping")
                    existing_contribution = next((c for c in work.contributions if c.contributor.contributorId == contributor_id), None)
                    if not existing_contribution:
                        contribution = {
                            "workId": work.workId,
                            "contributorId": contributor_id,
                            "contributionType": contribution_type,
                            "mainContribution": "true",
                            "contributionOrdinal": highest_contribution_ordinal + 1,
                            "biography": None,
                            "firstName": name,
                            "lastName": surname,
                            "fullName": full_name,
                        }
                        self.thoth.create_contribution(contribution)
                        logging.info(f"created contribution for {full_name}, type: {contribution_type} with contributorId: {contributor_id}")
                        highest_contribution_ordinal += 1
                    else:
                        logging.info(f"existing contribution for {full_name}, type: {contribution_type} with contributorId: {contributor_id}")
        if creator_category_count == 1 and individual_creator_count == 1:
            logging.info(f"{full_name} is the only contributor for {work.title}")









        # if only one creator, add orcid and website to them, else ignore
        # add Contributor to Thoth attached by workId


