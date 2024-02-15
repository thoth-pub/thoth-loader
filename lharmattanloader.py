#!/usr/bin/env python
"""Load L'Harmattan OA book metadata into Thoth"""

import logging
import sys
import json
import requests
from bookloader import BookLoader
from thothlibrary import ThothError


class LHarmattanLoader(BookLoader):
    """L'Harmattan specific logic to ingest metadata from CSV into Thoth"""
    single_imprint = True
    cache_institutions = False
    cache_series = True
    publisher_name = "L'Harmattan Open Access"
    publisher_shortname = "L'Harmattan"
    publisher_url = "https://openaccess.hu"
    # set appropriate separator for CSV; default is comma
    separation = ";"

    def run(self):
        """Process CSV and call Thoth to insert its data"""
        for index, row in self.data.iterrows():
            logging.info("\n\n\n\n**********")
            logging.info(f"processing book {index + 1}: {row['title']}")
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
                        logging.info(f"workId for updated work: {work_id}")
                    # if update fails, log the error and exit the import
                    except ThothError as t:
                        logging.error(f"Failed to update work with id {work_id}, exception: {t}")
                        sys.exit(1)
            # if work isn't found, create it
            except (IndexError, AttributeError, ThothError):
                work_id = self.thoth.create_work(work)
                logging.info(f"created work with workId: {work_id}")
            work = self.thoth.work_by_id(work_id)
            self.create_contributors(row, work)
            self.create_publications(row, work)
            self.create_languages(row, work)
            self.create_series(row, work)
            self.create_subjects(row, work)

    def get_work(self, row, imprint_id):
        """Returns a dictionary with all attributes of a 'work'

        row: current row number

        imprint_id: previously obtained ID of this work's imprint
        """
        reference = row["uid"]
        doi = self.sanitise_doi(row["scs023_doi"])
        # resolve DOI to obtain landing page
        response = requests.get(row["scs023_doi"])
        landing_page = response.url
        work_type = row["taxonomy_EN"]
        if work_type in self.work_types:
            work_type = self.work_types[row["taxonomy_EN"]]
        # CSV work types "text edition", "academic notes", "literary translation" currently assigned to Monograph
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
            "landingPage": landing_page,
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
        authors = row["scs023_author"] if row["scs023_author"] else None
        translators = row["scs023_translator"] if row["scs023_translator"] else None
        contributors = row["contributor"] if row["contributor"] else None
        editors = row["scs023_editor"] if row["scs023_editor"] else None
        orcid = self.sanitise_orcid(row["scs023_orcid"]) if row["scs023_orcid"] else None
        website = row["scs023_web"] if row["scs023_web"] else None

        all_creators = [
            [authors, "AUTHOR"], [translators, "TRANSLATOR"], [contributors, "CONTRIBUTIONS_BY"], [editors, "EDITOR"]
        ]
        highest_contribution_ordinal = max((c.contributionOrdinal for c in work.contributions), default=0)
        creator_category_count = 0
        individual_creator_count = 0
        for creators, contribution_type in all_creators:
            if creators:
                creator_category_count += 1
                # names are separated by pipes
                creators_array = creators.split("|")
                for creator in creators_array:
                    individual_creator_count += 1
                    # sanitise names for correct Thoth formatting
                    split_creator = creator.split()
                    # case for names with middle initial, e.g. "K. Németh András"
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
                    existing_contribution = next(
                        (c for c in work.contributions if c.contributor.contributorId == contributor_id),
                        None)
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
                        logging.info(f"created contribution for {full_name}, type: {contribution_type}")
                        highest_contribution_ordinal += 1
                    else:
                        logging.info(f"existing contribution for {full_name}, type: {contribution_type}")
        # CSV may contain ORCID and/or website that corresponds to a creator, but there's no way to tell which
        # when there are multiple creators
        # if only one creator in CSV, add orcid and website to them, else don't add
        if creator_category_count == 1 and individual_creator_count == 1:
            logging.info(f"{full_name} is the only contributor for {work.title}, adding ORCID and website")
            contributor["orcid"] = orcid
            contributor["website"] = website
            self.update_lharmattan_contributor(contributor, contributor_id)

    def update_lharmattan_contributor(self, contributor, contributor_id):
        # find existing contributor in Thoth
        contributor_record = self.thoth.contributor(contributor_id, True)
        thoth_contributor = json.loads(contributor_record)['data']['contributor']
        # remove unnecesary fields for comparison to contributor
        del thoth_contributor['__typename']
        del thoth_contributor['contributions']
        # add contributorId to contributor dictionary so it can be compared to thoth_contributor
        contributor["contributorId"] = contributor_id
        if contributor != thoth_contributor:
            combined_contributor = {}
            # some contributors may have contributed to multiple books and be in the JSON multiple times
            # with profile_link containing different values.
            # Combine the dictionaries and keep the value that is not None.
            for key in set(thoth_contributor) | set(contributor):
                if contributor[key] is not None:
                    combined_contributor[key] = contributor[key]
                else:
                    combined_contributor[key] = thoth_contributor[key]
            # combined contributor now contains the values from both dictionaries
            # however, if all of these values are already in Thoth, there's no need to update
            # so only update if combined_contributor is different from thoth_contributor
            if combined_contributor != thoth_contributor:
                self.thoth.update_contributor(combined_contributor)
                logging.info(f"updated contributor: {contributor_id}")
        else:
            logging.info(f"existing contributor, no changes needed to Thoth: {contributor_id}")
        return contributor

    def create_publications(self, row, work):
        """Creates PDF and paperback publications associated with the current work

        row: current CSV record

        work: Work from Thoth
        """
        isbn = self.sanitise_isbn(row["scs023_isbn"].strip())
        print_landing_page = row["scs023_printed_version"]
        pdf_landing_page = row["scs023_doi"]

        publications = [
            ["PDF", None, pdf_landing_page],
            ["PAPERBACK", isbn, print_landing_page]]

        for publication_type, isbn, landing_page in publications:
            publication = {
                "workId": work.workId,
                "publicationType": publication_type,
                "isbn": isbn,
                "widthMm": None,
                "widthIn": None,
                "heightMm": None,
                "heightIn": None,
                "depthMm": None,
                "depthIn": None,
                "weightG": None,
                "weightOz": None,
            }

            existing_pub = next((p for p in work.publications if p.publicationType == publication_type), None)
            if existing_pub:
                publication_id = existing_pub.publicationId
                logging.info(f"existing {publication_type} publication: {publication_id}")
            else:
                publication_id = self.thoth.create_publication(publication)
                logging.info(f"created {publication_type} publication: {publication_id}")
            # TODO: change locationPlatform once added to Thoth
            if existing_pub and any(location.locationPlatform == "OTHER" for location in existing_pub.locations):
                logging.info("existing location")
                continue
            location = {
                "publicationId": publication_id,
                "landingPage": landing_page,
                "fullTextUrl": landing_page,  # TODO: temp fix, change when fullTextUrl is available
                "locationPlatform": "OTHER",  # TODO: change locationPlatform once added to Thoth
                "canonical": "true",
            }
            self.thoth.create_location(location)
            logging.info(f"created location: with publicationId {publication_id}")

    def create_languages(self, row, work):
        """Creates language associated with the current work

        row: current CSV record

        work: Work from Thoth
        """
        csv_language_codes = row["language_ISO"].split("|")
        for csv_language in csv_language_codes:
            language_code = csv_language.upper()
            # CSV contains "fra" for French instead of "fre"
            if language_code == "FRA":
                language_code = "FRE"
            # check to see if work already has this language
            if any(language.languageCode == language_code for language in work.languages):
                logging.info("existing language")
                return
            language = {
                "workId": work.workId,
                "languageCode": language_code,
                "languageRelation": "ORIGINAL",
                "mainLanguage": "true"
            }
            self.thoth.create_language(language)
            logging.info(f"created language {language_code} for workId: {work.workId}")

    def create_series(self, row, work):
        """Creates series associated with the current work

        row: current CSV row

        work: current work
        """
        series_name = row["scs023_series"]
        series_issn = row["scs023_issn"]

        if not series_issn or not series_name:
            logging.info(f"{work.fullTitle} missing series metadata (ISSN and/or name); skipping create_series")
            return
        if series_name not in self.all_series:
            try:
                issn = self.sanitise_issn(series_issn)
            except ValueError as e:
                logging.error(f"{e} ({work.workId})")
            if not issn:
                logging.info(f"{work.fullTitle} does not have properly formed ISSN; skipping")
                return
            series = {
                "seriesType": "BOOK_SERIES",
                "seriesName": series_name,
                "issnDigital": issn,
                "issnPrint": issn,
                "seriesUrl": None,
                "seriesDescription": None,
                "seriesCfpUrl": None,
                "imprintId": self.imprint_id
            }
            series_id = self.thoth.create_series(series)
            logging.info(f"new series created: {series['seriesName']}")
            self.all_series[series_name] = series_id
        else:
            logging.info(f"existing series {series_name}")
            series_id = self.all_series[series_name]

        if not work.issues:
            # find all existing issues in Series
            current_series = self.thoth.series(series_id)
            # count them
            number_of_issues = len(current_series.issues)
            # assign next highest issueOrdinal
            issue = {
                "seriesId": series_id,
                "workId": work.workId,
                "issueOrdinal": number_of_issues + 1,
            }
            self.thoth.create_issue(issue)
            logging.info("Created new issue for work")
        else:
            logging.info("Work already has associated issue")

    def create_subjects(self, row, work):
        """Creates all subjects associated with the current work

        row: current row in CSV

        work: Work from Thoth
        """
        keyword_subjects = row["scs023_keywords"].split("|")
        # correctly parse "scs023_field_science" into keywords and add them to keyword_subjects
        # example field value:
        # "Társadalom és gazdaságtörténet / Social and economic history (12979)|
        # Újkori és jelenkori történelem / Modern and contemporary history (12977)"
        fields_science = row["scs023_field_science"].split("|")
        for field in fields_science:
            hungarian_field, second_part = field.split(" / ")
            english_field = second_part.rsplit(" ", 1)[0]
            keyword_subjects.append(hungarian_field)
            keyword_subjects.append(english_field)

        def create_subject(subject_type, subject_code, subject_ordinal):
            subject = {
                "workId": work.workId,
                "subjectType": subject_type,
                "subjectCode": subject_code,
                "subjectOrdinal": subject_ordinal
            }
            self.thoth.create_subject(subject)

        for subject_ordinal, keyword in enumerate(keyword_subjects, start=1):
            # check if the work already has a subject with the keyword subject type/subject code combination
            if not any(
                subject.subjectCode == keyword and subject.subjectType == "KEYWORD" for subject in work.subjects
            ):
                create_subject("KEYWORD", keyword, subject_ordinal)
                logging.info(f"New keyword {keyword} added as Subject")
            else:
                logging.info(f"Existing keyword {keyword} associated with Work")


