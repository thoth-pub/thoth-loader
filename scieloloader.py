#!/usr/bin/env python
"""Load SciELO metadata into Thoth"""

import json
import logging
from bookloader import BookLoader
from thothlibrary import ThothError


class SciELOLoader(BookLoader):
    """SciELO specific logic to ingest metadata from JSON into Thoth"""
    import_format = "JSON"
    single_imprint = True
    cache_institutions = False

    def run(self):
        """Process JSON and call Thoth to insert its data"""
        for record in self.data:
            work = self.get_work(record, self.imprint_id)
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
                    except Exception as e:
                        logging.error(f"Failed to update work with id {work_id}, exception: {e}")
                        return
            # if work isn't found, create it
            except (IndexError, AttributeError, ThothError):
                work_id = self.thoth.create_work(work)
                logging.info(f"created workId: {work_id}")
            work = self.thoth.work_by_id(work_id)
            self.create_publications(record, work, work_id)
            self.create_contributors(record, work, work_id)
            self.create_languages(record, work, work_id)
            self.create_subjects(record, work, work_id)

    def get_work(self, record, imprint_id):
        """Returns a dictionary with all attributes of a 'work'

        record: current JSON record

        imprint_id: previously obtained ID of this work's imprint
        """
        title = self.split_title(record["title"])
        doi = self.sanitise_doi(record["doi_number"])
        publication_date = self.sanitise_date(record["year"])
        publication_place = None
        if record["city"] and record["country"]:
            publication_place = record["city"] + ", " + record["country"]
        elif record["city"] and not record["country"]:
            publication_place = record["city"]
        elif not record["city"] and record["country"]:
            publication_place = record["country"]
        work_type = None
        # create workType based on creator role
        for creator in record["creators"]:
            # if any creator is an "organizer" in JSON, workType is EDITED_BOOK
            if creator[0][1] == "organizer":
                work_type = "EDITED_BOOK"
                break
            else:
                work_type = "MONOGRAPH"

        work = {
            "workType": work_type,
            "workStatus": "ACTIVE",
            "fullTitle": title["fullTitle"],
            "title": title["title"],
            "subtitle": title["subtitle"],
            "reference": record["_id"],
            "edition": record["edition"][0] if record["edition"] and record["edition"][0].isdigit() else 1,
            "imprintId": imprint_id,
            "doi": doi,
            "publicationDate": publication_date,
            "place": publication_place,
            "pageCount": int(record["pages"]),
            "pageBreakdown": None,
            "imageCount": None,
            "tableCount": None,
            "audioCount": None,
            "videoCount": None,
            "license": record["use_licence"],
            "copyrightHolder": None,
            "landingPage": record["books_url"],
            "lccn": None,
            "oclc": None,
            "shortAbstract": None,
            "longAbstract": record["synopsis"],
            "generalNote": None,
            "bibliographyNote": None,
            "toc": None,
            "coverUrl": record["cover_url"],
            "coverCaption": None,
            "firstPage": None,
            "lastPage": None,
            "pageInterval": None,
        }
        return work

    def create_publications(self, record, work, work_id):
        """Creates PDF, EPUB, and paperback publications associated with the current work

        record: current JSON record

        work_id: previously obtained ID of the current work
        """
        books_url = record["books_url"]
        pdf_url = record["pdf_url"]
        epub_url = record["epub_url"]
        eisbn = self.sanitise_isbn(record["eisbn"])
        isbn = self.sanitise_isbn(record["isbn"])
        publications = [["PDF", None, books_url, pdf_url], ["EPUB", eisbn, books_url, epub_url], ["PAPERBACK", isbn, books_url, None]]
        for publication_type, isbn, landing_page, full_text in publications:
            publication = {
                "workId": work_id,
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
                logging.info(f"existing publication: {publication_id}")
            else:
                publication_id = self.thoth.create_publication(publication)
                logging.info(f"created publication: {publication_id}")
            if existing_pub and any(l.locationPlatform == "SCIELO_BOOKS" for l in existing_pub.locations):
                logging.info("existing location")
                continue
            location = {
                "publicationId": publication_id,
                "landingPage": landing_page,
                "fullTextUrl": full_text,
                "locationPlatform": "SCIELO_BOOKS",
                "canonical": "true",
            }
            logging.info(f"created location: with publicationId {publication_id}")
            self.thoth.create_location(location)

    def create_contributors(self, record, work, work_id):
        """Creates/updates all contributors associated with the current work and their contributions

        record: current JSON record

        work_id: previously obtained ID of the current work
        """
        highest_contribution_ordinal = max((c.contributionOrdinal for c in work.contributions), default=0)
        for creator in record["creators"]:
            full_name_inverted = creator[1][1].split(',')
            name = full_name_inverted[1].strip()
            surname = full_name_inverted[0]
            fullname = f"{name} {surname}"
            contribution_type = self.contribution_types[creator[0][1]]
            orcid_id = None
            website = None
            profile_link = creator[2][1]
            # profile_link (link_resume in JSON) may contain either an ORCID ID or a website
            # assign value to orcid_id or website accordingly
            if profile_link:
                orcid = self.orcid_regex.search(profile_link)
                if orcid:
                    orcid_id = profile_link
                    website = None
                else:
                    orcid_id = None
                    website = profile_link
            contributor = {
                "firstName": name,
                "lastName": surname,
                "fullName": fullname,
                "orcid": orcid_id,
                "website": website,
            }
            if fullname not in self.all_contributors:
                contributor_id = self.thoth.create_contributor(contributor)
                logging.info(f"created contributor: {contributor_id}")
                self.all_contributors[fullname] = contributor_id
            else:
                contributor_id = self.all_contributors[contributor["fullName"]]
                self.update_scielo_contributor(contributor, contributor_id)
            existing_contribution = next((c for c in work.contributions if c.contributor.contributorId == contributor_id), None)
            if not existing_contribution:
                contribution = {
                    "workId": work_id,
                    "contributorId": contributor_id,
                    "contributionType": contribution_type,
                    "mainContribution": "true",
                    "contributionOrdinal": highest_contribution_ordinal + 1,
                    "biography": None,
                    "firstName": name,
                    "lastName": surname,
                    "fullName": fullname,
                }
                self.thoth.create_contribution(contribution)
                logging.info(f"created contribution with contributorId: {contributor_id}")
                highest_contribution_ordinal += 1
            else:
                logging.info(f"existing contribution with contributorId: {contributor_id}")

    def update_scielo_contributor(self, contributor, contributor_id):
        # find existing contributor in Thoth
        contributor_record = self.thoth.contributor(contributor_id, True)
        contributor_json_from_thoth = json.loads(contributor_record)
        thoth_first_name = contributor_json_from_thoth['data']['contributor']['firstName']
        thoth_last_name = contributor_json_from_thoth['data']['contributor']['lastName']
        thoth_full_name = contributor_json_from_thoth['data']['contributor']['fullName']
        thoth_orcid = contributor_json_from_thoth['data']['contributor']['orcid']
        thoth_website = contributor_json_from_thoth['data']['contributor']['website']
        thoth_contributor = {
            "firstName": thoth_first_name,
            "lastName": thoth_last_name,
            "fullName": thoth_full_name,
            "orcid": thoth_orcid,
            "website": thoth_website,
            "contributorId": contributor_id,
        }
        # add contributorId to contributor dictionary so it can be compared to thoth_contributor
        contributor["contributorId"] = contributor_id
        if contributor != thoth_contributor:
            combined_contributor = {}
            # some contributors may have contributed to multiple books and be in the JSON multiple times
            # with profile_link containing different values. Combine the dictionaries and keep the value that is not None.
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
            logging.info(f"existing contributor: {contributor_id}")
        return contributor

    def create_languages(self, record, work, work_id):
        """Creates language associated with the current work

        record: current JSON record

        work_id: previously obtained ID of the current work
        """
        language_code = self.language_codes[record["language"]]

        # check to see if work already has this language
        if any(l.languageCode == language_code for l in work.languages):
            logging.info("existing language")
            return
        language = {
            "workId": work_id,
            "languageCode": language_code,
            "languageRelation": "ORIGINAL",
            "mainLanguage": "true"
        }
        self.thoth.create_language(language)
        logging.info(f"created language for workId: {work_id}")

    def create_subjects(self, record, work, work_id):
        """Creates all subjects associated with the current work

        record: current JSON record

        work_id: previously obtained ID of the current work
        """
        bisac_subject_code = record["bisac_code"][0][0][1]
        keyword_subject_codes = record["primary_descriptor"].split("; ")

        def create_subject(subject_type, subject_code, subject_ordinal):
            subject = {
                "workId": work_id,
                "subjectType": subject_type,
                "subjectCode": subject_code,
                "subjectOrdinal": subject_ordinal
            }
            self.thoth.create_subject(subject)

        # check if the work already has a subject with the BISAC subject code
        if not any(s.subjectCode == bisac_subject_code and s.subjectType == "BISAC" for s in work.subjects):
            logging.info("New BISAC subject")
            create_subject("BISAC", bisac_subject_code, 1)
        else:
            logging.info("Existing BISAC subject")

        for subject_ordinal, keyword in enumerate(keyword_subject_codes, start=1):
            # check if the work already has a subject with the keyword subject type/subject code combination
            if not any(s.subjectCode == keyword and s.subjectType == "KEYWORD" for s in work.subjects):
                logging.info("New keyword subject")
                create_subject("KEYWORD", keyword, subject_ordinal)
            else:
                logging.info("Existing keyword subject")




