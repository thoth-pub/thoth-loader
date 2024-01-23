#!/usr/bin/env python
"""Load SciELO metadata into Thoth"""

import json
import logging
from bookloader import BookLoader
from thothlibrary import ThothError


class SciELOLoader(BookLoader):
    """SciELO specific logic to ingest metadata from JSON into Thoth"""

    def run(self):
        """Process JSON and call Thoth to insert its data"""
        for record in self.data:
            work = self.get_work(record, self.imprint_id)
            try:
                work_id = self.thoth.work_by_doi(work['doi']).workId
                existing_work = self.thoth.work_by_id(work_id)
                existing_work.update((k, v) for k, v in work.items() if v is not None)
                self.thoth.update_work(existing_work)
            except (IndexError, AttributeError, ThothError):
                work_id = self.thoth.create_work(work)
            logging.info('workId: %s' % work_id)
            self.create_publications(record, work_id)
            self.create_contributors(record, work_id)
            self.create_languages(record, work_id)
            self.create_subjects(record, work_id)

    def get_work(self, record, imprint_id):
        """Returns a dictionary with all attributes of a 'work'

        record: current JSON record

        imprint_id: previously obtained ID of this work's imprint
        """
        title = self.split_title(record["title"])
        publication_date = self.sanitise_date(record["year"])
        if record["city"] and record["country"]:
            publication_place = record["city"] + ", " + record["country"]
        elif record["city"] and not record["country"]:
            publication_place = record["city"]
        elif not record["city"] and record["country"]:
            publication_place = record["country"]
        else:
            publication_place = None
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
            "edition": record["edition"][0] if record["edition"] else 1,
            "imprintId": imprint_id,
            "doi": record["doi_number"],
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

    def create_publications(self, record, work_id):
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
            publication_id = self.thoth.create_publication(publication)
            location = {
                "publicationId": publication_id,
                "landingPage": landing_page,
                "fullTextUrl": full_text,
                "locationPlatform": "OTHER",
                "canonical": "true",
            }
            self.thoth.create_location(location)

    def create_contributors(self, record, work_id):
        """Creates/updates all contributors associated with the current work and their contributions

        record: current JSON record

        work_id: previously obtained ID of the current work
        """
        contribution_ordinal = 0
        for creator in record["creators"]:
            full_name_inverted = creator[1][1].split(',')
            name = full_name_inverted[1].strip()
            surname = full_name_inverted[0]
            fullname = f"{name} {surname}"
            contribution_type = self.contribution_types[creator[0][1]]
            profile_link = creator[2][1]
            orcid_id = None
            website = None
            # profile_link (link_resume in JSON) may contain either an ORCID ID or a website
            if profile_link:
                orcid = self.orcid_regex.search(profile_link)
                if orcid:
                    orcid_id = profile_link
                    website = None
                else:
                    orcid_id = None
                    website = profile_link
            contribution_ordinal += 1
            contributor = {
                "firstName": name,
                "lastName": surname,
                "fullName": fullname,
                "orcid": orcid_id,
                "website": website,
            }

            if fullname not in self.all_contributors:
                contributor_id = self.thoth.create_contributor(contributor)
                self.all_contributors[fullname] = contributor_id
            else:
                # find existing contributor in Thoth
                contributor_id = self.all_contributors[fullname]
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
                # create contributor dict from JSON
                json_contributor = {
                    "firstName": name,
                    "lastName": surname,
                    "fullName": fullname,
                    "orcid": orcid_id,
                    "website": website,
                    "contributorId": contributor_id,
                }
                if json_contributor != thoth_contributor:
                    combined_contributor = {}
                    # some contributors may have contributed to multiple books and be in the JSON multiple times
                    # with profile_link containing different values. Combine the dictionaries and keep the value that is not None.
                    for key in set(thoth_contributor) | set(json_contributor):
                        if json_contributor[key] is not None:
                            combined_contributor[key] = json_contributor[key]
                        else:
                            combined_contributor[key] = thoth_contributor[key]
                    # logging.info(combined_contributor)
                    self.thoth.update_contributor(combined_contributor)

            contribution = {
                "workId": work_id,
                "contributorId": contributor_id,
                "contributionType": contribution_type,
                "mainContribution": "true",
                "contributionOrdinal": contribution_ordinal,
                "biography": None,
                "firstName": name,
                "lastName": surname,
                "fullName": fullname,
            }
            # logging.info(contribution)
            self.thoth.create_contribution(contribution)

    def create_languages(self, record, work_id):
        """Creates language associated with the current work

        record: current JSON record

        work_id: previously obtained ID of the current work
        """

        languageCode = self.language_codes[record["language"]]
        language = {
            "workId": work_id,
            "languageCode": languageCode,
            "languageRelation": "ORIGINAL",
            "mainLanguage": "true"
        }
        self.thoth.create_language(language)

    def create_subjects(self, record, work_id):
        """Creates all subjects associated with the current work

        record: current JSON record

        work_id: previously obtained ID of the current work
        """
        BISAC_subject_code = record["bisac_code"][0][0][1]
        keyword_subject_codes = record["primary_descriptor"].split( "; " )

        def create_subject(subject_type, subject_code, subject_ordinal):
            subject = {
                "workId": work_id,
                "subjectType": subject_type,
                "subjectCode": subject_code,
                "subjectOrdinal": subject_ordinal
            }
            self.thoth.create_subject(subject)
            logging.info(subject)

        create_subject("BISAC", BISAC_subject_code, 1)

        for subject_ordinal, keyword in enumerate(keyword_subject_codes, start=1):
            create_subject("KEYWORD", keyword, subject_ordinal)
