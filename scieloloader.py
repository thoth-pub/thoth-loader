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
    # TODO: when ingesting from other imprints (e.g. EDUFBA), change name.
    publisher_name = "EDITUS"
    publisher_shortname = None
    # TODO: when ingesting other publishers, change URL.
    publisher_url = "http://www.uesc.br/editora/"
    cache_institutions = False

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
            # self.create_publications(record, work_id)
            self.create_contributors(record, work_id)
            # self.create_languages(record, work_id)
            # self.create_subjects(record, work_id)
            # can't ingest series data: SciELO series don't include ISSN, which is a required field in Thoth

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
            # if any creator is an organizer, workType is EDITED_BOOK
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
                contributor_id = self.all_contributors[fullname]
                contributor_record = self.thoth.contributor(contributor_id, True)
                contributor_json = json.loads(contributor_record)
                logging.info("Existing contributor record:")
                logging.info(contributor_record)
                # logging.info("Existing contributor parsed JSON:")
                # logging.info(contributor_json['data']['contributor']['firstName'])
                json_first_name = contributor_json['data']['contributor']['firstName']
                json_last_name = contributor_json['data']['contributor']['lastName']
                json_full_name = contributor_json['data']['contributor']['fullName']
                json_orcid = contributor_json['data']['contributor']['orcid']
                json_contributor = {
                    "firstName": json_first_name,
                    "lastName": json_last_name,
                    "fullName": json_full_name,
                    "orcid": json_orcid,
                    "contributorId": contributor_id,
                }

                # logging.info("JSON contributor dict:")
                # logging.info(json_contributor)

                contributor = {
                    "firstName": name,
                    "lastName": surname,
                    "fullName": fullname,
                    "orcid": orcid_id,
                    "website": website,
                    "contributorId": contributor_id,
                }
                # logging.info("Existing contributor dict:")
                # logging.info(contributor)
                # TODO: It'd be good to get the existing contributor from Thoth and only update if anything's changed
                # self.thoth.update_contributor(contributor)

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
            # self.thoth.create_contribution(contribution)

    def create_languages(self, record, work_id):
        """Creates language associated with the current work

        record: current JSON record

        work_id: previously obtained ID of the current work
        """
        scielo_languages = {
        "pt": "POR",
        "en": "ENG",
        "es": "SPA",
        }
        languageCode = scielo_languages[record["language"]]
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
        def create_BISAC_subject():
            subject = {
                "workId": work_id,
                "subjectType": "BISAC",
                "subjectCode": BISAC_subject_code,
                "subjectOrdinal": 1
            }
            self.thoth.create_subject(subject)
        create_BISAC_subject()

        def create_keyword_subjects():
            subject_ordinal = 0
            for keyword in keyword_subject_codes:
                subject_ordinal += 1
                subject = {
                    "workId": work_id,
                    "subjectType": "KEYWORD",
                    "subjectCode": keyword,
                    "subjectOrdinal": subject_ordinal
                }
                self.thoth.create_subject(subject)
        create_keyword_subjects()

    # TODO: problem with create_series: SciELO series don't include ISSN, which is a required field in Thoth.
    # so this function doesn't currently work
    def create_series(self, record, imprint_id, work_id):
        """Creates series associated with the current work

        record: current JSON record

        work_id: previously obtained ID of the current work
        """
        series_name = record["serie"][0][1]
        issue_ordinal = int(record["serie"][2][1]) if record["serie"][2][1] else None
        issn_digital = record["serie"][3][1]
        series_type = "BOOK_SERIES"
        collection_title = record["collection"][2][1]
        if series_name:
            series = {
                "imprintId": imprint_id,
                "seriesType": series_type,
                "seriesName": series_name,
                "issnPrint": None,
                "issnDigital": issn_digital,
                "seriesUrl": None,
                "seriesDescription": None,
                "seriesCfpUrl": None
            }
            logging.info(series)
        elif collection_title:
            series = {
                "imprintId": imprint_id,
                "seriesType": series_type,
                "seriesName": collection_title,
                "issnPrint": None,
                "issnDigital": None,
                "seriesUrl": None,
                "seriesDescription": None,
                "seriesCfpUrl": None
            }
            logging.info(series)
        if series:
            if series["seriesName"] not in self.all_series:
                series_id = self.thoth.create_series(series)
                self.all_series[series_name] = series_id
            else:
                series_id = self.all_series[series_name]
            issue = {
                "seriesId": series_id,
                "workId": work_id,
                "issueOrdinal": int(issue_ordinal) if issue_ordinal else None
                }
            logging.info(issue)
            self.thoth.create_issue(issue)