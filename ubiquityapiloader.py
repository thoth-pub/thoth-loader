#!/usr/bin/env python
"""Load metadata from Ubiquity API endpoints (XXX.rua.re) into Thoth"""

from bookloader import BookLoader


class UbiquityAPILoader(BookLoader):
    """
    Ubiquity specific logic to ingest metadata from JSON API dump into Thoth
    Currently only ingests works from Radboud and UWP
    TODO Works which already exist in Thoth should be extended/overwritten
    """
    import_format = "JSON"
    cache_series = True

    def run(self):
        """Process JSON and call Thoth to insert its data"""
        for record in self.data:
            self.publisher_name = record["publisher"]["name"]
            if self.publisher_name == "Radboud University Press":
                self.publisher_url = "https://www.radbouduniversitypress.nl/"
                self.set_publisher_and_imprint()
            elif self.publisher_name == "University of Westminster Press":
                self.publisher_url = "https://www.uwestminsterpress.co.uk/"
                self.publisher_shortname = "UWP"
                self.set_publisher_and_imprint()
            else:
                continue
            work = self.get_work(record)
            work_id = self.thoth.create_work(work)
            self.create_contributors(record, work_id)
            self.create_publications(record, work_id)
            self.create_languages(record, work_id)
            self.create_subjects(record, work_id)
            self.create_series(record, work_id)
            thoth_work = self.thoth.work_by_id(work_id)
            self.create_relations(record, thoth_work)

    def get_work(self, record):
        """Returns a dictionary with all attributes of a book 'work'

        record: current JSON record
        """
        doi = self.sanitise_doi(
            [n["value"] for n in record["identifier"] if n["identifier"] == "doi"][0]
        )

        work = {
            "workType": self.work_types[record["book_type"]],
            "workStatus": "ACTIVE",
            "fullTitle": "{}: {}".format(record["title"], record["subtitle"]),
            "title": record["title"],
            "subtitle": record["subtitle"],
            "reference": None,
            "edition": 1,
            "imprintId": self.imprint_id,
            "doi": doi,
            "publicationDate": record["publication_date"],
            "place": record["publisher"]["location"],
            "pageCount": int(record["pages"]),
            "pageBreakdown": None,
            "imageCount": None,
            "tableCount": None,
            "audioCount": None,
            "videoCount": None,
            "license": record["license"]["url"],
            "copyrightHolder": None,
            "landingPage": None,
            "lccn": None,
            "oclc": None,
            "shortAbstract": record["short_description"],
            "longAbstract": record["description"],
            "generalNote": None,
            "bibliographyNote": None,
            "toc": None,
            "coverUrl": record["cover"],
            "coverCaption": None,
            "firstPage": None,
            "lastPage": None,
            "pageInterval": None,
        }
        return work

    def create_contributor(self, contributor, work_id):
        """Creates a contribution associated with the specified work

        contributor: dict extracted from JSON record representing a single contribution

        work_id: ID of the associated work (either the current work or a child of it)
        """
        first_name = contributor["first_name"].strip()
        last_name = contributor["last_name"].strip()
        full_name = contributor["full_name"].strip()
        biography = contributor["biography"].strip()
        orcid = self.sanitise_orcid(contributor["orcid"])
        contribution_type = contributor["contribution_type"]
        ordinal = int(contributor["sequence"])
        institution_name = contributor["institution"]
        department = contributor["department"]
        country = contributor["country"]

        if orcid and orcid in self.all_contributors:
            contributor_id = self.all_contributors[orcid]
        elif full_name in self.all_contributors:
            contributor_id = self.all_contributors[full_name]
        else:
            new_contributor = {
                "firstName": first_name,
                "lastName": last_name,
                "fullName": full_name,
                "orcid": orcid,
                "website": None,
            }
            contributor_id = self.thoth.create_contributor(new_contributor)
            # cache new contributor
            self.all_contributors[full_name] = contributor_id
            if orcid:
                self.all_contributors[orcid] = contributor_id

        new_contribution = {
            "workId": work_id,
            "contributorId": contributor_id,
            "contributionType": contribution_type,
            "mainContribution": "true",
            "contributionOrdinal": ordinal,
            "biography": biography,
            "firstName": first_name,
            "lastName": last_name,
            "fullName": full_name,
        }
        contribution_id = self.thoth.create_contribution(new_contribution)

        if institution_name:
            # retrieve institution or create if it doesn't exist
            if institution_name in self.all_institutions:
                institution_id = self.all_institutions[institution_name]
            else:
                new_institution = {
                    "institutionName": institution_name,
                    "institutionDoi": None,
                    "ror": None,
                    "countryCode": self.country_codes[country],
                }
                institution_id = self.thoth.create_institution(new_institution)
                # cache new institution
                self.all_institutions[institution_name] = institution_id

            new_affiliation = {
                "contributionId": contribution_id,
                "institutionId": institution_id,
                "affiliationOrdinal": 1,
                "position": department,
            }

            self.thoth.create_affiliation(new_affiliation)

    def create_contributors(self, record, work_id):
        """Creates all contributions associated with the current work

        record: current JSON record

        work_id: previously obtained ID of the current work
        """
        for editor in record["editor"]:
            editor.update({"contribution_type": "EDITOR"})

        for author in record["author"]:
            author.update({"contribution_type": "AUTHOR"})

        for index, contributor in enumerate(record["editor"] + record["author"]):
            contributor.update({"sequence": index + 1})
            self.create_contributor(contributor, work_id)

    def create_publication(self, work_id, publication_type, isbn):
        """Creates a publication associated with the specified work
        (either the current work or a child of it)
        """
        publication = {
            "workId": work_id,
            "publicationType": publication_type,
            "isbn": self.sanitise_isbn(isbn),
            "widthMm": None,
            "widthIn": None,
            "heightMm": None,
            "heightIn": None,
            "depthMm": None,
            "depthIn": None,
            "weightG": None,
            "weightOz": None,
        }
        return self.thoth.create_publication(publication)

    def create_publications(self, record, work_id):
        """Creates all publications associated with the current work

        record: current JSON record

        work_id: previously obtained ID of the current work
        """
        for format in record["formats"] + record["physical_formats"]:
            isbn = next((n["value"] for n in record["identifier"] if n["identifier"] == "isbn-13"
                         and n["object_id"] == format["id"]), None)
            publication_type = self.publication_types[format["file_type"]]
            publication_id = self.create_publication(work_id, publication_type, isbn)

            for index, retailer in enumerate([retailer for retailer in record["retailers"] if retailer["type"].lower() == format["file_type"]]):
                canonical = "true" if index == 0 else "false"
                location = {
                    "publicationId": publication_id,
                    "landingPage": retailer["url"],
                    "fullTextUrl": None,
                    "locationPlatform": "OTHER",
                    "canonical": canonical,
                }
                self.thoth.create_location(location)

    def create_languages(self, record, work_id):
        """Creates languages associated with the current work

        record: current JSON record

        work_id: previously obtained ID of the current work
        """
        for lang_dict in record["languages"]:
            language = {
                "workId": work_id,
                "languageCode": self.language_codes[lang_dict["code"]],
                "languageRelation": "ORIGINAL",
                "mainLanguage": "true",
            }
            self.thoth.create_language(language)

    def create_subject(self, subject_type, subject_code, subject_ordinal, work_id):
        """Creates a subject associated with the specified work
        (either the current work or a child of it)
        """
        subject = {
            "workId": work_id,
            "subjectType": subject_type,
            "subjectCode": subject_code,
            "subjectOrdinal": subject_ordinal,
        }
        self.thoth.create_subject(subject)

    def create_subjects(self, record, work_id):
        """Creates subjects associated with the current work

        record: current JSON record

        work_id: previously obtained ID of the current work
        """
        for index, subject in enumerate(record["keywords"]):
            self.create_subject("KEYWORD", subject["name"], index + 1, work_id)

        for index, subject in enumerate(record["subject"]):
            self.create_subject("CUSTOM", subject["name"], index + 1, work_id)

    def create_series(self, record, work_id):
        """Creates series associated with the current work

        record: current JSON record

        work_id: previously obtained ID of the current work
        """
        if not record["series"]:
            return

        series_name = record["series"]["title"]
        series_description = record["series"]["description"]
        issn = record["series"]["issn"] if record["series"]["issn"] != "XXXX-XXXX" else None
        ordinal = record["series_number"]

        new_series = None
        if series_name not in self.all_series:
            new_series = {
                "imprintId": self.imprint_id,
                "seriesType": "BOOK_SERIES",
                "seriesName": series_name,
                "issnPrint": None,
                "issnDigital": issn,
                "seriesUrl": None,
                "seriesDescription": series_description,
                "seriesCfpUrl": None,
            }

            series_id = self.thoth.create_series(new_series)
            # cache newly created series
            self.all_series[series_name] = series_id
        else:
            series_id = self.all_series[series_name]

        if not ordinal:
            if new_series:
                # First issue in newly-created series
                ordinal = 1
            else:
                # Check number of issues in existing series
                series = self.thoth.series(series_id)
                ordinal = max((issue.issueOrdinal for issue in series.issues), default=0) + 1

        issue = {
            "seriesId": series_id,
            "workId": work_id,
            "issueOrdinal": int(ordinal),
        }

        self.thoth.create_issue(issue)

    def create_relations(self, record, relator_work):
        """Creates all relations associated with the current work

        record: current JSON record

        relator_work: current work
        """
        for chapter in record["chapters"]:
            # Create a new child work which inherits from the current work
            related_work = {
                "workType": "BOOK_CHAPTER",
                "workStatus": relator_work.workStatus,
                "fullTitle": chapter["name"],
                "title": chapter["name"],
                "subtitle": None,
                "reference": None,
                "edition": None,
                "imprintId": self.imprint_id,
                "doi": self.sanitise_doi(chapter["doi"]),
                "publicationDate": relator_work.publicationDate,
                "place": relator_work.place,
                "pageCount": None,
                "pageBreakdown": None,
                "firstPage": None,
                "lastPage": None,
                "pageInterval": None,
                "imageCount": None,
                "tableCount": None,
                "audioCount": None,
                "videoCount": None,
                "license": relator_work.license,
                "copyrightHolder": None,
                "landingPage": None,
                "lccn": None,
                "oclc": None,
                "shortAbstract": None,
                "longAbstract": chapter["blurbs"],
                "generalNote": None,
                "toc": None,
                "coverUrl": None,
                "coverCaption": None,
            }
            related_work_id = self.thoth.create_work(related_work)

            # Create work relation associating current work with child work
            work_relation = {
                "relatorWorkId": relator_work.workId,
                "relatedWorkId": related_work_id,
                "relationType": "HAS_CHILD",
                "relationOrdinal": chapter["sequence"],
            }
            self.thoth.create_work_relation(work_relation)

            for index, contributor in enumerate(chapter["chapter_authors"]):
                contributor.update({"contribution_type": "AUTHOR"})
                contributor.update({"sequence": index + 1})
                self.create_contributor(contributor, related_work_id)

            for index, subject in enumerate(chapter["keywords"]):
                self.create_subject("KEYWORD", subject["name"], index + 1, related_work_id)

            for index, subject in enumerate(chapter["disciplines"]):
                self.create_subject("CUSTOM", subject["name"], index + 1, related_work_id)

            for format in chapter["formats"]:
                publication_type = self.publication_types[format["file_type"]]
                self.create_publication(related_work_id, publication_type, None)
