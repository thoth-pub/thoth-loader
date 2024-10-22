#!/usr/bin/env python
"""Load metadata from Ubiquity API endpoints (XXX.rua.re) into Thoth"""

from bookloader import BookLoader
from thothlibrary import ThothError


class UbiquityAPILoader(BookLoader):
    """
    Ubiquity specific logic to ingest metadata from JSON API dump into Thoth
    Currently only ingests works from Radboud and UWP
    Works which already exist in Thoth should be extended/overwritten
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
            try:
                work_id = self.thoth.work_by_doi(work['doi']).workId
                existing_work = self.thoth.work_by_id(work_id)
                existing_work.update((k, v)
                                     for k, v in work.items() if v is not None)
                self.thoth.update_work(existing_work)
            except (IndexError, AttributeError, ThothError):
                work_id = self.thoth.create_work(work)
            print("workId: {}".format(work_id))
            work = self.thoth.work_by_id(work_id)
            self.create_contributors(record, work)
            self.create_publications(record, work)
            self.create_languages(record, work)
            self.create_subjects(record, work)
            self.create_series(record, work)
            self.create_relations(record, work)

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
            "publicationDate": self.sanitise_date(record["publication_date"]),
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

    def create_contributor(self, contributor, work, highest_contribution_ordinal):
        """Creates a contribution associated with the specified work

        contributor: dict extracted from JSON record representing a single contribution

        work: the associated work (either the current work or a child of it)

        highest_contribution_ordinal: highest ordinal found to be associated with an existing contribution
        (passed as a one-item array so that it can be modified by-reference)
        """
        first_name = contributor["first_name"].strip()
        last_name = contributor["last_name"].strip()
        full_name = contributor["full_name"].strip()
        biography = contributor["biography"].strip()
        orcid = self.sanitise_orcid(contributor["orcid"])
        contribution_type = contributor["contribution_type"]
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

        existing_contribution = next((c for c in work.contributions if c.contributor.contributorId == contributor_id
                                      and c.contributionType == contribution_type), None)
        if existing_contribution:
            contribution_id = existing_contribution.contributionId
        else:
            new_contribution = {
                "workId": work.workId,
                "contributorId": contributor_id,
                "contributionType": contribution_type,
                "mainContribution": "true",
                "contributionOrdinal": highest_contribution_ordinal[0] + 1,
                "biography": biography,
                "firstName": first_name,
                "lastName": last_name,
                "fullName": full_name,
            }
            contribution_id = self.thoth.create_contribution(new_contribution)
            highest_contribution_ordinal[0] += 1

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

            existing_affiliations = next(
                (c.affiliations for c in work.contributions if c.contributionId == contribution_id), [])

            if not any(a.institution.institutionId == institution_id for a in existing_affiliations):
                highest_affiliation_ordinal = max((a.affiliationOrdinal for a in existing_affiliations), default=0)
                new_affiliation = {
                    "contributionId": contribution_id,
                    "institutionId": institution_id,
                    "affiliationOrdinal": highest_affiliation_ordinal + 1,
                    "position": department,
                }

                self.thoth.create_affiliation(new_affiliation)

    def create_contributors(self, record, work):
        """Creates all contributions associated with the current work

        record: current JSON record

        work: current work
        """
        for editor in record["editor"]:
            editor.update({"contribution_type": "EDITOR"})

        for author in record["author"]:
            author.update({"contribution_type": "AUTHOR"})

        # Pass ordinal as a one-item array so that it can be modified by-reference
        highest_contribution_ordinal = [max((c.contributionOrdinal for c in work.contributions), default=0)]
        for contributor in record["editor"] + record["author"]:
            self.create_contributor(contributor, work, highest_contribution_ordinal)

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

    def create_publications(self, record, work):
        """Creates all publications associated with the current work

        record: current JSON record

        work: current work
        """
        for format in record["formats"] + record["physical_formats"]:
            publication_type = self.publication_types[format["file_type"]]
            existing_pub = next((p for p in work.publications if p.publicationType == publication_type), None)
            if existing_pub:
                publication_id = existing_pub.publicationId
            else:
                isbn = next((n["value"] for n in record["identifier"] if n["identifier"] == "isbn-13"
                             and n["object_id"] == format["id"]), None)
                publication_id = self.create_publication(work.workId, publication_type, isbn)

            if existing_pub and existing_pub.locations:
                existing_canonical = True
            else:
                existing_canonical = False

            for index, retailer in enumerate([retailer for retailer in record["retailers"]
                                              if retailer["type"].lower() == format["file_type"]]):
                canonical = "true" if index == 0 and not existing_canonical else "false"
                location = {
                    "publicationId": publication_id,
                    "landingPage": retailer["url"],
                    "fullTextUrl": None,
                    "locationPlatform": "OTHER",
                    "canonical": canonical,
                }
                self.thoth.create_location(location)

    def create_languages(self, record, work):
        """Creates languages associated with the current work

        record: current JSON record

        work: current work
        """
        for lang_dict in record["languages"]:
            language_code = self.language_codes[lang_dict["code"]]
            # skip this language if the work already has a language with that code
            if any(l.languageCode == language_code for l in work.languages):
                continue
            language = {
                "workId": work.workId,
                "languageCode": language_code,
                "languageRelation": "ORIGINAL",
                "mainLanguage": "true",
            }
            self.thoth.create_language(language)

    def create_subject(self, subject_type, subject_code, subject_ordinal, work):
        """Creates a subject associated with the specified work
        (either the current work or a child of it)
        """
        # skip this subject if the work already has a subject
        # with that subject type/subject code combination
        if any(s.subjectCode == subject_code and s.subjectType == subject_type for s in work.subjects):
            return
        subject = {
            "workId": work.workId,
            "subjectType": subject_type,
            "subjectCode": subject_code,
            "subjectOrdinal": subject_ordinal,
        }
        self.thoth.create_subject(subject)

    def create_subjects(self, record, work):
        """Creates subjects associated with the current work

        record: current JSON record

        work: current work
        """
        for index, subject in enumerate(record["keywords"]):
            self.create_subject("KEYWORD", subject["name"], index + 1, work)

        for index, subject in enumerate(record["subject"]):
            self.create_subject("CUSTOM", subject["name"], index + 1, work)

    def create_series(self, record, work):
        """Creates series associated with the current work

        record: current JSON record

        work: current work
        """
        if not record["series"]:
            return

        series_name = record["series"]["title"]
        # skip this issue if the work already has an issue in this series
        try:
            if any(i.series.seriesName == series_name for i in work.issues):
                return
        except AttributeError:
            # If work.issues isn't present at all, we know it's fine to add the issue
            pass

        new_series = None
        if series_name not in self.all_series:
            series_description = record["series"]["description"]
            issn = record["series"]["issn"] if record["series"]["issn"] != "XXXX-XXXX" else None
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

        ordinal = record["series_number"]
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
            "workId": work.workId,
            "issueOrdinal": int(ordinal),
        }

        self.thoth.create_issue(issue)

    def create_relations(self, record, relator_work):
        """Creates all relations associated with the current work

        record: current JSON record

        relator_work: current work
        """
        highest_child_ordinal = max(
            (r.relationOrdinal for r in relator_work.relations if r.relationType == "HAS_CHILD"), default=0)
        for chapter in record["chapters"]:
            doi = self.sanitise_doi(chapter["doi"])
            if any(r.relatedWork.doi.rstrip('/') == doi.rstrip('/') for r in relator_work.relations):
                # Relation specified in dataset already exists in Thoth - can update it
                related_work_id = self.thoth.work_by_doi(doi).workId
            else:
                try:
                    # Check whether child work exists but relation hasn't been created
                    related_work_id = self.thoth.work_by_doi(doi).workId
                except (IndexError, AttributeError, ThothError):
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
                        "doi": doi,
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

                relation_ordinal = chapter["sequence"]
                if any(r.relationOrdinal == relation_ordinal and r.relationType == "HAS_CHILD"
                       for r in relator_work.relations):
                    # Avoid clashes if a child with this ordinal already exists
                    # but is linked to a different DOI - just use next available
                    relation_ordinal = highest_child_ordinal + 1
                    highest_child_ordinal += 1

                # Create work relation associating current work with child work
                work_relation = {
                    "relatorWorkId": relator_work.workId,
                    "relatedWorkId": related_work_id,
                    "relationType": "HAS_CHILD",
                    "relationOrdinal": relation_ordinal,
                }
                self.thoth.create_work_relation(work_relation)

            thoth_related_work = self.thoth.work_by_id(related_work_id)

            # Pass ordinal as a one-item array so that it can be modified by-reference
            highest_contribution_ordinal = [max(
                (c.contributionOrdinal for c in thoth_related_work.contributions), default=0)]
            for contributor in chapter["chapter_authors"]:
                contributor.update({"contribution_type": "AUTHOR"})
                self.create_contributor(contributor, thoth_related_work, highest_contribution_ordinal)

            for index, subject in enumerate(chapter["keywords"]):
                self.create_subject("KEYWORD", subject["name"], index + 1, thoth_related_work)

            for index, subject in enumerate(chapter["disciplines"]):
                self.create_subject("CUSTOM", subject["name"], index + 1, thoth_related_work)

            for format in chapter["formats"]:
                publication_type = self.publication_types[format["file_type"]]
                if not any(p.publicationType == publication_type for p in thoth_related_work.publications):
                    self.create_publication(related_work_id, publication_type, None)
