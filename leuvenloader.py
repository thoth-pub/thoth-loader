#!/usr/bin/env python
"""Load Leuven University Press metadata into Thoth"""

import logging
import requests
import itertools
from bookloader import BookLoader
from onix3 import Onix3Record


class LeuvenLoader(BookLoader):
    """Leuven University Press specific logic to ingest metadata from ONIX into Thoth"""
    import_format = "ONIX3"
    single_imprint = True
    publisher_name = "Leuven University Press"
    publisher_shortname = None
    publisher_url = "https://lup.be/"
    cache_institutions = True
    cache_series = True

    def run(self):
        """Process ONIX and call Thoth to insert its data"""
        default_language = self.data.header.default_language_of_text.value.value.upper()
        issues_to_create = []
        for product in self.data.no_product_or_product:
            record = Onix3Record(product)
            work = self.get_work(record)
            work_id = self.thoth.create_work(work)
            logging.info('workId: %s' % work_id)
            self.create_publications(record, work_id)
            self.create_contributors(record, work_id)
            self.create_languages(record, work_id, default_language)
            self.create_subjects(record, work_id)
            self.create_fundings(record, work_id)
            issues_to_create.extend(
                self.extract_issues_data(record, work_id))
        self.create_all_issues(issues_to_create)

    def get_work(self, record):
        """Returns a dictionary with all attributes of a 'work'

        record: current onix record
        """
        title = record.title()

        try:
            doi = record.doi()
        except IndexError:
            doi = None

        landing_page = record.available_content_url()
        if landing_page is None and doi is not None:
            # Backstop: resolve DOI to obtain landing page
            # (this takes some time and is not always accurate)
            landing_page = requests.get(doi).url

        edition = record.edition_number()
        if edition is None:
            edition = 1

        long_abstract = record.long_abstract()
        if long_abstract is not None:
            long_abstract = long_abstract.replace("\r", "")

        short_abstract = record.short_abstract()
        if short_abstract is not None:
            short_abstract = short_abstract.replace("\r", "")

        work = {
            "workType": record.work_type(),
            "workStatus": self.work_statuses[record.work_status()],
            "fullTitle": title["fullTitle"],
            "title": title["title"],
            "subtitle": title["subtitle"],
            "reference": record.related_system_internal_identifier(),
            "edition": edition,
            "imprintId": self.imprint_id,
            "doi": doi,
            "publicationDate": record.publication_date(),
            "place": record.publication_place(),
            "pageCount": record.page_count(),
            "pageBreakdown": None,
            "imageCount": record.illustration_count(),
            "tableCount": None,
            "audioCount": None,
            "videoCount": None,
            "license": record.license(),
            "copyrightHolder": record.copyright_holder(),
            "landingPage": landing_page,
            "lccn": None,
            "oclc": None,
            "shortAbstract": short_abstract,
            "longAbstract": long_abstract,
            "generalNote": None,
            "bibliographyNote": None,
            "toc": record.toc(),
            "coverUrl": record.cover_url(),
            "coverCaption": None,
            "firstPage": None,
            "lastPage": None,
            "pageInterval": None,
        }
        return work

    def create_fundings(self, record, work_id):
        funders = record.fundings()
        funder_names = [f.publisher_identifier_or_publisher_name[0].value
                        for f in funders if f.publisher_identifier_or_publisher_name]
        program = None
        for institution_name in funder_names:
            # normalize commonly used funder names
            if institution_name == "ERC":
                institution_name = "European Research Council"
                program = None
            elif institution_name == "KU Leuven Fund for Fair Open Access":
                institution_name = "KU Leuven"
                program = "Fund for Fair Open Access"
            else:
                program = None

            # retrieve institution or create if it doesn't exist
            if institution_name in self.all_institutions:
                institution_id = self.all_institutions[institution_name]
            else:
                institution = {
                    "institutionName": institution_name,
                    "institutionDoi": None,
                    "ror": None,
                    "countryCode": None,
                }
                institution_id = self.thoth.create_institution(institution)
                # cache new institution
                self.all_institutions[institution_name] = institution_id

            funding = {
                "workId": work_id,
                "institutionId": institution_id,
                "program": program,
                "projectName": None,
                "projectShortname": None,
                "grantNumber": None,
                "jurisdiction": None,
            }

            self.thoth.create_funding(funding)

    def create_publications(self, record, work_id):
        """Creates publication and prices associated with the current work

        record: current onix record

        work_id: previously obtained ID of the current work
        """

        def create_location():
            oapen_record_id = oapen_landing_page_url[-5:]
            isbn_no_hyphens = record.isbn().replace("-", "")
            full_text_url = "https://library.oapen.org/bitstream/handle/20.500.12657/" \
                + oapen_record_id + "/" + isbn_no_hyphens + ".pdf"
            location = {
                "publicationId": publication_id,
                "landingPage": oapen_landing_page_url,
                "fullTextUrl": full_text_url,
                "locationPlatform": "OAPEN",
                "canonical": canonical,
            }
            self.thoth.create_location(location)

        main_publication = {
            "workId": work_id,
            "publicationType": self.publication_types[record.product_type()],
            "isbn": record.isbn(),
            "widthMm": None,
            "widthCm": None,
            "widthIn": None,
            "heightMm": None,
            "heightCm": None,
            "heightIn": None,
            "depthMm": None,
            "depthCm": None,
            "depthIn": None,
            "weightG": None,
            "weightOz": None,
        }
        publication_id = self.thoth.create_publication(main_publication)

        for index, oapen_landing_page_url in enumerate(record.full_text_urls()):
            canonical = "true" if index == 0 else "false"
            create_location()

        for (product_type, isbn) in record.alternative_formats():
            # Translations etc are sometimes included in "alternative formats" section
            if product_type != record.product_type():
                related_publication = {
                    "workId": work_id,
                    "publicationType": self.publication_types[product_type],
                    "isbn": isbn,
                    "widthMm": None,
                    "widthCm": None,
                    "widthIn": None,
                    "heightMm": None,
                    "heightCm": None,
                    "heightIn": None,
                    "depthMm": None,
                    "depthCm": None,
                    "depthIn": None,
                    "weightG": None,
                    "weightOz": None,
                }
                self.thoth.create_publication(related_publication)

    def create_contributors(self, record, work_id):
        """Creates all contributions associated with the current work

        record: current onix record

        work_id: previously obtained ID of the current work
        """
        for contributor_record in record.contributors():
            given_name = None
            try:
                given_name = Onix3Record.get_names_before_key(
                    contributor_record)
            except IndexError:
                # Sometimes this is missing: this is OK as it's optional in Thoth
                pass
            family_name = Onix3Record.get_key_names(contributor_record)
            full_name = Onix3Record.get_person_name(contributor_record)
            orcid = Onix3Record.get_orcid(contributor_record)

            if orcid and orcid in self.all_contributors:
                contributor_id = self.all_contributors[orcid]
            elif full_name in self.all_contributors:
                contributor_id = self.all_contributors[full_name]
            else:
                contributor = {
                    "firstName": given_name,
                    "lastName": family_name,
                    "fullName": full_name,
                    "orcid": orcid,
                    "website": None,
                }
                contributor_id = self.thoth.create_contributor(contributor)
                # cache new contributor
                self.all_contributors[full_name] = contributor_id
                if orcid:
                    self.all_contributors[orcid] = contributor_id

            contribution = {
                "workId": work_id,
                "contributorId": contributor_id,
                "contributionType": self.contribution_types[contributor_record.contributor_role[0].value.value],
                "mainContribution": "true",
                "contributionOrdinal": int(contributor_record.sequence_number.value),
                "biography": Onix3Record.get_biography(contributor_record),
                "firstName": given_name,
                "lastName": family_name,
                "fullName": full_name,
            }
            self.thoth.create_contribution(contribution)

    def create_languages(self, record, work_id, default_language):
        """Creates language associated with the current work

        record: current onix record

        work_id: previously obtained ID of the current work

        default_language: default language code to use if no language is found in record
        """
        languages = record.language_codes_and_roles()
        if len(languages) == 0:
            languages.append((default_language, "ORIGINAL"))
        for (language_code, language_relation) in languages:
            language = {
                "workId": work_id,
                "languageCode": language_code,
                "languageRelation": language_relation,
                "mainLanguage": "true"
            }
            self.thoth.create_language(language)

    def create_subjects(self, record, work_id):
        """Creates all subjects associated with the current work

        record: current onix record

        work_id: previously obtained ID of the current work
        """
        def process_codes(codes, subject_type):
            for index, subject_code in enumerate(codes):
                subject = {
                    "workId": work_id,
                    "subjectType": subject_type,
                    "subjectCode": subject_code,
                    "subjectOrdinal": index + 1
                }
                self.thoth.create_subject(subject)

        process_codes(record.thema_codes(), "THEMA")
        process_codes(record.bisac_codes(), "BISAC")
        process_codes(record.bic_codes(), "BIC")
        process_codes(record.keywords_from_text(), "KEYWORD")
        process_codes(record.custom_codes(), "CUSTOM")

    def extract_issues_data(self, record, work_id):
        """
        Extracts required data for all issues associated with the current work,
        creating serieses where they don't already exist (if possible)

        record: current onix record

        work_id: previously obtained ID of the current work
        """
        issues_in_work = []
        for series_record in record.serieses():
            series_name = Onix3Record.get_series_name(series_record)
            if series_name in self.all_series:
                series_id = self.all_series[series_name]
            elif not series_name:
                # Can't add series without name, so can't add issue
                continue
            else:
                series = {
                    "seriesType": "BOOK_SERIES",
                    "seriesName": series_name,
                    "issnDigital": None,
                    "issnPrint": None,
                    "seriesUrl": None,
                    "seriesDescription": None,
                    "seriesCfpUrl": None,
                    "imprintId": self.imprint_id
                }
                series_id = self.thoth.create_series(series)
                self.all_series[series_name] = series_id
            issue_ordinal = Onix3Record.get_issue_ordinal(series_record)
            issues_in_work.append({
                "work_id": work_id,
                "series_id": series_id,
                "issue_ordinal": issue_ordinal,
            })
        return issues_in_work

    def create_all_issues(self, issues_to_create):
        """
        Creates series and issue data for all relevant works in ONIX file,
        ensuring numbering within series is consistent

        issues_to_create: list of dicts representing issues to create
        """
        sorted_issues = sorted(issues_to_create, key=lambda x: x['series_id'])
        grouped_issues = []
        for key, group in itertools.groupby(sorted_issues, key=lambda x: x['series_id']):
            grouped_issues.append(list(group))

        for issue_list in grouped_issues:
            # Sorts issues in same series by ordinal, pushing Nones to start of list
            # Retains series order but avoids problems with missing/clashing ordinals
            sorted_list = sorted(issue_list, key=lambda x: (
                x['issue_ordinal'] is not None, x['issue_ordinal']))
            for index, issue_data in enumerate(sorted_list):
                issue = {
                    "seriesId": issue_data['series_id'],
                    "workId": issue_data['work_id'],
                    "issueOrdinal": index + 1,
                }
                try:
                    self.thoth.create_issue(issue)
                except Exception as e:
                    logging.error(f"{e} ({issue_data['work_id']})")
