#!/usr/bin/env python
"""Load University of London Press metadata into Thoth"""

import logging
import requests
import itertools
from bookloader import BookLoader
from onix3 import Onix3Record


class UOLLoader(BookLoader):
    """University of London Press specific logic to ingest metadata from ONIX into Thoth"""
    import_format = "ONIX3"
    single_imprint = True
    publisher_name = "University of London Press"
    publisher_shortname = None
    publisher_url = "https://uolpress.co.uk/"
    cache_institutions = True

    def run(self):
        """Process ONIX and call Thoth to insert its data"""
        default_language = self.data.header.default_language_of_text.value.value.upper()
        # Default currency is also supplied, but no records in dataset have price amount without currency
        # default_currency = self.data.header.default_currency_code.value.value
        # there's one publication per ONIX product, all related using a common ID within <RelatedWork>.
        # sort all related products into their own list: [[product, product], [product, product]]
        products = [Onix3Record(product)
                    for product in self.data.no_product_or_product]
        sorted_products = sorted(
            products, key=lambda x: x.related_system_internal_identifier())
        grouped_products = []
        for key, group in itertools.groupby(sorted_products, key=lambda x: x.related_system_internal_identifier()):
            grouped_products.append(list(group))

        issues_to_create = []
        for product_list in grouped_products:
            if len(product_list) == 1:
                canonical_record = product_list[0]
            else:
                # Where a PDF record is present, it's usually the most comprehensive
                # Otherwise, no consistent pattern/differences are minor, so choose first record
                # (Almost all fields are replicated across records in a group; occasionally
                # DOI/licence/landing page are missing from some, or page counts/dates differ slightly)
                try:
                    canonical_record = [record for record in product_list
                                        if self.publication_types[record.product_type()] == "PDF"][0]
                except IndexError:
                    canonical_record = product_list[0]

            work = self.get_work(canonical_record)
            work_id = self.thoth.create_work(work)
            logging.info('workId: %s' % work_id)
            for record in product_list:
                self.create_publications(record, work_id)
            self.create_contributors(canonical_record, work_id)
            self.create_languages(canonical_record, work_id, default_language)
            self.create_subjects(canonical_record, work_id)
            issues_to_create.extend(
                self.extract_issues_data(canonical_record, work_id))

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

    def create_publications(self, record, work_id):
        """Creates publication and prices associated with the current work

        record: current onix record

        work_id: previously obtained ID of the current work
        """
        def create_price():
            price = {
                "publicationId": publication_id,
                "currencyCode": currency_code,
                "unitPrice": unit_price,
            }
            self.thoth.create_price(price)

        def create_location():
            location = {
                "publicationId": publication_id,
                "landingPage": url,
                "fullTextUrl": url,
                "locationPlatform": "OTHER",
                "canonical": canonical,
            }
            self.thoth.create_location(location)

        publication = {
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
        for measure_type, measure_unit, measurement in record.dimensions():
            try:
                publication.update(
                    {self.dimension_types[(measure_type, measure_unit)]: measurement})
            except KeyError:
                # Ignore any dimension types which aren't stored in Thoth (e.g. weight in kg)
                pass
        publication_id = self.thoth.create_publication(publication)

        # Records frequently include the same currency/price pair multiple times
        # (representing different suppliers): remove duplicates
        for currency_code, unit_price in list(set(record.prices())):
            create_price()

        for index, url in enumerate(record.full_text_urls()):
            canonical = "true" if index == 0 else "false"
            create_location()

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
                    "website": Onix3Record.get_website(contributor_record),
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
            contribution_id = self.thoth.create_contribution(contribution)

            for index, (position, institution_string) in enumerate(Onix3Record.get_affiliations_with_positions(contributor_record)):
                if institution_string is None:
                    # can't add a position without an institution
                    continue

                # Institution string sometimes concludes with country name in brackets
                # TODO could potentially extract this country name for use in creating
                # new institution - but would have to convert to country code
                institution_name = institution_string.split('(')[0].rstrip()

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

                affiliation = {
                    "contributionId": contribution_id,
                    "institutionId": institution_id,
                    "position": position,
                    "affiliationOrdinal": index + 1
                }
                self.thoth.create_affiliation(affiliation)

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
            issn = None
            try:
                issn = BookLoader.sanitise_issn(
                    Onix3Record.get_issn(series_record))
            except ValueError as e:
                logging.error(f"{e} ({work_id})")

            # TODO for first import there will be no existing UoL series;
            # if updating for recurring import, initialise self.all_series first
            if issn and issn in self.all_series:
                series_id = self.all_series[issn]
            elif series_name and series_name in self.all_series:
                series_id = self.all_series[series_name]
            elif not series_name or not issn:
                # Can't add series without name/ISSN, so can't add issue
                continue
            else:
                # Only one ISSN per series is permitted in ONIX
                # Use for both print and digital (as Thoth requires both)
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
                self.all_series[issn] = series_id
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
