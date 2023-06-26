#!/usr/bin/env python
"""Load WHP metadata into Thoth"""

import logging
import requests
from bookloader import BookLoader


class UWPLoader(BookLoader):
    """WHP specific logic to ingest metadata from CSV into Thoth"""
    import_format = "MARCXML"
    single_imprint = True
    publisher_name = "University of Westminster Press"
    publisher_shortname = "UWP"
    publisher_url = "https://www.uwestminsterpress.co.uk/"

    def run(self):
        """Process CSV and call Thoth to insert its data"""
        for record in self.data:
            work = self.get_work(record, self.imprint_id)
            work_id = self.thoth.create_work(work)
            logging.info('workId: %s' % work_id)
            self.create_publications(record, work_id)
            self.create_languages(record, work_id)
            self.create_subjects(record, work_id)
            self.create_contributors(record, work_id)

    def get_work(self, record, imprint_id):
        """Returns a dictionary with all attributes of a 'work'

        record: current MARC record

        imprint_id: previously obtained ID of this work's imprint
        """
        reference = record.get('001').value()
        doi = record.get('856').get('u')
        try:
            title = record.get('245').get('a').rstrip("\\/:").strip()
            subtitle = record.get('245').get('b').rstrip("\\/:").strip()
        except (ValueError, AttributeError):
            title = record.get('245').get('a').rstrip("\\/:").strip()
            subtitle = None
        title = self.sanitise_title(title, subtitle)

        # only year is included
        publication_year = record.get('264').get('c').replace("[", "").replace("]", "").replace(".", "")
        publication_date = "%s-01-01" % publication_year

        page_count, page_breakdown = self.parse_page_string(record.get('300').get('a'))
        place = record.get('264').get('a').rstrip(" :")
        abstract = record.get('520').get('a').lstrip('"').replace('"--Publisher\'s website.', '')
        bibliography_note = record.get('504').get('a') if record.get('504') else None

        license_statement = record.get('540').value()
        if license_statement == "Open access. This is an Open Access book distributed under the terms of the Creative " \
                                "Commons Attribution 4.0 license (unless stated otherwise), which permits " \
                                "unrestricted use, distribution and reproduction in any medium, provided the original " \
                                "work is properly cited. Copyright is retained by the author(s).":
            cc_license = "http://creativecommons.org/licenses/by/4.0/"
        elif license_statement == "Open access. This book distributed under the terms of the Creative Commons " \
                                  "Attribution + Noncommercial + NoDerivatives 4.0 license. Copyright is retained by " \
                                  "the author(s).":
            cc_license = "https://creativecommons.org/licenses/by-nc-nd/4.0/"
        else:
            logging.error("Unrecognised license: %s" % license_statement)
            raise

        roles = [subfield for field in record.get_fields('100', '700') for subfield in field.get_subfields('e')]
        work_type = "EDITED_BOOK" if any('editor' in role for role in roles) else "MONOGRAPH"

        # resolve DOI to obtain landing page
        response = requests.get(doi)
        landing_page = response.url

        work = {
            "workType": work_type,
            "workStatus": "ACTIVE",
            "fullTitle": title["fullTitle"],
            "title": title["title"],
            "subtitle": title["subtitle"],
            "reference": reference,
            "edition": 1,
            "imprintId": imprint_id,
            "doi": doi,
            "publicationDate": publication_date,
            "place": place,
            "pageCount": page_count,
            "pageBreakdown": page_breakdown,
            "imageCount": None,
            "tableCount": None,
            "audioCount": None,
            "videoCount": None,
            "license": cc_license,
            "copyrightHolder": None,
            "landingPage": landing_page,
            "lccn": None,
            "oclc": None,
            "shortAbstract": None,
            "longAbstract": abstract,
            "generalNote": None,
            "bibliographyNote": bibliography_note,
            "toc": None,
            "coverUrl": None,
            "coverCaption": None,
            "firstPage": None,
            "lastPage": None,
            "pageInterval": None,
        }
        return work

    # pylint: disable=too-many-locals
    def create_publications(self, record, work_id):
        """Creates all publications associated with the current work

        record: current MARC record

        work_id: previously obtained ID of the current work
        """
        publications = []
        for isbn in record.get_fields('020'):
            publications.append((isbn.get('q').upper(), self.sanitise_isbn(isbn.get('a'))))

        # physical: if one value is present it's the paperback, if two then the first one is hardback
        if record.get('776'):
            physical = record.get('776').get_subfields('z')
            if len(physical) == 1:
                publications.append(("PAPERBACK", self.sanitise_isbn(physical[0])))
            elif len(physical) == 2:
                (hardback, paperback) = physical
                publications.append(("PAPERBACK", self.sanitise_isbn(paperback)))
                publications.append(("HARDBACK", self.sanitise_isbn(hardback)))
        prices = [item for item in record.get('037').get_subfields('c') if '£' in item and item != '£0']

        for ptype, isbn in publications:
            publication = {
                "workId": work_id,
                "publicationType": ptype,
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
            if ptype == "PAPERBACK" and prices:
                price = {
                    "publicationId": publication_id,
                    "currencyCode": "GBP",
                    "unitPrice": prices[0].replace("£", "").replace(" (paperback)", "")
                }
                self.thoth.create_price(price)
            elif ptype == "HARDBACK" and len(prices) == 2:
                price = {
                    "publicationId": publication_id,
                    "currencyCode": "GBP",
                    "unitPrice": prices[1].replace("£", "").replace(" (hardback)", "")
                }
                self.thoth.create_price(price)

    def create_languages(self, record, work_id):
        """Creates all languages associated with the current work

        record: current MARC record

        work_id: previously obtained ID of the current work
        """
        work_lang = record.get('008').value()[-5:-2].upper()
        original_lang = None
        if record.get('041'):
            original_lang = record.get('041').get('h').upper()

        if work_lang and original_lang:
            languages = [
                ("TRANSLATED_INTO", work_lang),
                ("TRANSLATED_FROM", original_lang),
            ]
        else:
            languages = [("ORIGINAL", work_lang)]

        for lang_relation, language in languages:
            language = {
                "workId": work_id,
                "languageCode": language.strip().upper(),
                "languageRelation": lang_relation,
                "mainLanguage": "true"
            }
            self.thoth.create_language(language)

    def create_subjects(self, record, work_id):
        """Creates all subjects associated with the current work

        record: current MARC record

        work_id: previously obtained ID of the current work
        """
        subject = {
            "workId": work_id,
            "subjectType": "LCC",
            "subjectCode": record.get('050').value()[:-4].replace(" ", ""),
            "subjectOrdinal": 1
        }
        self.thoth.create_subject(subject)

    def create_contributors(self, record, work_id):
        """Creates all contributions associated with the current work

        record: current MARC record

        work_id: previously obtained ID of the current work
        """
        contributors = []
        for field in record.get_fields('100', '700'):
            name = field.get('a').rstrip(',')
            for role in field.get_subfields('e'):
                contributors.append((name, role.rstrip(',').rstrip('.')))

        for index, (contributor, role) in enumerate(contributors):
            if not contributor:
                continue

            names = contributor.split(",")
            surname = names[0].strip()
            if len(names) == 1:
                name = None
                fullname = surname
            elif len(names) == 2:
                name = names[1].strip()
                surname = names[0].strip()
                fullname = "{} {}".format(name, surname)
            contributor = {
                "firstName": name,
                "lastName": surname,
                "fullName": fullname,
                "orcid": None,
                "website": None

            }
            if fullname not in self.all_contributors:
                contributor_id = self.thoth.create_contributor(
                    contributor)
                self.all_contributors[fullname] = contributor_id
            else:
                contributor_id = self.all_contributors[fullname]

            contribution_role = self.contribution_types[role]
            is_main = "true" if contribution_role in ["AUTHOR", "EDITOR"] else "false"
            contribution = {
                "workId": work_id,
                "contributorId": contributor_id,
                "contributionType": contribution_role,
                "mainContribution": is_main,
                "contributionOrdinal": index + 1,
                "biography": None,
                "firstName": name,
                "lastName": surname,
                "fullName": fullname,
            }
            self.thoth.create_contribution(contribution)
