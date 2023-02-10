#!/usr/bin/env python
"""Load punctum chapter metadata into Thoth"""

import logging
from crossrefchapterloader import CrossrefChapterLoader
from bookloader import BookLoader


class ObpChapterLoaderCrossref(CrossrefChapterLoader):
    """OBP specific logic to ingest chapter metadata from Crossref into Thoth"""
    publisher_name = "Open Book Publishers"

    def run(self):
        """Obtain all books from Thoth, query Crossref to obtain chapter metadata and insert it into Thoth"""
        all_books = self.all_books()
        for index, book in enumerate(all_books):
            logging.info('Book (%s/%s): %s' % (index + 1, len(all_books), book['fullTitle']))
            book_doi = book['doi']
            book_simple_doi = CrossrefChapterLoader.simple_doi(book_doi)
            relation_ordinal = 1
            skip_count = 0  # number of times chapters have not been found in crossref

            while skip_count < 2:
                doi = "%s.%s" % (book_simple_doi, str(relation_ordinal).zfill(2))
                full_doi = CrossrefChapterLoader.full_doi(doi)

                if self.doi_in_thoth(full_doi):
                    relation_ordinal += 1
                    continue

                metadata = self.get_crossref_metadata(doi)
                if not metadata:
                    skip_count += 1
                    continue

                # There are a few deleted DOIs under the publisher "Test accounts" – skip them
                if metadata['publisher'] != self.publisher_name:
                    logging.warning('Skipping different publisher (%s) %s' % (metadata['publisher'], doi))
                    relation_ordinal += 1
                    continue

                # we issue DOIs for things others than chapters (e.g. resources) – skip them
                if metadata['type'] != "book-chapter":
                    logging.warning('Skipping (%s) %s' % (metadata['type'], doi))
                    relation_ordinal += 1
                    continue

                chapter = self.get_work(full_doi, metadata, book)
                logging.info('Chapter %s: %s' % (relation_ordinal, chapter['fullTitle']))
                work_id = self.thoth.create_work(chapter)
                try:
                    full_text_url = metadata['link'][0]['URL']
                    landing_page = book['landingPage']
                    publication_id = self.create_publication(work_id)
                    self.create_location(full_text_url, landing_page, publication_id)
                except (KeyError, IndexError):
                    pass
                try:
                    self.create_contributors(metadata, work_id)
                except KeyError:
                    logging.error('No contributors in %s' % doi)
                    pass
                self.create_chapter_relation(book['workId'], work_id, relation_ordinal)
                relation_ordinal += 1

    def get_work(self, doi, chapter, book):
        """Returns a dictionary with all attributes of a 'work'"""
        try:
            subtitle = chapter['subtitle'][0]
        except (KeyError, IndexError):
            subtitle = None
        title = BookLoader.sanitise_title(chapter['title'][0], subtitle)
        try:
            cc_license = chapter['license'][0]['URL']
        except KeyError:
            cc_license = None

        try:
            page_interval = chapter['page']
            try:
                (first_page, last_page) = page_interval.split('-')
                first_decimal = CrossrefChapterLoader.roman_to_decimal(first_page)
                last_decimal = CrossrefChapterLoader.roman_to_decimal(last_page)
                page_count = last_decimal - first_decimal
            except ValueError:
                first_page = last_page = page_interval
                page_interval = "%s-%s" % (page_interval, page_interval)
                page_count = 1
            page_count = page_count if page_count > 0 else 1
        except KeyError:
            page_interval = first_page = last_page = page_count = None

        try:
            authors = ["%s %s" % (a['given'], a['family']) for a in chapter['author']]
            copyright_text = "; ".join(authors)
        except KeyError:
            copyright_text = None

        work = {
            "workType": "BOOK_CHAPTER",
            "workStatus": "ACTIVE",
            "fullTitle": title["fullTitle"],
            "title": title["title"],
            "subtitle": title["subtitle"],
            "reference": None,
            "edition": None,
            "imprintId": self.imprint_id,
            "doi": doi,
            "publicationDate": book['publicationDate'],
            "place": book['place'],
            "width": None,
            "height": None,
            "pageCount": page_count,
            "pageBreakdown": None,
            "imageCount": None,
            "tableCount": None,
            "audioCount": None,
            "videoCount": None,
            "license": cc_license,
            "copyrightHolder": copyright_text,
            "landingPage": book['landingPage'],
            "lccn": None,
            "oclc": None,
            "shortAbstract": None,
            "longAbstract": None,
            "generalNote": None,
            "toc": None,
            "coverUrl": None,
            "coverCaption": None,
            "firstPage": first_page,
            "lastPage": last_page,
            "pageInterval": page_interval
        }
        return work

    def create_publication(self, work_id):
        publication = {
            "publicationType": "PDF",
            "workId": work_id,
            "isbn": None,
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
        return publication_id

    def create_location(self, full_text_url, landing_page, publication_id):
        location = {
            "locationPlatform": "OTHER",
            "publicationId": publication_id,
            "canonical": "true",
            "fullTextUrl": full_text_url,
            "landingPage": landing_page,
        }
        location_id = self.thoth.create_location(location)
        return location_id

    def create_contributors(self, chapter, work_id):
        """Creates all contributions associated with the current work"""
        contribution_ordinal = 0
        for author in chapter['author']:
            name = author['given']
            surname = author['family']
            fullname = "%s %s" % (name, surname)
            try:
                orcid = author['ORCID'].replace('http:', 'https:')
            except KeyError:
                orcid = None

            contributor = {
                "firstName": name,
                "lastName": surname,
                "fullName": fullname,
                "orcid": orcid,
                "website": None
            }

            contributor_id = None
            if orcid in self.all_contributors:
                contributor_id = self.all_contributors[orcid]
            if not contributor_id:
                if fullname not in self.all_contributors:
                    contributor_id = self.thoth.create_contributor(contributor)
                    self.all_contributors[fullname] = contributor_id
                    if orcid:
                        self.all_contributors[orcid] = contributor_id
                else:
                    contributor_id = self.all_contributors[fullname]

            contribution_ordinal += 1
            contribution = {
                "workId": work_id,
                "contributorId": contributor_id,
                "contributionType": "AUTHOR",
                "mainContribution": "true",
                "biography": None,
                "firstName": name,
                "lastName": surname,
                "fullName": fullname,
                "contributionOrdinal": contribution_ordinal
            }
            contribution_id = self.thoth.create_contribution(contribution)

            affiliation_ordinal = 1
            for crossref_institution in author['affiliation']:
                institution = {
                    "institutionName": crossref_institution['name'],
                    "institutionDoi": None,
                    "ror": None,
                    "countryCode": None
                }
                try:
                    institution_id = self.all_institutions[institution['institutionName']]
                except KeyError:
                    institution_id = self.thoth.create_institution(institution)
                    self.all_institutions[institution['institutionName']] = institution_id
                affiliation = {
                    "contributionId": contribution_id,
                    "institutionId": institution_id,
                    "affiliationOrdinal": affiliation_ordinal,
                    "position": None
                }
                self.thoth.create_affiliation(affiliation)
                affiliation_ordinal += 1
