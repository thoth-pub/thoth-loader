#!/usr/bin/env python
"""Load SciELO metadata into Thoth"""

import json
import logging
import sys
import re
import roman
from bookloader import BookLoader
from thothlibrary import ThothError

class SciELOLoader(BookLoader):
    """Shared logic for SciELO book and chapter loaders"""
    import_format = "JSON"
    single_imprint = True
    cache_institutions = False

    def create_contributors(self, record, work):
        """Creates/updates all contributors associated with the current work and their contributions

        record: current JSON record

        work: Work from Thoth
        """
        highest_contribution_ordinal = max((c.contributionOrdinal for c in work.contributions), default=0)
        for creator in record["creators"]:
            orcid_id = None
            website = None

            # sometimes JSON contains "creators" "role" information, but
            # no "full_name". Only create a contributor if "full_name" is not null.
            if creator[1][1]:
                # JSON "full_name" generally contains a comma separating surname and name
                # e.g. "Quevedo-Blasco, Raúl"
                if ',' in creator[1][1]:
                    full_name_inverted = creator[1][1].split(',')
                    name = full_name_inverted[1].strip()
                    surname = full_name_inverted[0]
                    full_name = f"{name} {surname}"
                # sometimes JSON "full_name" field contains an institution name,
                # e.g. "Universidad de Granada". In this case, name, surname, and full_name
                # are all assigned the institution name.
                else:
                    name = surname = full_name = creator[1][1]
                contribution_type = self.contribution_types[creator[0][1]]
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
                    "fullName": full_name,
                    "orcid": orcid_id,
                    "website": website,
                }
                # determine the identifier to use (prefer ORCID ID if available)
                identifier = orcid_id if orcid_id else full_name

                # check if the contributor is not in Thoth
                if identifier not in self.all_contributors:
                    # if not in Thoth, create a new contributor
                    contributor_id = self.thoth.create_contributor(contributor)
                    logging.info(f"created contributor: {contributor_id}")
                    # cache new contributor
                    self.all_contributors[full_name] = contributor_id
                    if orcid_id:
                        self.all_contributors[orcid_id] = contributor_id
                else:
                    # if in Thoth, get the contributor_id and run
                    # update_scielo_contributor to check if any values need to be updated
                    contributor_id = self.all_contributors[identifier]
                    self.update_scielo_contributor(contributor, contributor_id)

                existing_contribution = next((c for c in work.contributions if c.contributor.contributorId == contributor_id), None)
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
                    logging.info(f"created contribution with contributorId: {contributor_id}")
                    highest_contribution_ordinal += 1
                else:
                    logging.info(f"existing contribution with contributorId: {contributor_id}, did not update")

    def update_scielo_contributor(self, contributor, contributor_id):
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
            logging.info(f"contributor data from JSON and Thoth matches, no changes needed to Thoth: {contributor_id}")
        return contributor

    def create_languages(self, record, work):
        """Creates language associated with the current work

        record: current JSON record

        work: Work from Thoth
        """
        language_code = None
        # for book JSON, language is in "language" field
        if "language" in record:
            language_code = self.language_codes[record["language"]]
        # for chapter JSON, language is in "text_language" field
        elif "text_language" in record:
            language_code = self.language_codes[record["text_language"]]

        # check to see if work already has this language
        if any(l.languageCode == language_code for l in work.languages):
            logging.info("existing language, did not update")
            return
        language = {
            "workId": work.workId,
            "languageCode": language_code,
            "languageRelation": "ORIGINAL",
            "mainLanguage": "true"
        }
        self.thoth.create_language(language)
        logging.info(f"created language for workId: {work.workId}")

    def create_publications(self, record, work):
        """Creates PDF, EPUB, and paperback publications associated with the current work

        record: current JSON record

        work: Work from Thoth
        """
        pdf_url = record["pdf_url"]

        # logic for chapter metadata
        if record["TYPE"] == "Part":
            books_url_regex = r"https://books\.scielo\.org/id/.{5}"
            match = re.match(books_url_regex, pdf_url)
            if match:
                books_url = match.group()
            else:
                books_url = None
            publications = [["PDF", None, books_url, pdf_url]]
        # logic for book metadata
        else:
            books_url = record["books_url"]
            epub_url = record["epub_url"]
            eisbn = self.sanitise_isbn(record["eisbn"])
            isbn = self.sanitise_isbn(record["isbn"])
            publications = [["PDF", None, books_url, pdf_url], ["EPUB", eisbn, books_url, epub_url], ["PAPERBACK", isbn, books_url, None]]

        for publication_type, isbn, landing_page, full_text in publications:
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
                logging.info(f"existing publication: {publication_id}, did not update")
            else:
                publication_id = self.thoth.create_publication(publication)
                logging.info(f"created publication: {publication_id}")
            if existing_pub and any(l.locationPlatform == "SCIELO_BOOKS" for l in existing_pub.locations):
                logging.info("existing location, did not update")
                continue
            location = {
                "publicationId": publication_id,
                "landingPage": landing_page,
                "fullTextUrl": full_text,
                "locationPlatform": "SCIELO_BOOKS",
                "canonical": "true",
            }
            self.thoth.create_location(location)
            logging.info(f"created location with publicationId {publication_id}")

class SciELOChapterLoader(SciELOLoader):
    """SciELO specific logic to ingest chapter metadata from JSON into Thoth"""

    def run(self):
        """Process JSON and call Thoth to insert its data"""
        for record in self.data:
            logging.info("*************\n" * 4)
            logging.info(f"processing chapter: {record['title']}")

            book_title = record["monograph_title"]
            chapter_internal_id = record["_id"]
            book_id = self.get_book_by_title(book_title).workId
            relation_ordinal = record["order"]
            chapter_exists = False
            try:
                chapter = self.get_chapter_by_string(chapter_internal_id)
                chapter_exists = True
                logging.info("Chapter already exists, attempting to update")
                try:
                    work = self.get_work(record, self.imprint_id, book_id, chapter_exists)
                    chapter.update((k, v) for k, v in work.items() if v is not None)
                    # handle case where firstPage, lastPage, and chapterInterval were null in JSON, and thus
                    # weren't added to record in Thoth, creating an error with update_work
                    if 'firstPage' not in chapter:
                        chapter['firstPage'] = None
                    if 'lastPage' not in chapter:
                        chapter['lastPage'] = None
                    if 'pageInterval' not in chapter:
                        chapter['pageInterval'] = None
                    chapter_id = self.thoth.update_work(chapter)
                    logging.info(f"updated chapter: {chapter['title']}")
                    chapter_work = self.thoth.work_by_id(chapter_id)
                    self.create_languages(record, chapter_work)
                    logging.info("languages updated")
                    self.create_contributors(record, chapter_work)
                    logging.info("contributors updated")
                    self.create_publications(record, chapter_work)
                    logging.info("publications updated")
                except ThothError as t:
                    logging.error(f"Failed to update chapter: {chapter['title']}, exception: {t}")
                    sys.exit(1)
            except IndexError:
                logging.info("Chapter does not exist in Thoth, creating")
                chapter_exists = False
                # add new chapter to Book Work
                work = self.get_work(record, self.imprint_id, book_id, chapter_exists)
                chapter_id = self.thoth.create_work(work)
                logging.info(f"created chapter: {chapter_id}")
                chapter_work = self.thoth.work_by_id(chapter_id)
                self.create_languages(record, chapter_work)
                logging.info("languages created")
                self.create_contributors(record, chapter_work)
                logging.info("contributors created")
                self.create_publications(record, chapter_work)
                logging.info("publications created")
                # convert relation_ordinal from JSON to int
                if relation_ordinal == "00":
                    relation_ordinal = 1
                else:
                    relation_ordinal = int(relation_ordinal) + 1
                self.create_chapter_relation(book_id, chapter_id, relation_ordinal)
                logging.info("chapter relation created")

    def get_work(self, record, imprint_id, book_id, chapter_exists):
        """Returns a dictionary with all attributes of a 'work'

        record: current JSON record

        imprint_id: previously obtained ID of this work's imprint
        """
        first_page = None
        last_page = None
        page_interval = None
        page_count = None
        doi = None

        existing_book = self.thoth.work_by_id(book_id, True)
        thoth_book_record = json.loads(existing_book)['data']['work']
        place = thoth_book_record['place']
        cc_license = thoth_book_record['license']
        landing_page = thoth_book_record['landingPage']
        publication_date = self.sanitise_date(record["monograph_year"])
        title = self.split_title(record["title"])
        raw_doi = record["descriptive_information"] if record["descriptive_information"] else None
        if raw_doi:
            doi = self.sanitise_doi(raw_doi)
        # JSON has some duplicate DOIs for chapters, so check if DOI already exists in Thoth
        # to avoid an error with create_work
        if doi:
            doi = self.check_duplicate_doi(doi, chapter_exists, record)
        if record["pages"][0][1]:
            first_page = record["pages"][0][1]
        if record["pages"][1][1]:
            last_page = record["pages"][1][1]
        if first_page and last_page:
            page_interval = "{}–{}".format(first_page, last_page)
            if first_page.isdigit() and last_page.isdigit():
                page_count = int(last_page) - int(first_page) + 1
            # logic in case we have roman numerals in the page numbers
            # haven't found any in JSON so far, but just in case
            else:
                try:
                    first_page_int = roman.fromRoman(first_page.upper())
                    last_page_int = roman.fromRoman(last_page.upper())
                    page_count = last_page_int - first_page_int + 1
                except ValueError:
                    logging.error(f"Page numbers are not integer or roman numeral: {first_page}–{last_page}; skipping adding page numbers to Thoth")
                    pass

        work = {
            "workType": "BOOK_CHAPTER",
            "workStatus": "ACTIVE",
            "fullTitle": title["fullTitle"],
            "title": title["title"],
            "subtitle": title["subtitle"],
            "reference": record["_id"],
            "edition": None,
            "imprintId": imprint_id,
            "doi": doi,
            "publicationDate": publication_date,
            "place": place,
            "pageCount": page_count,
            "pageBreakdown": None,
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
            "longAbstract": None,
            "generalNote": None,
            "bibliographyNote": None,
            "toc": None,
            "coverUrl": None,
            "coverCaption": None,
            "firstPage": first_page if first_page else None,
            "lastPage": last_page if last_page else None,
            "pageInterval": page_interval if page_interval else None,
        }
        return work

    def get_chapter_by_string(self, string):
        """Query Thoth to return a chapter given a string, e.g. internal ID"""
        chapter = self.thoth.works(search=string, work_types = "BOOK_CHAPTER", publishers=f'"{self.publisher_id}"')
        return chapter[0]

    def check_duplicate_doi(self, doi, chapter_exists, record):
        if chapter_exists:
            # find if current JSON chapter has a DOI in Thoth from internal ID
            chapter_from_internal_id = self.get_chapter_by_string(record["_id"])
            chapter_doi = chapter_from_internal_id.doi
            work_id_from_internal_id = chapter_from_internal_id.workId
            try:
                # see if there's a work in Thoth with the doi from JSON
                chapter_from_doi = self.thoth.work_by_doi(doi=doi)
                # get existing workId associated with DOI
                work_id_from_doi = chapter_from_doi.workId
            except ThothError:
                logging.info("JSON has DOI, but DOI is not present in record in Thoth. Adding DOI to Thoth record.")
            # if the existing work in Thoth that we found by internal ID has a DOI,
            # and its workId matches the work we found in Thoth by DOI, then it's the same work.
            # in this case, keep current DOI.
            if chapter_doi and work_id_from_internal_id == work_id_from_doi:
                logging.info("existing chapter by internal ID in Thoth has DOI, and workId matches work found by DOI. Keeping current DOI.")
            else:
                logging.info("duplicate DOI, do not assign")
                doi = None
        else:
            try:
                self.thoth.work_by_doi(doi=doi)
                logging.info(f"existing doi in Thoth: {doi} for different chapter, skipping adding doi to current chapter")
                doi = None
            except ThothError:
                doi = doi
        return doi

class SciELOBookLoader(SciELOLoader):
    """SciELO specific logic to ingest metadata from Book JSON into Thoth"""

    def run(self):
        """Process JSON and call Thoth to insert its data"""
        for record in self.data:
            logging.info("*************\n" * 4)
            logging.info(f"processing book: {record['title']}")
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
                    except ThothError as t:
                        logging.error(f"Failed to update work with id {work_id}, exception: {t}")
                        sys.exit(1)
            # if work isn't found, create it
            except (IndexError, AttributeError, ThothError):
                work_id = self.thoth.create_work(work)
                logging.info(f"created workId: {work_id}")
            work = self.thoth.work_by_id(work_id)
            # below methods check for existing data
            # and create or update as necessary
            self.create_publications(record, work)
            self.create_contributors(record, work)
            self.create_languages(record, work)
            self.create_subjects(record, work)

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

    def create_subjects(self, record, work):
        """Creates all subjects associated with the current work

        record: current JSON record

        work: Work from Thoth
        """
        bisac_subject_code = record["bisac_code"][0][0][1]
        keyword_subject_codes = record["primary_descriptor"].split("; ")

        def create_subject(subject_type, subject_code, subject_ordinal):
            subject = {
                "workId": work.workId,
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




