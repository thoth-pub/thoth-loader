#!/usr/bin/env python
"""Load punctum metadata into Thoth"""

from bookloader import BookLoader


class PunctumBookLoader(BookLoader):
    """Punctum specific logic to ingest metadata from CSV into Thoth"""
    single_imprint = False
    publisher_name = "punctum books"
    publisher_shortname = None
    publisher_url = "https://punctumbooks.com/"
    all_imprints = {}

    def run(self):
        """Process CSV and call Thoth to insert its data"""
        for row in self.data.index:
            imprint_name = self.data.at[row, "Imprint"]
            imprint_id = self.imprint_id
            if imprint_name:
                imprint_id = self.get_imprint(row)

            work = self.get_work(row, imprint_id)
            work_id = self.thoth.create_work(work)
            print("workId: {}".format(work_id))
            self.create_publications(row, work_id, work["landingPage"])
            self.create_languages(row, work_id)
            self.create_subjects(row, work_id)
            self.create_contributors(row, work_id)

    def get_imprint(self, row):
        """Create imprint if it doesn't already exist, otherwise return ID"""
        imprint_name = self.data.at[row, "Imprint"]
        try:
            imprint_id = self.all_imprints[imprint_name]
        except KeyError:
            imprint = {
                "publisherId": self.publisher_id,
                "imprintName": imprint_name,
                "imprintUrl": None
            }
            imprint_id = self.thoth.create_imprint(imprint)
            self.all_imprints[imprint_name] = imprint_id
        return imprint_id

    def get_work(self, row, imprint_id):
        """Returns a dictionary with all attributes of a 'work'

        row: current row number

        imprint_id: previously obtained ID of this work's imprint
        """
        try:
            title, subtitle = self.data.at[row, 'Book Title'].split(":")
            title = title.strip()
            subtitle = subtitle.strip()
        except ValueError:
            title = self.data.at[row, 'Book Title']
            subtitle = None
        title = self.sanitise_title(title, subtitle)
        doi = "https://doi.org/{}".format(self.data.at[row, 'DOI'].strip())

        publication_date = self.sanitise_date(self.data.at[row, "Date"])

        copyright_text = "{}; {}".format(self.data.at[row, "Authors"],
                                         self.data.at[row, "Editors"])
        page_count = int(self.data.at[row, "Number of Pages"]) \
            if self.data.at[row, "Number of Pages"] else None
        edition = int(self.data.at[row, "Edition"]) \
            if self.data.at[row, "Edition"] else 1
        lccn = str(int(self.data.at[row, "LCCN"])) \
            if self.data.at[row, "LCCN"] else None

        work = {
            "workType": self.work_types[
                self.data.at[row, "Type of Document"]],
            "workStatus": "ACTIVE",
            "fullTitle": title["fullTitle"],
            "title": title["title"],
            "subtitle": title["subtitle"],
            "reference": self.data.at[row, "Record Reference"],
            "edition": edition,
            "imprintId": imprint_id,
            "doi": doi,
            "publicationDate": publication_date,
            "place": self.data.at[row, "Place of publication"],
            "width": self.in_to_mm(self.data.at[row, "Width (in)"]),
            "height": self.in_to_mm(self.data.at[row, "Height (in)"]),
            "pageCount": page_count,
            "pageBreakdown": None,
            "imageCount": None,
            "tableCount": None,
            "audioCount": None,
            "videoCount": None,
            "license": self.data.at[row, "License"],
            "copyrightHolder": copyright_text,
            "landingPage": self.data.at[row, "Website"],
            "lccn": lccn,
            "oclc": None,
            "shortAbstract": None,
            "longAbstract": self.data.at[row, "Abstract"],
            "generalNote": None,
            "toc": None,
            "coverUrl": self.data.at[row, "Cover URL"],
            "coverCaption": None,
        }
        return work

    def create_publications(self, row, work_id, landing_page):
        """Creates all publications associated with the current work

        row: current row number

        work_id: previously obtained ID of the current work

        landing_page: previously obtained landing page of the current work
        """
        publications = [{
            "workId": work_id,
            "publicationType": "PAPERBACK",
            "isbn": self.sanitise_isbn(self.data.at[row, "Primary ISBN"]),
            "publicationUrl": landing_page
        }, {
            "workId": work_id,
            "publicationType": "PDF",
            "isbn": self.sanitise_isbn(self.data.at[row, "Other ISBN"]),
            "publicationUrl": self.data.at[row, "OAPEN URL"]
        }]
        for publication in publications:
            self.thoth.create_publication(publication)

    def create_languages(self, row, work_id):
        """Creates all languages associated with the current work

        row: current row number

        work_id: previously obtained ID of the current work
        """
        languages = self.data.at[row, "Language"].upper().split(";")
        for language in languages:
            language = {
                "workId": work_id,
                "languageCode": language.strip(),
                "languageRelation": "ORIGINAL",
                "mainLanguage": "true"
            }
            self.thoth.create_language(language)

    def create_subjects(self, row, work_id):
        """Creates all subjects associated with the current work

        row: current row number

        work_id: previously obtained ID of the current work
        """
        subjects = {
            "BIC": self.data.at[row, "BIC"],
            "THEMA": self.data.at[row, "Thema"],
            "KEYWORD": self.data.at[row, "Keywords"]
        }
        for stype, codes in subjects.items():
            if not codes:
                continue
            for index, code in enumerate(codes.replace(",", ";").split(";")):
                if not code:
                    continue
                subject = {
                    "workId": work_id,
                    "subjectType": stype,
                    "subjectCode": code.strip(),
                    "subjectOrdinal": index + 1
                }
                self.thoth.create_subject(subject)

    def create_contributors(self, row, work_id):
        """Creates all contributions associated with the current work

        row: current row number

        work_id: previously obtained ID of the current work
        """
        contributors = {
            "AUTHOR": self.data.at[row, "Authors"],
            "EDITOR": self.data.at[row, "Editors"]
        }

        for contribution_type, people in contributors.items():
            if not people:
                continue
            for contributor in people.split(";"):
                if not contributor:
                    continue
                names = contributor.split(",")
                surname = names[0].strip()
                if len(names) == 1:
                    name = None
                    fullname = surname
                    orcid = None
                elif len(names) == 2:
                    name = names[1].strip()
                    surname = names[0].strip()
                    try:
                        orcid = self.orcid_regex.search(name).group(0)
                        name = name.replace(" ({})".format(orcid), "")
                    except AttributeError:
                        orcid = None
                    fullname = "{} {}".format(name, surname)
                contributor = {
                    "firstName": name,
                    "lastName": surname,
                    "fullName": fullname,
                    "orcid": orcid,
                    "website": None

                }
                if fullname not in self.all_contributors:
                    contributor_id = self.thoth.create_contributor(
                        contributor)
                    self.all_contributors[fullname] = contributor_id
                else:
                    contributor_id = self.all_contributors[fullname]

                contribution = {
                    "workId": work_id,
                    "contributorId": contributor_id,
                    "contributionType": contribution_type,
                    "mainContribution": "true",
                    "biography": None,
                    "institution": None
                }
                self.thoth.create_contribution(contribution)
