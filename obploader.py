"""Load OBP metadata into Thoth"""

from bookloader import BookLoader


class OBPBookLoader(BookLoader):
    """OBP specific logic to ingest metadata from CSV into Thoth"""
    single_imprint = True
    publisher_name = "Open Book Publishers"
    publisher_shortname = "OBP"
    publisher_url = "https://www.openbookpublishers.com/"

    def run(self):
        """Process CSV and call Thoth to insert its data"""
        for row in self.data.index:
            work = self.get_work(row, self.imprint_id)
            work_id = self.thoth.create_work(work)
            print("workId: {}".format(work_id))

            self.create_publications(row, work_id, work["landingPage"])
            self.create_languages(row, work_id)
            self.create_subjects(row, work_id)
            self.create_contributors(row, work_id)
            self.create_series(row, self.imprint_id, work_id, work["workType"])

    def get_work(self, row, imprint_id):
        """Returns a dictionary with all attributes of a 'work'

        row: current row number

        imprint_id: previously obtained ID of this work's imprint
        """
        title = self.sanitise_title(self.data.at[row, 'Title'],
                                    self.data.at[row, 'Subtitle'])
        doi = "https://doi.org/{}/{}".format(self.data.at[row, 'DOI prefix'],
                                             self.data.at[row, 'DOI suffix'])
        try:
            publication_date = "-".join([
                str(int(self.data.at[row, "publication year"])),
                str(int(self.data.at[row, "publication month"])),
                str(int(self.data.at[row, "publication day"]))])
        except TypeError:
            publication_date = None

        copyright_text = ""
        for index in range(1, 4):
            holder = self.data.at[row, "Copyright holder {}".format(index)]
            if holder:
                if copyright_text != "":
                    copyright_text += "; "
                copyright_text += str(holder)
        oclc = int(self.data.at[row, "OCN (OCLC number)"]) \
            if self.data.at[row, "OCN (OCLC number)"] else None
        image_count = int(self.data.at[row, "no of illustrations"]) \
            if self.data.at[row, "no of illustrations"] else None
        table_count = int(self.data.at[row, "no of tables"]) \
            if self.data.at[row, "no of tables"] else None
        #  at OBP these are combined audio *and* video
        media_count = int(self.data.at[row, "no of audio/video"]) \
            if self.data.at[row, "no of audio/video"] else None

        work = {
            "workType": self.work_types[
                self.data.at[row, "Publication type"]],
            "workStatus": self.work_statuses[self.data.at[row, "Status"]],
            "fullTitle": title["fullTitle"],
            "title": title["title"],
            "subtitle": title["subtitle"],
            "reference": None,
            "edition": int(self.data.at[
                row, "edition number (integers only)"]),
            "imprintId": imprint_id,
            "doi": doi,
            "publicationDate": publication_date,
            "place": "Cambridge, UK",
            "width": int(self.data.at[row, "Width (mm)"]),
            "height": int(self.data.at[row, "Height (mm)"]),
            "pageCount": int(self.data.at[row, "no of pages"]),
            "pageBreakdown": self.data.at[row, "pages"],
            "imageCount": image_count,
            "tableCount": table_count,
            "audioCount": media_count,
            "videoCount": None,
            "license": self.data.at[
                row, "License URL (human-readable summary)"],
            "copyrightHolder": copyright_text,
            "landingPage": self.data.at[row, "Book-page URL"],
            "lccn": None,
            "oclc": oclc,
            "shortAbstract": self.data.at[
                row, "Short Blurb (less than 100 words)"],
            "longAbstract": self.data.at[row, "Plain Text Blurb"],
            "generalNote": None,
            "toc": self.data.at[row, "Table of Content"],
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
        currencies = ["GBP", "USD", "EUR", "AUD", "CAD"]
        for index in range(1, 6):
            publication_type = self.data.at[
                row, "Format {}".format(str(index))]
            isbn = self.sanitise_isbn(self.data.at[
                row, "ISBN {} with dashes".format(str(index))])
            url = landing_page
            if not publication_type or not isbn or len(isbn) != 17:
                continue
            publication_type = publication_type.upper()
            if publication_type == "PDF":
                url = self.data.at[row, "Full-text URL - PDF"]
            publication = {
                "workId": work_id,
                "publicationType": publication_type,
                "isbn": isbn,
                "publicationUrl": url
            }
            publication_id = self.thoth.create_publication(publication)
            if publication_type == "PDF":
                continue
            for currency in currencies:
                price = {
                    "publicationId": publication_id,
                    "currencyCode": currency,
                    "unitPrice": self.data.at[row, "{} price {}".format(
                        currency, publication_type.lower())]
                }
                if not price["unitPrice"]:
                    continue
                self.thoth.create_price(price)

        htmlreader = {
            "workId": work_id,
            "publicationType": "HTML",
            "isbn": None,
            "publicationUrl": self.data.at[row, "Full-text URL - HTML"]
        }
        if htmlreader["publicationUrl"]:
            self.thoth.create_publication(htmlreader)

    def create_languages(self, row, work_id):
        """Creates all languages associated with the current work

        row: current row number

        work_id: previously obtained ID of the current work
        """
        lang = self.data.at[row, "ONIX Language Code"].upper()
        original = self.data.at[row, "Original ONIX Language Code"].upper()
        if lang == original:
            language = {
                "workId": work_id,
                "languageCode": lang,
                "languageRelation": "ORIGINAL",
                "mainLanguage": "true"
            }
            languages = [language]
        else:
            languages = [{
                "workId": work_id,
                "languageCode": lang,
                "languageRelation": "TRANSLATED_INTO",
                "mainLanguage": "true"
            }, {
                "workId": work_id,
                "languageCode": original,
                "languageRelation": "ORIGINAL",
                "mainLanguage": "false"
            }]
        for language in languages:
            self.thoth.create_language(language)

    def create_subjects(self, row, work_id):
        """Creates all subjects associated with the current work

        row: current row number

        work_id: previously obtained ID of the current work
        """
        for index in range(1, 6):
            for stype in ["BIC", "BISAC"]:
                code = self.data.at[row, "{} subject code {}".format(
                    stype, index)]
                if not code:
                    continue
                subject = {
                    "workId": work_id,
                    "subjectType": stype,
                    "subjectCode": code,
                    "subjectOrdinal": index
                }
                self.thoth.create_subject(subject)

        custom = self.data.at[row, "Academic discipline (OBP)"]
        if custom:
            subject = {
                "workId": work_id,
                "subjectType": "CUSTOM",
                "subjectCode": custom,
                "subjectOrdinal": 1
            }
            self.thoth.create_subject(subject)

        keywords = self.data.at[row, "keywords"].replace(",", ";")
        if keywords:
            for index, keyword in enumerate(keywords.split(";")):
                if not keyword:
                    continue
                subject = {
                    "workId": work_id,
                    "subjectType": "KEYWORD",
                    "subjectCode": keyword.strip(),
                    "subjectOrdinal": index + 1
                }
                self.thoth.create_subject(subject)

    def create_contributors(self, row, work_id):
        """Creates all contributions associated with the current work

        row: current row number

        work_id: previously obtained ID of the current work
        """
        for index in range(1, 7):
            name = self.data.at[
                row, "Contributor {} first name".format(index)]
            surname = self.data.at[
                row, "Contributor {} surname".format(index)]
            if not name or not surname:
                continue
            fullname = "{} {}".format(name, surname)
            numeral = ".{}".format(index - 1) if index > 1 else ""
            orcid = self.data.at[row, "ORCID ID{}".format(numeral)]
            contributor = {
                "firstName": name,
                "lastName": surname,
                "fullName": fullname,
                "orcid": orcid,
                "website": None

            }
            if fullname not in self.all_contributors:
                contributor_id = self.thoth.create_contributor(contributor)
                self.all_contributors[fullname] = contributor_id
            else:
                contributor_id = self.all_contributors[fullname]

            numeral = ".{}".format(index - 1) if index > 1 else ""
            contribution_type = self.contribution_types[
                self.data.at[row, "OBP Role Name{}".format(numeral)]]
            main = "true" \
                if contribution_type in self.main_contributions \
                else "false"
            contribution = {
                "workId": work_id,
                "contributorId": contributor_id,
                "contributionType": contribution_type,
                "mainContribution": main,
                "biography": None,
                "institution": None
            }
            self.thoth.create_contribution(contribution)

    def create_series(self, row, imprint_id, work_id, work_type):
        """Creates all series associated with the current work

        row: current row number

        work_id: previously obtained ID of the current work

        work_type: previously obtained type of the current work
        """
        series_name = self.data.at[row, "Series Name"]
        issn_print = self.data.at[row, "ISSN Print with dashes"]
        issn_digital = self.data.at[row, "ISSN Digital with dashes"]
        issue_ordinal = self.data.at[row, "No. in the Series"]
        series_type = "JOURNAL" if work_type == "JOURNAL_ISSUE" \
            else "BOOK_SERIES"
        if series_name and issn_print:
            series = {
                "seriesType": series_type,
                "seriesName": series_name,
                "issnDigital": issn_digital,
                "issnPrint": issn_print,
                "seriesUrl": None,
                "imprintId": imprint_id
            }
            if series_name not in self.all_series:
                series_id = self.thoth.create_series(series)
                self.all_series[series_name] = series_id
            else:
                series_id = self.all_series[series_name]

            issue = {
                "seriesId": series_id,
                "workId": work_id,
                "issueOrdinal": int(issue_ordinal)
            }
            self.thoth.create_issue(issue)
