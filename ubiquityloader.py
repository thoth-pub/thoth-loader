"""Load Ubiquity presses metadata into Thoth"""
import re
import pandas as pd

from bookloader import BookLoader
from thothlibrary import ThothError


class UbiquityPressesLoader(BookLoader):
    """
    Ubiquity specific logic to ingest metadata from CSV into Thoth
    Currently only ingests works from LSE Press and University of Westminster Press
    Works which already exist in Thoth should be extended/overwritten
    """

    def run(self):
        """Process CSV and call Thoth to insert its data"""
        for row in self.data.index:
            self.publisher_name = self.data.at[row, 'publisher']
            if self.publisher_name == "LSE Press":
                self.publisher_url = "https://press.lse.ac.uk/"
                self.set_publisher_and_imprint()
            elif self.publisher_name == "University of Westminster Press":
                self.publisher_shortname = "UWP"
                self.publisher_url = "https://www.uwestminsterpress.co.uk/"
                self.set_publisher_and_imprint()
            else:
                continue
            work = self.get_work(row)
            try:
                work_id = self.thoth.work_by_doi(work['doi']).workId
                existing_work = self.thoth.work_by_id(work_id)
                existing_work.update((k, v) for k, v in work.items() if v is not None)
                self.thoth.update_work(existing_work)
            except (IndexError, AttributeError, ThothError):
                work_id = self.thoth.create_work(work)
            print("workId: {}".format(work_id))
            work = self.thoth.work_by_id(work_id)
            self.create_contributors(row, work)
            self.create_publications(row, work_id)
            self.create_languages(row, work)
            self.create_subjects(row, work)
            self.create_relations(row, work)

    # pylint: disable=too-many-locals
    def get_work(self, row):
        """Returns a dictionary with all attributes of a 'work'

        row: current row number
        """
        work_type = self.work_types[self.data.at[row, "work_type"]]
        work_status = self.data.at[row, "work_status"] \
            if pd.notna(self.data.at[row, "work_status"]) else "Active"
        title = self.data.at[row, 'title']
        subtitle = self.data.at[row, 'subtitle']
        title = self.sanitise_title(title, subtitle)
        edition = int(self.data.at[row, "edition"]) \
            if pd.notna(self.data.at[row, "edition"]) else 1
        doi = "https://doi.org/{}".format(self.sanitise_string(self.data.at[row, 'doi'])) \
            if self.data.at[row, 'doi'] else None
        reference = str(self.data.at[row, "reference"]) \
            if pd.notna(self.data.at[row, "reference"]) else None
        publication_date = str(self.data.at[row, "publication_date"]) \
            if pd.notna(self.data.at[row, "publication_date"]) else None
        publication_place = str(self.data.at[row, "publication_place"]) \
            if pd.notna(self.data.at[row, "publication_place"]) else None
        license_abbrev = \
            self.data.at[row, "license"] \
            if pd.notna(self.data.at[row, "license"]) \
            else None
        if license_abbrev == None:
            license_url = None
        elif license_abbrev == "cc-4-by":
            license_url = "https://creativecommons.org/licenses/by/4.0/"
        elif license_abbrev == "cc-4-by-nc":
            license_url = "https://creativecommons.org/licenses/by-nc/4.0/"
        elif license_abbrev == "cc-4-by-nc-nd":
            license_url = "https://creativecommons.org/licenses/by-nc-nd/4.0/"
        else:
            # TODO proper logging
            print("Unrecognised license: %s" % license_abbrev)
            raise
        copyright_holder = self.data.at[row, 'copyright_holder']
        landing_page = "https://{}".format(self.data.at[row, "landing_page"]) \
            if pd.notna(self.data.at[row, "landing_page"]) else None
        page_count = int(self.data.at[row, "page_count"]) \
            if pd.notna(self.data.at[row, "page_count"]) else None
        page_breakdown = self.data.at[row, "page_breakdown"] \
            if pd.notna(self.data.at[row, "page_breakdown"]) else None
        first_page = str(self.data.at[row, "first_page"]) \
            if pd.notna(self.data.at[row, "first_page"]) else None
        last_page = str(self.data.at[row, "last_page"]) \
            if pd.notna(self.data.at[row, "last_page"]) else None
        page_interval = str(self.data.at[row, "page_interval"]) \
            if pd.notna(self.data.at[row, "page_interval"]) else None
        if not page_interval and first_page and last_page:
            page_interval = "{}–{}".format(first_page, last_page)
        image_count = int(self.data.at[row, "image_count"]) \
            if pd.notna(self.data.at[row, "image_count"]) else None
        table_count = int(self.data.at[row, "table_count"]) \
            if pd.notna(self.data.at[row, "table_count"]) else None
        audio_count = int(self.data.at[row, "audio_count"]) \
            if pd.notna(self.data.at[row, "audio_count"]) else None
        video_count = int(self.data.at[row, "video_count"]) \
            if pd.notna(self.data.at[row, "video_count"]) else None
        lccn = str(self.data.at[row, "lccn"]) \
            if pd.notna(self.data.at[row, "lccn"]) else None
        oclc = str(self.data.at[row, "oclc"]) \
            if pd.notna(self.data.at[row, "oclc"]) else None
        short_abstract = \
            self.data.at[row, "short_abstract"] \
            if pd.notna(self.data.at[row, "short_abstract"]) \
            else None
        long_abstract = self.data.at[row, "long_abstract"] \
            if pd.notna(self.data.at[row, "long_abstract"]) else None
        general_note = self.data.at[row, "general_note"] \
            if pd.notna(self.data.at[row, "general_note"]) else None
        bibliography_note = self.data.at[row, "bibliography_note"] \
            if pd.notna(self.data.at[row, "bibliography_note"]) else None
        toc = self.data.at[row, "toc"] \
            if pd.notna(self.data.at[row, "toc"]) else None
        cover_url = self.data.at[row, "cover_url"] \
            if pd.notna(self.data.at[row, "cover_url"]) else None
        cover_caption = self.data.at[row, "cover_caption"] \
            if pd.notna(self.data.at[row, "cover_caption"]) else None

        work = {
            "imprintId": self.imprint_id,
            "workType": work_type,
            "workStatus": self.work_statuses[work_status],
            "fullTitle": title["fullTitle"],
            "title": title["title"],
            "subtitle": title["subtitle"],
            "edition": edition,
            "doi": doi,
            "reference": reference,
            "publicationDate": publication_date,
            "place": publication_place,
            "license": license_url,
            "copyrightHolder": copyright_holder,
            "landingPage": landing_page,
            "pageCount": page_count,
            "pageBreakdown": page_breakdown,
            "firstPage": first_page,
            "lastPage": last_page,
            "pageInterval": page_interval,
            "imageCount": image_count,
            "tableCount": table_count,
            "audioCount": audio_count,
            "videoCount": video_count,
            "lccn": lccn,
            "oclc": oclc,
            "shortAbstract": short_abstract,
            "longAbstract": long_abstract,
            "generalNote": general_note,
            "bibliographyNote": bibliography_note,
            "toc": toc,
            "coverUrl": cover_url,
            "coverCaption": cover_caption,
        }
        return work

    def create_contributors(self, row, work):
        """Creates all contributions associated with the current work

        row: current row number

        work: current work
        """
        column = self.data.at[row, "contributions"]
        contributions = re.findall('\\((.*?\\[\\(.*?\\)\\])\\)', column)
        work_contributions = self.get_work_contributions(work)
        for index, contribution_string in enumerate(contributions):
            affiliations = re.findall('\\((".*?")\\)', contribution_string)
            contribution = re.split(',', contribution_string)
            contribution_type = contribution[0].strip().strip('"').upper()
            contribution_type = self.contribution_types[contribution_type]
            first_name = contribution[1].strip().strip('"')
            last_name = contribution[2].strip().strip('"')
            full_name = contribution[3].strip().strip('"')
            is_main = contribution[4].strip().strip('"')
            biography = contribution[5].strip().strip('"')
            orcid = contribution[6].strip().strip('"')
            if orcid:
                orcid = "https://orcid.org/{}".format(orcid)
            website = contribution[7].strip().strip('"')
            contributor = {
                "firstName": first_name,
                "lastName": last_name,
                "fullName": full_name,
                "orcid": orcid,
                "website": website,
            }
            # skip this contribution if already in the work
            if orcid in work_contributions or full_name in work_contributions:
                continue

            # contribution not in work, try to get contributor or create it
            if orcid and orcid in self.all_contributors:
                contributor_id = self.all_contributors[orcid]
            elif full_name in self.all_contributors:
                contributor_id = self.all_contributors[full_name]
            else:
                contributor_id = self.thoth.create_contributor(contributor)
                # cache new contributor
                self.all_contributors[full_name] = contributor_id
                if orcid:
                    self.all_contributors[orcid] = contributor_id

            contribution = {
                "workId": work.workId,
                "contributorId": contributor_id,
                "contributionType": contribution_type,
                "mainContribution": is_main,
                "contributionOrdinal": index + 1,
                "biography": biography,
                "firstName": first_name,
                "lastName": last_name,
                "fullName": full_name
            }
            contribution_id = self.thoth.create_contribution(contribution)

            for index, affiliation_string in enumerate(affiliations):
                affiliation = re.split(',', affiliation_string)
                position = affiliation[0].strip().strip('"')
                institution_name = affiliation[1].strip().strip('"')
                institution_doi = affiliation[2].strip().strip('"')
                ror = affiliation[3].strip().strip('"')
                country_code = affiliation[4].strip().strip('"')
                if institution_name:
                    # retrieve institution or create if it doesn't exist
                    if institution_name in self.all_institutions:
                        institution_id = self.all_institutions[institution_name]
                    else:
                        institution = {
                            "institutionName": institution_name,
                            "institutionDoi": institution_doi,
                            "ror": ror,
                            "countryCode": country_code,
                        }
                        institution_id = self.thoth.create_institution(institution)
                        self.all_institutions[institution_name] = institution_id
                    affiliation = {
                        "contributionId": contribution_id,
                        "institutionId": institution_id,
                        "affiliationOrdinal": index + 1,
                        "position": position,
                    }
                    self.thoth.create_affiliation(affiliation)

    def create_publications(self, row, work_id):
        """Creates all publications associated with the current work

        row: current row number

        work_id: previously obtained ID of the current work
        """
        column = self.data.at[row, "publications"]
        publications = re.findall(
            '\\((.*?\\[\\(.*?\\)\\],\\[\\(.*?\\)\\])\\)', column)
        for publication_string in publications:
            (prices_string, locations_string) = re.findall(
                '\\[(.*?)\\],\\[(.*?)\\]', publication_string)[0]
            prices = re.findall('\\((.*?)\\)', prices_string)
            locations = re.findall('\\((.*?)\\)', locations_string)
            publication = re.split(',', publication_string)
            publication_type = publication[0].strip().strip('"').upper()
            isbn = publication[1].strip().strip('"')
            isbn = self.sanitise_isbn(isbn)
            width_mm = publication[2].strip().strip('"')
            width_cm = publication[3].strip().strip('"')
            width_in = publication[4].strip().strip('"')
            height_mm = publication[5].strip().strip('"')
            height_cm = publication[6].strip().strip('"')
            height_in = publication[7].strip().strip('"')
            depth_mm = publication[8].strip().strip('"')
            depth_cm = publication[9].strip().strip('"')
            depth_in = publication[10].strip().strip('"')
            weight_g = publication[11].strip().strip('"')
            weight_oz = publication[12].strip().strip('"')
            publication = {
                "workId": work_id,
                "publicationType": publication_type,
                "isbn": isbn,
                "widthMm": width_mm,
                "widthCm": width_cm,
                "widthIn": width_in,
                "heightMm": height_mm,
                "heightCm": height_cm,
                "heightIn": height_in,
                "depthMm": depth_mm,
                "depthCm": depth_cm,
                "depthIn": depth_in,
                "weightG": weight_g,
                "weightOz": weight_oz,
            }
            publication_id = self.thoth.create_publication(publication)
            for price_string in prices:
                price = re.split(',', price_string)
                currency_code = price[0].strip().strip('"')
                unit_price = price[1].strip().strip('"')
                unit_price = self.sanitise_price(unit_price)
                # No point trying to create zero prices (not allowed in Thoth)
                if unit_price and (unit_price != 0.0):
                    price = {
                        "publicationId": publication_id,
                        "currencyCode": currency_code,
                        "unitPrice": unit_price,
                    }
                    self.thoth.create_price(price)
            for location_string in locations:
                location = re.split(',', location_string)
                landing_page = location[0].strip().strip('"')
                if landing_page:
                    landing_page = "https://{}".format(landing_page)
                full_text_url = location[1].strip().strip('"')
                if full_text_url:
                    full_text_url = "https://{}".format(full_text_url)
                platform = location[2].strip().strip('"')
                is_canonical = location[3].strip().strip('"')
                location = {
                    "publicationId": publication_id,
                    "landingPage": landing_page,
                    "fullTextUrl": full_text_url,
                    "locationPlatform": platform,
                    "canonical": is_canonical,
                }
                self.thoth.create_location(location)

    def create_languages(self, row, work):
        """Creates all languages associated with the current work

        row: current row number

        work: current work
        """
        column = self.data.at[row, "languages"]
        languages = re.findall('\\((.*?)\\)', column)
        for language_string in languages:
            language = re.split(',', language_string)
            language_relation = language[0].strip().strip('"').upper()
            language_code = language[1].strip().strip('"').upper()
            is_main = language[2].strip().strip('"')

            # skip this language if the work already has a language with that code
            if any(l.languageCode == language_code for l in work.languages):
                continue

            language = {
                "workId": work_id,
                "languageRelation": language_relation,
                "languageCode": language_code,
                "mainLanguage": is_main,
            }
            self.thoth.create_language(language)

    def create_subjects(self, row, work):
        """Creates all subjects associated with the current work

        row: current row number

        work: current work
        """
        for stype in ["bic", "thema", "bisac", "lcc", "custom_categories", "keywords"]:
            column = self.data.at[row, stype] \
                if pd.notna(self.data.at[row, stype]) else None
            if not column or not column.strip():
                continue
            codes = re.findall('"(.*?)"', column)
            for index, code in enumerate(codes):
                if stype == "custom_categories":
                    subject_type = "CUSTOM"
                elif stype == "keywords":
                    subject_type = "KEYWORD"
                else:
                    subject_type = stype.upper()
                subject_code = code.strip()

                # skip this subject if the work already has a subject
                # with that subject type/subject code combination
                if any((s.subjectCode == subject_code and s.subjectType == subject_type \
                    for s in work.subjects)):
                    continue

                subject = {
                    "workId": work_id,
                    "subjectType": subject_type,
                    "subjectCode": subject_code,
                    "subjectOrdinal": index + 1,
                }
                self.thoth.create_subject(subject)

    def create_relations(self, row, relator_work):
        """Creates all relations associated with the current work

        row: current row number

        relator_work: current work
        """
        column = self.data.at[row, "relations"] \
            if pd.notna(self.data.at[row, "relations"]) else None
        if not column or not column.strip():
            return
        relations = re.findall('\\((".*?")\\)', column)
        for relation_string in relations:
            relation = re.findall('"(.*?)"', relation_string)
            relation_title = relation[0].strip().strip('"')
            doi = relation[1].strip().strip('"')
            if doi:
                doi = "https://doi.org/{}".format(doi)
            relation_type = relation[2].strip().strip('"').upper()
            relation_ordinal = relation[3].strip().strip('"')

            if relation_type == "HAS_CHILD":
                # Create a new work which inherits from the current work
                related_work = {
                    "workType": "BOOK_CHAPTER",
                    "workStatus": relator_work.workStatus,
                    "fullTitle": relation_title,
                    "title": relation_title,
                    "subtitle": None,
                    "reference": None,
                    "edition": None,
                    "imprintId": relator_work.imprintId,
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
                    "longAbstract": None,
                    "generalNote": None,
                    "toc": None,
                    "coverUrl": None,
                    "coverCaption": None,
                }
                related_work_id = self.thoth.create_work(related_work)

                # Create work relation associating current work with new work
                work_relation = {
                    "relatorWorkId": relator_work.workId,
                    "relatedWorkId": related_work_id,
                    "relationType": relation_type,
                    "relationOrdinal": relation_ordinal,
                }
                self.thoth.create_work_relation(work_relation)

            else:
                # TODO proper logging
                print("Unhandled relation type: %s" % relation_type)
                raise
