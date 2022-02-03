"""Load African Minds metadata into Thoth"""
import re
import pandas as pd

from bookloader import BookLoader
from thothlibrary import ThothError


class AfricanMindsBookLoader(BookLoader):
    """African Minds specific logic to ingest metadata from CSV into Thoth"""
    single_imprint = True
    publisher_name = "African Minds"
    publisher_url = "https://www.africanminds.co.za/"

    def run(self):
        """Process CSV and call Thoth to insert its data"""
        for row in self.data.index:
            work = self.get_work(row)
            try:
                work_id = self.thoth.work_by_doi(work['doi']).workId
            except (IndexError, AttributeError, ThothError):
                work_id = self.thoth.create_work(work, units="MM")
            print("workId: {}".format(work_id))
            work = self.thoth.work_by_id(work_id)
            self.create_contributors(row, work)

    # pylint: disable=too-many-locals
    def get_work(self, row):
        """Returns a dictionary with all attributes of a 'work'

        row: current row number

        imprint_id: previously obtained ID of this work's imprint
        """
        title = self.split_title(self.data.at[row, 'title'])
        doi = self.sanitise_string(self.data.at[row, 'doi'])
        copyright_holder = self.data.at[row, 'copyright_holder']
        work_type = self.work_types[self.data.at[row, "work_type"]]

        work_status = self.data.at[row, "work_status"] \
            if pd.notna(self.data.at[row, "work_status"]) else "Active"
        publication_date = str(self.data.at[row, "publication_date"]) \
            if pd.notna(self.data.at[row, "publication_date"]) else None
        publication_place = str(self.data.at[row, "publication_place"]) \
            if pd.notna(self.data.at[row, "publication_place"]) else None
        oclc = str(self.data.at[row, "oclc"]) \
            if pd.notna(self.data.at[row, "oclc"]) else None
        lccn = str(self.data.at[row, "lccn"]) \
            if pd.notna(self.data.at[row, "lccn"]) else None
        image_count = int(self.data.at[row, "image_count"]) \
            if pd.notna(self.data.at[row, "image_count"]) else None
        table_count = int(self.data.at[row, "table_count"]) \
            if pd.notna(self.data.at[row, "table_count"]) else None
        audio_count = int(self.data.at[row, "audio_count"]) \
            if pd.notna(self.data.at[row, "audio_count"]) else None
        video_count = int(self.data.at[row, "video_count"]) \
            if pd.notna(self.data.at[row, "video_count"]) else None
        width = int(self.data.at[row, "width (mm)"]) \
            if pd.notna(self.data.at[row, "width (mm)"]) else None
        height = int(self.data.at[row, "height (mm)"]) \
            if pd.notna(self.data.at[row, "height (mm)"]) else None
        page_count = int(self.data.at[row, "page_count"]) \
            if pd.notna(self.data.at[row, "page_count"]) else None
        page_breakdown = self.data.at[row, "page_breakdown"] \
            if pd.notna(self.data.at[row, "page_breakdown"]) else None
        edition = int(self.data.at[row, "edition"]) \
            if pd.notna(self.data.at[row, "edition"]) else 1
        license_url = \
            self.data.at[row, "license"] \
            if pd.notna(self.data.at[row, "license"]) \
            else None
        short_abstract = \
            self.data.at[row, "short_abstract"] \
            if pd.notna(self.data.at[row, "short_abstract"]) \
            else None
        long_abstract = self.data.at[row, "long_abstract"] \
            if pd.notna(self.data.at[row, "long_abstract"]) else None
        toc = self.data.at[row, "toc"] \
            if pd.notna(self.data.at[row, "toc"]) else None
        cover = self.data.at[row, "cover_url"] \
            if pd.notna(self.data.at[row, "cover_url"]) else None
        cover_caption = self.data.at[row, "cover_caption"] \
            if pd.notna(self.data.at[row, "cover_caption"]) else None
        landing = self.data.at[row, "landing_page"] \
            if pd.notna(self.data.at[row, "landing_page"]) else None

        work = {
            "workType": work_type,
            "workStatus": self.work_statuses[work_status],
            "fullTitle": title["fullTitle"],
            "title": title["title"],
            "subtitle": title["subtitle"],
            "reference": None,
            "edition": edition,
            "imprintId": self.imprint_id,
            "doi": doi,
            "publicationDate": publication_date,
            "place": publication_place,
            "width": width,
            "height": height,
            "pageCount": page_count,
            "pageBreakdown": page_breakdown,
            "imageCount": image_count,
            "tableCount": table_count,
            "audioCount": audio_count,
            "videoCount": video_count,
            "license": license_url,
            "copyrightHolder": copyright_holder,
            "landingPage": landing,
            "lccn": lccn,
            "oclc": oclc,
            "shortAbstract": short_abstract,
            "longAbstract": long_abstract,
            "generalNote": None,
            "toc": toc,
            "coverUrl": cover,
            "coverCaption": cover_caption,
        }
        return work

    def create_contributors(self, row, work):
        """Creates all contributions associated with the current work

        row: current row number

        work_id: previously obtained ID of the current work
        """
        # [(type, first_name, last_name, institution, orcid)]
        # create list of all instances of "(type, [...])" without brackets
        column = self.data.at[row, "contributions"]
        contributions = re.findall('\\((.*?)\\)', column)
        work_contributions = self.get_work_contributions(work)
        for index, contribution_string in enumerate(contributions):
            # we are now left with an ordered list of fields, the first three
            # are always present: type, first_name, last_name.
            # followed by optional institution and orcid.
            # NB. Those without an institution and an orcid will have format:
            # type, first_name, last_name, orcid
            contribution = re.split(',', contribution_string)
            contribution_type = contribution[0].strip().upper()
            contribution_type = self.contribution_types[contribution_type]
            if len(contribution) == 2:
                # There's only one name for the author, potentially an
                # organisation
                first_name = None
                last_name = contribution[1].strip()
                full_name = last_name
            else:
                first_name = contribution[1].strip()
                last_name = contribution[2].strip()
                full_name = "{} {}".format(first_name, last_name)
            orcid = None
            institution_name = None
            if len(contribution) == 5:
                institution_name = contribution[3].strip()
                orcid = contribution[4].strip()
            if len(contribution) == 4:
                # fourth attribute may be institution or orcid
                unknown = contribution[3].strip()
                try:
                    orcid = self.orcid_regex.search(unknown).group(0)
                except AttributeError:
                    institution_name = unknown
            if orcid:
                orcid = "https://orcid.org/{}".format(orcid)
            contributor = {
                "firstName": first_name,
                "lastName": last_name,
                "fullName": full_name,
                "orcid": orcid,
                "website": None
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
                "mainContribution": "true",
                "contributionOrdinal": index + 1,
                "biography": None,
                "firstName": first_name,
                "lastName": last_name,
                "fullName": full_name
            }
            contribution_id = self.thoth.create_contribution(contribution)

            if institution_name:
                # retrieve institution or create if it doesn't exist
                if institution_name in self.all_institutions:
                    institution_id = self.all_institutions[institution_name]
                else:
                    institution = {
                        "institutionName": institution_name,
                        "institutionDoi": None,
                        "ror": None,
                        "countryCode": None
                    }
                    institution_id = self.thoth.create_institution(institution)
                    self.all_institutions[institution_name] = institution_id
                affiliation = {
                    "contributionId": contribution_id,
                    "institutionId": institution_id,
                    "affiliationOrdinal": 1,
                    "position": None
                }
                self.thoth.create_affiliation(affiliation)
