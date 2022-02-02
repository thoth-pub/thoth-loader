"""Load African Minds metadata into Thoth"""
import json
import re

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
                work_id = self.thoth.work_by_doi(work.doi).workId
            except (IndexError, AttributeError, ThothError):
                work_id = self.thoth.create_work(work)
            print("workId: {}".format(work_id))

            self.create_contributors(row, work_id)

    # pylint: disable=too-many-locals
    def get_work(self, row):
        """Returns a dictionary with all attributes of a 'work'

        row: current row number

        imprint_id: previously obtained ID of this work's imprint
        """
        title = self.split_title(self.data.at[row, 'title'])
        doi = self.data.at[row, 'doi']
        copyright_holder = self.data.at[row, 'copyright_holder']
        work_type = self.work_types[self.data.at[row, "work_type"]]

        work_status = self.work_statuses[self.data.at[row, "work_status"]] \
            if self.data.at[row, "work_status"] else "ACTIVE"
        publication_date = str(self.data.at[row, "publication_date"]) \
            if self.data.at[row, "publication_date"] else None
        publication_place = str(self.data.at[row, "publication_place"]) \
            if self.data.at[row, "publication_place"] else None
        oclc = str(self.data.at[row, "oclc"]) \
            if self.data.at[row, "oclc"] else None
        lccn = str(self.data.at[row, "lccn"]) \
            if self.data.at[row, "lccn"] else None
        image_count = int(self.data.at[row, "image_count"]) \
            if self.data.at[row, "image_count"] else None
        table_count = int(self.data.at[row, "table_count"]) \
            if self.data.at[row, "table_count"] else None
        audio_count = int(self.data.at[row, "audio_count"]) \
            if self.data.at[row, "audio_count"] else None
        video_count = int(self.data.at[row, "video_count"]) \
            if self.data.at[row, "video_count"] else None
        width = int(self.data.at[row, "width (mm)"]) \
            if self.data.at[row, "width (mm)"] else None
        height = int(self.data.at[row, "height (mm)"]) \
            if self.data.at[row, "height (mm)"] else None
        page_count = int(self.data.at[row, "page_count"]) \
            if self.data.at[row, "page_count"] else None
        page_breakdown = self.data.at[row, "page_breakdown"] \
            if self.data.at[row, "page_breakdown"] else None
        edition = int(self.data.at[row, "edition"]) \
            if self.data.at[row, "edition"] else 1
        license_url = \
            self.data.at[row, "license"] \
            if self.data.at[row, "license"] \
            else None
        short_abstract = \
            self.data.at[row, "short_abstract"] \
            if self.data.at[row, "short_abstract)"] \
            else None
        long_abstract = self.data.at[row, "long_abstract"] \
            if self.data.at[row, "long_abstract"] else None
        toc = self.data.at[row, "toc"] \
            if self.data.at[row, "toc"] else None
        cover = self.data.at[row, "cover_url"] \
            if self.data.at[row, "cover_url"] else None
        cover_caption = self.data.at[row, "cover_caption"] \
            if self.data.at[row, "cover_caption"] else None
        landing = self.data.at[row, "landing_page"] \
            if self.data.at[row, "landing_page"] else None

        work = {
            "workType": work_type,
            "workStatus": work_status,
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

    def create_contributors(self, row, work_id):
        """Creates all contributions associated with the current work

        row: current row number

        work_id: previously obtained ID of the current work
        """
        # create cache of all existing contributors
        for c in self.thoth.contributors():
            self.all_contributors[c.fullName] = c.contributorId
            if c.orcid:
                self.all_contributors[c.orcid] = c.contributorId

        # [(type, first_name, last_name, institution, orcid)]
        # create list of all instances of "(type, [...])" without brackets
        column = self.data.at["contributions"]
        contributions = re.findall('\\((.*?)\\)', column)
        for index, contribution_string in enumerate(contributions):
            # we are now left with an ordered list of fields, the first three
            # are always present: type, first_name, last_name.
            # followed by optional institution and orcid.
            # NB. Those without an institution and an orcid will have format:
            # type, first_name, last_name, orcid
            contribution = re.split(',', contribution_string)
            contribution_type = self.contribution_types[contribution[0].strip()]
            first_name = contribution[1].strip()
            last_name = contribution[2].strip()
            full_name = "{} {}".format(first_name, last_name)
            orcid = None
            institution = None
            if len(contribution) == 5:
                institution = contribution[3].strip()
                orcid = contribution[4].strip()
            if len(contribution) == 4:
                # fourth attribute may be institution or orcid
                unknown = contribution[3].strip()
                try:
                    orcid = self.orcid_regex.search(unknown).group(0)
                except AttributeError:
                    institution = unknown
            orcid = "https://orcid.org/{}".format(orcid)
            contributor = {
                "firstName": first_name,
                "lastName": last_name,
                "fullName": full_name,
                "orcid": orcid,
                "website": None
            }
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
                "workId": work_id,
                "contributorId": contributor_id,
                "contributionType": contribution_type,
                "mainContribution": True,
                "contributionOrdinal": index + 1,
                "biography": None,
                "firstName": first_name,
                "lastName": last_name,
                "fullName": full_name
            }
            contribution_id = self.thoth.create_contribution(contribution)
