"""Load African Minds metadata into Thoth"""

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
