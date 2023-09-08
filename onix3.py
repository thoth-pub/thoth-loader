"""Parse an ONIX 3.0 Product"""
import logging
from onix.book.v3_0.reference.strict import Product
from bookloader import BookLoader


class Onix3Record:
    """Generic logic to extract data from an ONIX 3.0 product record"""

    def __init__(self, product: Product):
        self._product = product

    def title(self):
        title_element = self._product.descriptive_detail.title_detail[0].title_element[0]
        try:
            title = title_element.choice[0].value
            subtitle = title_element.choice[1].value
        except (ValueError, AttributeError):
            title = title_element.choice[0].value
            subtitle = None
        return BookLoader.sanitise_title(title, subtitle)

    def doi(self):
        dois = [ident.idvalue.value for ident in self._product.product_identifier
                if ident.product_idtype.value.value == "06"]
        try:
            return f"https://doi.org/{dois[0]}"
        except IndexError:
            logging.error(f"No DOI found: {self._product.record_reference}")
            raise

    def isbn(self):
        isbns = [ident.idvalue.value for ident in self._product.product_identifier
                 if ident.product_idtype.value.value == "15"]
        try:
            return BookLoader.sanitise_isbn(isbns[0])
        except IndexError:
            logging.error("No ISBN found")
            raise

    def work_type(self):
        contributors = self._product.descriptive_detail.contributor_or_contributor_statement_or_no_contributor
        roles = [role.value.value for contributor in contributors for role in contributor.contributor_role]
        if roles[0] == "B01":
            return "EDITED_BOOK"
        else:
            return "MONOGRAPH"

    def long_abstract(self):
        abstracts = self._product.collateral_detail.text_content
        long_abstract = [text.text[0].content[0] for text in abstracts if text.text_type.value.value == "03"]
        return long_abstract[0]

    def reference(self):
        return self._product.record_reference.value

    def license(self):
        licenses = self._product.descriptive_detail.epub_license.epub_license_expression
        cc = [cc.epub_license_expression_link.value for cc in licenses
              if cc.epub_license_expression_type.value.value == "02"]
        return cc[0]

    def cover_url(self):
        resources = self._product.collateral_detail.supporting_resource
        cover = [resource.resource_version[0].resource_link[0].value for resource in resources
                 if resource.resource_content_type.value.value == "01"]
        return cover[0]

    def publication_place(self):
        return self._product.publishing_detail.city_of_publication[0].value

    def publication_date(self):
        return BookLoader.sanitise_date(self._product.publishing_detail.publishing_date[0].date.value)

    def oapen_url(self):
        locations = self._product.product_supply[0].supply_detail
        oapen = [location.supplier.website[0].website_link[0].value for location in locations
                 if location.supplier.supplier_identifier_or_supplier_name[0].value == "DOAB Library"]
        return oapen[0]

    def page_count(self):
        page_count = [extent.extent_value.value for extent in self._product.descriptive_detail.extent
                      if extent.extent_type.value.value == "00"]
        return int(page_count[0])

    def contributors(self):
        return self._product.descriptive_detail.contributor_or_contributor_statement_or_no_contributor

    def language_code(self):
        return self._product.descriptive_detail.language[0].language_code.value.value.upper()

    def bic_codes(self):
        subjects = self._product.descriptive_detail.subject
        return [subject.subject_code_or_subject_heading_text[0].value for subject in subjects
                if subject.subject_scheme_identifier.value.value == "12"]

    def keywords(self):
        subjects = self._product.descriptive_detail.subject
        return [subject.subject_code_or_subject_heading_text[0].value for subject in subjects
                if subject.subject_scheme_identifier.value.value == "20"]

