"""Parse an ONIX 3.0 Product"""
import re
import logging
from onix.book.v3_0.reference.strict import Product, Contributor, NamesBeforeKey, KeyNames, ProfessionalAffiliation, BiographicalNote
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
        except (ValueError, AttributeError, IndexError):
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
        roles = [role.value.value for contributor in contributors
                 for role in getattr(contributor, 'contributor_role', [])]
        if roles[0] == "B01":
            return "EDITED_BOOK"
        else:
            return "MONOGRAPH"

    def long_abstract(self):
        long_abstract = [text.text[0].content[0] for text in self._product.collateral_detail.text_content
                         if text.text_type.value.value == "03"]
        return long_abstract[0]

    def toc(self):
        long_abstract = [text.text[0].content[0] for text in self._product.collateral_detail.text_content
                         if text.text_type.value.value == "04"]
        return long_abstract[0]

    def reference(self):
        return self._product.record_reference.value

    def license(self):
        try:
            return [cc.epub_license_expression_link.value
                    for cc in self._product.descriptive_detail.epub_license.epub_license_expression
                    if cc.epub_license_expression_type.value.value in ["01", "02"]][0]
        except AttributeError:
            return None

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
                      if extent.extent_type.value.value in ["00", "11"]]
        return int(page_count[0])

    def illustration_count(self):
        """Get total number of illustrations from <IllustrationsNote>, which is of the form e.g. 10 bw illus"""
        try:
            illustrations_note = self._product.descriptive_detail.illustrations_note[0].content[0]
            numbers = re.findall(r'\d+', illustrations_note)
            total = sum(int(number) for number in numbers)
            return total
        except IndexError:
            return None

    def contributors(self):
        return [c for c in self._product.descriptive_detail.contributor_or_contributor_statement_or_no_contributor
                if type(c) is Contributor]

    def language_code(self):
        return self._product.descriptive_detail.language[0].language_code.value.value.upper()

    def bic_codes(self):
        subjects = self._product.descriptive_detail.subject
        return [subject.subject_code_or_subject_heading_text[0].value for subject in subjects
                if subject.subject_scheme_identifier.value.value in ["12", "13"]]

    def bisac_codes(self):
        subjects = self._product.descriptive_detail.subject
        return [subject.subject_code_or_subject_heading_text[0].value for subject in subjects
                if subject.subject_scheme_identifier.value.value == "10"]

    def keywords(self):
        subjects = self._product.descriptive_detail.subject
        return [subject.subject_code_or_subject_heading_text[0].value for subject in subjects
                if subject.subject_scheme_identifier.value.value == "20"]

    def keywords_from_text(self):
        """Used on subjects where SubjectHeadingText is used instead of SubjectCode"""
        return [keyword for all_keywords in self.keywords() for keyword in all_keywords.split('; ')]

    def thema_codes(self):
        subjects = self._product.descriptive_detail.subject
        return [subject.subject_code_or_subject_heading_text[0].value for subject in subjects
                if subject.subject_scheme_identifier.value.value in ["93", "94", "95", "96", "97", "98", "99"]]

    def prices(self):
        return [(price.currency_code.value.value, str(price.price_amount.value))
                for product_supply in self._product.product_supply
                for supply_detail in product_supply.supply_detail
                for price in supply_detail.unpriced_item_type_or_price
                if hasattr(price, 'price_amount') and str(price.price_amount.value) != "0.00"]

    def related_biblio_work_id(self):
        related = [ident.idvalue.value for ident in self._product.related_material.related_work[0].work_identifier
                   if ident.idtype_name.value == "Biblio Work ID"]
        return related[0]

    def product_type(self):
        try:
            return self._product.descriptive_detail.product_form_detail[0].value.value
        except IndexError:
            return self._product.descriptive_detail.product_form_description[0].value

    def available_content_url(self):
        try:
            return [website_link.value
                    for publisher in self._product.publishing_detail.imprint_or_publisher
                    for website in getattr(publisher, 'website', [])
                    for website_link in website.website_link][0]
        except IndexError:
            return None

    @staticmethod
    def get_key_names(contributor: Contributor):
        return [name.value for name in contributor.choice
                if type(name) is KeyNames][0]

    @staticmethod
    def get_names_before_key(contributor: Contributor):
        return [name.value for name in contributor.choice
                if type(name) is NamesBeforeKey][0]

    @staticmethod
    def get_affiliation(contributor: Contributor):
        logging.info(contributor.choice_1)
        return [affiliation.value
                for professional_affiliation in contributor.choice_1
                for affiliation in getattr(professional_affiliation, 'professional_position_or_affiliation', [])]

    @staticmethod
    def get_biography(contributor: Contributor):
        return [content
                for biographical_note in contributor.choice_1
                for content in getattr(biographical_note, 'content', [])][0]
