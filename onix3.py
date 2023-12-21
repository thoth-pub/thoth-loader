"""Parse an ONIX 3.0 Product"""
import re
import logging
from onix.book.v3_0.reference.strict import Product, Contributor, NamesBeforeKey, KeyNames, ProfessionalAffiliation, BiographicalNote, \
TitlePrefix, TitleWithoutPrefix, Subtitle, EditionNumber, PersonName, ProfessionalPosition, Affiliation
from bookloader import BookLoader


class Onix3Record:
    """Generic logic to extract data from an ONIX 3.0 product record"""

    def __init__(self, product: Product):
        self._product = product

    def title(self):
        title_element = self._product.descriptive_detail.title_detail[0].title_element[0]
        prefix = None
        title = None
        subtitle = None
        for title_part in title_element.choice:
            if type(title_part) is TitlePrefix:
                prefix = title_part.value
            if type(title_part) is TitleWithoutPrefix:
                title = title_part.value
            if type(title_part) is Subtitle:
                subtitle = title_part.value
        if title is None:
            # title may be contained in another element type, e.g. <TitleText>
            try:
                title = title_element.choice[0].value
                subtitle = title_element.choice[1].value
            except (ValueError, AttributeError, IndexError):
                title = title_element.choice[0].value
        if prefix is not None:
            title = ' '.join([prefix, title])
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
        try:
            return [text.text[0].content[0] for text in self._product.collateral_detail.text_content
                    if text.text_type.value.value == "03"][0]
        except IndexError:
            return None

    def toc(self):
        try:
            return [text.text[0].content[0] for text in self._product.collateral_detail.text_content
                    if text.text_type.value.value == "04"][0]
        except IndexError:
            return None

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
        try:
            return [resource.resource_version[0].resource_link[0].value for resource in resources
                    if resource.resource_content_type.value.value == "01"][0]
        except IndexError:
            return None

    def publication_place(self):
        return self._product.publishing_detail.city_of_publication[0].value

    def publication_date(self):
        return BookLoader.sanitise_date(self._product.publishing_detail.publishing_date[0].date.value)

    def work_status(self):
        return self._product.publishing_detail.publishing_status.value.value

    def oapen_url(self):
        locations = self._product.product_supply[0].supply_detail
        oapen = [location.supplier.website[0].website_link[0].value for location in locations
                 if location.supplier.supplier_identifier_or_supplier_name[0].value == "DOAB Library"]
        return oapen[0]

    def page_count(self):
        page_count = [extent.extent_value.value for extent in self._product.descriptive_detail.extent
                      if extent.extent_type.value.value in ["00", "11"]]
        try:
            return int(page_count[0])
        except IndexError:
            return None

    def illustration_count(self):
        number_of_illustrations = self._product.descriptive_detail.number_of_illustrations
        if number_of_illustrations is not None:
            return number_of_illustrations.value
        else:
            try:
                # Get total number of illustrations from <IllustrationsNote>, which is of the form e.g. 10 bw illus"""
                illustrations_note = self._product.descriptive_detail.illustrations_note[0].content[0]
                numbers = re.findall(r'\d+', illustrations_note)
                total = sum(int(number) for number in numbers)
                return total
            except IndexError:
                return None

    def edition_number(self):
        edition_number = [e.value for e in self._product.descriptive_detail.choice
                          if type(e) is EditionNumber]
        try:
            return int(edition_number[0])
        except IndexError:
            return None

    def contributors(self):
        return [c for c in self._product.descriptive_detail.contributor_or_contributor_statement_or_no_contributor
                if type(c) is Contributor]

    def language_code(self):
        return self._product.descriptive_detail.language[0].language_code.value.value.upper()

    def language_codes_and_roles(self):
        languages = self._product.descriptive_detail.language
        language_codes_and_roles = []
        unsupported = next((x for x in languages if x.language_role.value.value not in ["01", "02"]), None)
        if unsupported is not None:
            raise KeyError("Unsupported language role: %s" % unsupported.language_role.value.value)
        if next((x for x in languages if x.language_role.value.value == "02"), None) is not None:
            language_codes_and_roles = [(language.language_code.value.value.upper(), "TRANSLATED_FROM")
                                        for language in languages if language.language_role.value.value == "02"]
            language_codes_and_roles.extend([(language.language_code.value.value.upper(), "TRANSLATED_INTO")
                                             for language in languages if language.language_role.value.value == "01"])
        else:
            language_codes_and_roles = [(language.language_code.value.value.upper(), "ORIGINAL")
                                        for language in languages]
        return language_codes_and_roles

    def bic_codes(self):
        subjects = self._product.descriptive_detail.subject
        return [subject.subject_code_or_subject_heading_text[0].value for subject in subjects
                if subject.subject_scheme_identifier.value.value in ["12", "13", "14", "15"]]

    def bisac_codes(self):
        subjects = self._product.descriptive_detail.subject
        return [subject.subject_code_or_subject_heading_text[0].value for subject in subjects
                if subject.subject_scheme_identifier.value.value in ["10", "11", "22"]]

    def custom_codes(self):
        subjects = self._product.descriptive_detail.subject
        return [subject.subject_code_or_subject_heading_text[0].value for subject in subjects
                if subject.subject_scheme_identifier.value.value == "23"]

    def keywords(self):
        subjects = self._product.descriptive_detail.subject
        return [subject.subject_code_or_subject_heading_text[0].value for subject in subjects
                if subject.subject_scheme_identifier.value.value == "20"]

    def keywords_from_text(self):
        """Used on subjects where SubjectHeadingText is used instead of SubjectCode"""
        return [keyword for all_keywords in self.keywords() for keyword
                in all_keywords.replace(',', ';').replace('; ', ';').split(';')]

    def thema_codes(self):
        subjects = self._product.descriptive_detail.subject
        return [subject.subject_code_or_subject_heading_text[0].value for subject in subjects
                if subject.subject_scheme_identifier.value.value in ["93", "94", "95", "96", "97", "98", "99"]]

    def prices(self):
        return [(price.currency_code.value.value, str(price.price_amount.value))
                for product_supply in self._product.product_supply
                for supply_detail in product_supply.supply_detail
                for price in supply_detail.unpriced_item_type_or_price
                if hasattr(price, 'price_amount') and price.price_amount is not None \
                    and str(price.price_amount.value) != "0.00"]

    def dimensions(self):
        return [(m.measure_type.value.value, m.measure_unit_code.value.value, str(m.measurement.value))
                for m in self._product.descriptive_detail.measure]

    def related_biblio_work_id(self):
        related = [ident.idvalue.value for ident in self._product.related_material.related_work[0].work_identifier
                   if ident.idtype_name.value == "Biblio Work ID"]
        return related[0]

    def related_system_internal_identifier(self):
        related = [ident.idvalue.value for ident in self._product.related_material.related_work[0].work_identifier
                   if ident.idtype_name is not None and ident.idtype_name.value == "system-internal-identifier"]
        return related[0]

    def product_type(self):
        try:
            product_type = self._product.descriptive_detail.product_form_detail[0].value.value
            # Check that this ONIX code is one we can unambiguously convert to a Thoth publication type
            BookLoader.publication_types[product_type]
            return product_type
        except (IndexError, KeyError):
            try:
                product_type = self._product.descriptive_detail.product_form_description[0].value
                # Check that this ONIX code is one we can unambiguously convert to a Thoth publication type
                BookLoader.publication_types[product_type]
                return product_type
            except (IndexError, KeyError):
                return self._product.descriptive_detail.product_form.value.value

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
    def get_person_name(contributor: Contributor):
        return [name.value for name in contributor.choice
                if type(name) is PersonName][0]

    @staticmethod
    def get_affiliation(contributor: Contributor):
        logging.info(contributor.choice_1)
        return [affiliation.value
                for professional_affiliation in contributor.choice_1
                for affiliation in getattr(professional_affiliation, 'professional_position_or_affiliation', [])]

    @staticmethod
    def get_affiliations_with_positions(contributor: Contributor):
        affiliations = [affiliation
                        for affiliation in contributor.choice_1
                        if type(affiliation) is ProfessionalAffiliation]
        affiliations_with_positions = []
        for affiliation in affiliations:
            try:
                position = [position.value
                            for position in getattr(affiliation, 'professional_position_or_affiliation', [])
                            if type(position) is ProfessionalPosition][0]
            except IndexError:
                position = None
            try:
                institution = [insitution.value
                            for insitution in getattr(affiliation, 'professional_position_or_affiliation', [])
                            if type(insitution) is Affiliation][0]
            except IndexError:
                institution = None
            affiliations_with_positions.append((position, institution))
        return affiliations_with_positions

    @staticmethod
    def get_biography(contributor: Contributor):
        try:
            return [content
                    for biographical_note in contributor.choice_1
                    for content in getattr(biographical_note, 'content', [])][0]
        except IndexError:
            return None

    @staticmethod
    def get_orcid(contributor: Contributor):
        try:
            orcid_digits = [name_identifier.idvalue.value
                            for name_identifier in contributor.name_identifier
                            if name_identifier.name_idtype.value.value == "21"][0]
        except IndexError:
            return None
        orcid_hyphenated = '-'.join(orcid_digits[i:i+4] for i in range(0, len(orcid_digits), 4))
        return BookLoader.sanitise_orcid(orcid_hyphenated)
