"""Shared HTML parsing utilities for judgment metadata"""

from lxml import html
from datetime import datetime


def parse_decision_date_from_html(raw_html):
    """
    Extract decision date from judgment HTML.

    Args:
        raw_html: HTML string containing case details

    Returns:
        tuple: (decision_date_str, decision_year) where:
            - decision_date_str: Date string in DD-MM-YYYY format or None
            - decision_year: Integer year or None
    """
    if not raw_html:
        return None, None

    try:
        tree = html.fromstring(raw_html)

        # Find the case details section
        case_details_elements = tree.xpath('//strong[@class="caseDetailsTD"]')
        if not case_details_elements:
            return None, None

        # Extract decision date
        decision_dates = case_details_elements[0].xpath(
            './/span[contains(text(), "Decision Date")]/following-sibling::font/text()'
        )

        if decision_dates:
            date_str = decision_dates[0].strip()

            # Parse date format DD-MM-YYYY to get year
            try:
                parsed_date = datetime.strptime(date_str, "%d-%m-%Y")
                return date_str, parsed_date.year
            except ValueError:
                # If parsing fails, return the string but no year
                return date_str, None

        return None, None

    except Exception:
        return None, None


def parse_case_details_from_html(raw_html):
    """
    Extract all case details from judgment HTML.
    This is the complete parsing logic used by process_metadata.py

    Args:
        raw_html: HTML string containing case details

    Returns:
        dict: Dictionary containing cnr, date_of_registration, decision_date,
              disposal_nature, court fields. Missing fields are empty strings or None.
    """
    case_details = {
        "cnr": "",
        "date_of_registration": "",
        "decision_date": None,
        "disposal_nature": "",
        "court": "",
    }

    if not raw_html:
        return case_details

    try:
        tree = html.fromstring(raw_html)

        # Find the case details section
        case_details_elements = tree.xpath('//strong[@class="caseDetailsTD"]')
        if not case_details_elements:
            return case_details

        element = case_details_elements[0]

        # Extract CNR
        try:
            case_details["cnr"] = element.xpath(
                './/span[contains(text(), "CNR")]/following-sibling::font/text()'
            )[0].strip()
        except (IndexError, KeyError):
            pass

        # Extract date of registration
        try:
            case_details["date_of_registration"] = element.xpath(
                './/span[contains(text(), "Date of registration")]/following-sibling::font/text()'
            )[0].strip()
        except (IndexError, KeyError):
            pass

        # Extract decision date
        try:
            case_details["decision_date"] = element.xpath(
                './/span[contains(text(), "Decision Date")]/following-sibling::font/text()'
            )[0].strip()
        except (IndexError, KeyError):
            pass

        # Extract disposal nature
        try:
            case_details["disposal_nature"] = element.xpath(
                './/span[contains(text(), "Disposal Nature")]/following-sibling::font/text()'
            )[0].strip()
        except (IndexError, KeyError):
            pass

        # Extract court
        try:
            case_details["court"] = (
                element.xpath('.//span[contains(text(), "Court")]/text()')[0]
                .split(":")[1]
                .strip()
            )
        except (IndexError, KeyError):
            pass

        return case_details

    except Exception:
        return case_details
