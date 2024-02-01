import sys
import os

from get_company_code import get_companies, get_companies_to_keep
from get_location_site import get_adrc, get_t001w
from get_buildings import get_buildings, get_sites


def main():
    """True for local ssl cert"""

    # T001
    companies = get_companies(True)
    print(companies)

    # T001Z
    companies_to_keep = get_companies_to_keep(True)
    print(companies_to_keep)

    # T001W
    sites = get_t001w(True)
    print(sites)

    # ADRC
    adrc = get_adrc(True)
    print(adrc)

    # ZTXXETBLDG
    buildings = get_buildings(True)
    print(buildings)

    # T001W
    building_sites = get_sites(True)
    print(building_sites)


if __name__ == "__main__":
    os.environ["SECRETS"] = "insert arn here"

    main()
