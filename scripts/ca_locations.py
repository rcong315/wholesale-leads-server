# California locations list
# This is a comprehensive list of major cities across California
# Supports both city names and ZIP codes for scraping

CALIFORNIA_LOCATIONS = [
    # "Temecula",
    # "Murrieta",
    # TODO: "Moreno Valley",
    "San Jacinto",
    "Hemet",
    "Corona",
    "Norco",
    "Riverside",
    "Ontario",
    "Fontana",
    "Pomona",
    "West Covina",
    "Whittier",
    "Anaheim",
    "Santa Ana",
    "Tustin",
    "Gardena",
    "Carson",
    "Bakersfield",
    "Arvin",
    "Lamont",
    "Rosedale",
    "Tulare",
    "Visalia",
    "Delano",
    "Earlimart",
    "McFarland",
    "Palmdale",
    "Lancaster",
    "Oxnard",
]


def get_locations():
    """Return the list of California locations"""
    return CALIFORNIA_LOCATIONS


def get_location_count():
    """Return the total number of locations"""
    return len(CALIFORNIA_LOCATIONS)


if __name__ == "__main__":
    print(f"Total California locations: {get_location_count()}")
    print("First 10 locations:", CALIFORNIA_LOCATIONS[:10])
    print("Last 10 locations:", CALIFORNIA_LOCATIONS[-10:])
