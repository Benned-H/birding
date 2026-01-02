"""Define functions to access data from the iNaturalist API."""

import time
from typing import Any

from pyinaturalist import get_observations

from birding.sqlite_cache import get_cached_photo_observations, put_cached_photo_observations

VALID_PHOTO_LICENSES = [
    "CC-BY",
    "CC-BY-NC",
    "CC-BY-ND",
    "CC-BY-SA",
    "CC-BY-NC-ND",
    "CC-BY-NC-SA",
    "CC0",
]
"""Copyright licenses acceptable for images used by this project.

Conditions on these licenses include:
    CC BY: Provide attribution to the original creator
    CC BY-NC: CC BY + only noncommercial purposes
    CC BY-ND: CC BY + cannot modify the material in any way
    CC BY-SA: CC BY + derivatives must use the same license
    CC0: Public domain; the work may be used for any purpose
"""


def retrieve_photo_observations(taxon_name: str, day_of_month: int = 1) -> dict[str, Any]:
    """Retrieve iNaturalist observations with photos for the requested species/family/taxon group.

    :param taxon_name: Scientific or common name of a taxonomic group
    :param day_of_month: Day of month on which downloaded observations occurred
    :return: Collection of iNaturalist observations with photos
    """
    cached = get_cached_photo_observations(taxon_name=taxon_name, day_of_month=day_of_month)
    if cached is not None:
        print(f"iNaturalist photo observations for '{taxon_name}' were already cached.")
        return cached

    # Otherwise, fallback to calling the iNaturalist API
    print(f"Calling iNaturalist API for photo observations of '{taxon_name}'...")

    fetched_at_s = int(time.time())
    payload = get_observations(
        taxon_name=taxon_name,
        identified=True,
        captive=False,
        photos=True,
        quality_grade="research",
        photo_license=VALID_PHOTO_LICENSES,
        popular=True,  # Favorited by at least one user
        day=day_of_month,
    )
    put_cached_photo_observations(taxon_name, day_of_month, payload, fetched_at_s)

    return payload
