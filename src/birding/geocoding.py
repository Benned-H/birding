"""Define functions that support geocoding location names or descriptions."""

import time
from typing import Any

from geopy.geocoders import Nominatim

from birding.primitives import Coordinate
from birding.sqlite_cache import get_cached_geocode, put_cached_geocode


def retrieve_geocode(query: str, user_agent: str = "Geocode Cacher") -> dict[str, Any] | None:
    """Retrieve geocoded data for the given text query, using the local cache or a geocoder API.

    :param query: Text description of a location (e.g., "Minneapolis, MN")
    :param user_agent: Name used to identify the program when calling the geocoder API
    :return: Dictionary of JSON data for the geocoded location, or None if no geocode exists
    """
    cached = get_cached_geocode(query=query)
    if cached is not None:
        print(f"Geocode for query '{query}' was already cached.")
        return cached

    # Fallback to a Nominatim API call
    print(f"Querying Nominatim for geocode data on '{query}'.")
    fetched_at_s = int(time.time())

    geolocator = Nominatim(user_agent=user_agent, timeout=15)
    location = geolocator.geocode(query)
    time.sleep(1)  # Sleep to avoid exceeding rate limits

    if location is None:
        return None

    payload: dict[str, Any] = location.raw

    put_cached_geocode(query=query, payload=payload, fetched_at_s=fetched_at_s)

    return payload


def find_coordinate(location: str) -> Coordinate | None:
    """Retrieve or look up the GPS coordinate for the specified location.

    :param location: Text description of a location (e.g., "Minneapolis, MN")
    :return: Coordinate data structure, or None if geocoding failed
    """
    raw_data = retrieve_geocode(query=location)
    return None if raw_data is None else Coordinate.from_geocode_data(raw_data)
