"""Define functions to interface with and cache data from the eBird API."""

import os
import time
from typing import Any

from dotenv import load_dotenv
from ebird.api.requests import get_nearby_hotspots, get_species_list, get_taxonomy

from birding.primitives import Coordinate
from birding.sqlite_cache import (
    get_cached_nearby_hotspots,
    get_cached_species_list,
    get_cached_taxonomy_entry,
    put_cached_nearby_hotspots,
    put_cached_species_list,
    put_cached_taxonomy_entry,
)


def get_ebird_api_key() -> str:
    """Find the eBird API key by loading it from a .env file."""
    load_dotenv()  # Read variables from a .env file and set them in os.environ
    ebird_api_key = os.getenv("EBIRD_API_KEY")
    assert ebird_api_key, "Unable to load an eBird API key from the .env file."
    return ebird_api_key


def retrieve_nearby_hotspots(
    ebird_api_key: str,
    coord: Coordinate,
    distance_km: int = 25,
) -> list[dict[str, Any]]:
    """Retrieve the eBird hotspots near the given coordinate, using the local cache or the API.

    :param ebird_api_key: eBird API key, used if the requested data is not cached locally
    :param coord: GPS coordinate near which hotspots are found
    :param distance_km: Maximum distance (km) of included hotspots from the coordinate
    :return: List of dictionaries of JSON data for each hotspot
    """
    cached = get_cached_nearby_hotspots(coord, distance_km)
    if cached is not None:
        print(f"Nearby hotspots for {coord} were already cached.")
        return cached

    # Fallback to a real eBird API call
    print(f"Calling eBird API to identify hotspots near {coord}...")
    fetched_at_s = int(time.time())
    payload = get_nearby_hotspots(ebird_api_key, coord.latitude, coord.longitude, dist=distance_km)
    time.sleep(1)  # Sleep to avoid exceeding rate limits

    put_cached_nearby_hotspots(
        coord=coord,
        distance_km=distance_km,
        payload=payload,
        fetched_at_s=fetched_at_s,
    )

    return payload


def retrieve_species_list(ebird_api_key: str, area_code: str) -> list[str]:
    """Retrieve the bird species list for an eBird location, using the local cache or the API.

    :param ebird_api_key: eBird API key, used if the requested data is not cached locally
    :param area_code: Code for a country, subnational region, or eBird location
    :return: List of identifiers for bird species observed at/in the location/area
    """
    cached = get_cached_species_list(area_code=area_code)
    if cached is not None:
        print(f"Bird species list for '{area_code}' was already cached.")
        return cached

    print(f"Calling eBird API for the species list at '{area_code}'...")
    fetched_at_s = int(time.time())
    payload = get_species_list(token=ebird_api_key, area=area_code)
    time.sleep(1)  # Sleep to avoid exceeding rate limits

    put_cached_species_list(area_code=area_code, payload=payload, fetched_at_s=fetched_at_s)

    return payload


def retrieve_taxonomy_entries(ebird_api_key: str, species: list[str]) -> list[dict[str, Any]]:
    """Retrieve the eBird taxonomy entries for the requested species.

    :param ebird_api_key: eBird API key, used if the requested data is not cached locally
    :param species: List of eBird species codes identifying the entries to retrieve
    :return: List of dictionaries containing eBird taxonomy entry data
    """
    needs_lookup = [s_id for s_id in species if get_cached_taxonomy_entry(s_id) is None]
    if needs_lookup:
        print(f"Calling eBird API for the taxonomy entry of {len(needs_lookup)} species...")
        fetched_at_s = int(time.time())
        payload = get_taxonomy(token=ebird_api_key, category="species", species=needs_lookup)
        time.sleep(1)  # Sleep to avoid exceeding rate limits

        for species_entry_data in payload:
            entry_id = species_entry_data.get("speciesCode")
            if entry_id is None:
                raise KeyError(f"Unable to find species code for data: {species_entry_data}")

            put_cached_taxonomy_entry(entry_id, species_entry_data, fetched_at_s)

    output_data = []
    for species_id in species:
        cached = get_cached_taxonomy_entry(species_id)
        if cached is None:
            print(f"Unable to retrieve eBird taxonomy entry for species: {species_id}")
            continue
        output_data.append(cached)

    return output_data
