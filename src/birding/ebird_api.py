"""Define functions to interface with and cache data from the eBird API."""

import os
import time
from typing import Any

from dotenv import load_dotenv
from ebird.api.requests import (
    get_hotspots,
    get_nearby_hotspots,
    get_region,
    get_species_list,
    get_taxonomy,
)
from geopy.distance import geodesic
from thefuzz import fuzz

from birding.primitives import Coordinate, EBirdHotspot
from birding.sqlite_cache import (
    get_cached,
    get_cached_nearby_hotspots,
    get_cached_region_info,
    get_cached_species_list,
    get_cached_taxonomy_entry,
    put_cached,
    put_cached_nearby_hotspots,
    put_cached_region_info,
    put_cached_species_list,
    put_cached_taxonomy_entry,
)
from birding.yaml_primitives import EBirdSpecies


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


def retrieve_region_info(region_code: str) -> dict[str, Any]:
    """Retrieve the information for the specified eBird region.

    :param region_code: Unique identifier for the region on eBird
    :return: Dictionary containing eBird region information
    """
    ebird_api_key = get_ebird_api_key()

    cached = get_cached_region_info(region_code)
    if cached is not None:
        print(f"Region information for code '{region_code}' was already cached.")
        return cached

    print(f"Calling eBird API for region information for code '{region_code}'...")
    fetched_at_s = int(time.time())
    payload = get_region(token=ebird_api_key, region=region_code)
    time.sleep(1)  # Sleep to avoid exceeding rate limits

    put_cached_region_info(region_code, payload, fetched_at_s)

    return payload


def retrieve_hotspots_in_region(region_code: str) -> list[EBirdHotspot]:
    """Retrieve the eBird hotspots in the identified region.

    :param region_code: Unique identifier for a region on eBird
    :return: List of eBird hotspots in the region
    """
    payload = get_cached("hotspots_in_region_cache", region_code=region_code)
    if payload is not None:
        print(f"eBird hotspots in region '{region_code}' were already cached.")
    else:
        ebird_api_key = get_ebird_api_key()

        print(f"Calling eBird API for hotspots in region '{region_code}'...")
        fetched_at_s = int(time.time())
        payload = get_hotspots(token=ebird_api_key, region=region_code)
        time.sleep(1)  # Sleep to avoid exceeding rate limits

        put_cached("hotspots_in_region_cache", payload, fetched_at_s, region_code=region_code)

    return [EBirdHotspot.from_json(data) for data in payload]


def find_nearest_hotspot(coord: Coordinate) -> EBirdHotspot | None:
    """Retrieve the eBird hotspot closest to the given coordinate.

    :param coord: GPS coordinate used for hotspot lookup
    :return: Nearest eBird hotspot to the coordinate, or None if no hotspot was near enough
    """
    ebird_api_key = get_ebird_api_key()

    hotspots_data = []
    for dist_km in range(0, 501, 5):
        hotspots_data = retrieve_nearby_hotspots(ebird_api_key, coord, dist_km)
        if hotspots_data:
            break

    if not hotspots_data:
        return None

    hotspots = [EBirdHotspot.from_json(data) for data in hotspots_data]
    return min(hotspots, key=lambda hs: geodesic(coord, hs.location.coord).mi)


def find_ebird_region_code(region: str, coord: Coordinate) -> None:
    """Find the eBird region code for the specified region.

    :param region: Text description of the region (e.g., "California, USA")
    :param coord: GPS coordinate found for the region
    :return: String used by eBird to identify the region
    """
    nearest_hotspot = find_nearest_hotspot(coord)
    if nearest_hotspot is None:
        return None

    loc = nearest_hotspot.location
    candidate_codes: list[str | None] = [loc.country_code, loc.subnat1_code, loc.subnat2_code]
    region_infos = [retrieve_region_info(code) for code in candidate_codes if code is not None]
    best_region_info = max(region_infos, key=lambda info: fuzz.ratio(region, info["result"]))

    return best_region_info["code"]


def find_species_in_region(region_code: str) -> list[EBirdSpecies]:
    """Retrieve the list of species even seen in the specified eBird region.

    :param region_code: Unique identifier for the region on eBird
    :return: List of species data structures
    """
    ebird_api_key = get_ebird_api_key()

    species_in_region = retrieve_species_list(ebird_api_key, region_code)
    ebird_taxonomy_data = retrieve_taxonomy_entries(ebird_api_key, species_in_region)

    return [EBirdSpecies.from_api_json(data) for data in ebird_taxonomy_data]
