"""Define a class to interface with and cache data from the eBird API."""

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

from birding.primitives import Coordinate, EBirdHotspot, EBirdSpecies
from birding.sqlite_cache import (
    get_cached,
    put_cached,
)


class EBirdAPI:
    """An interface for the eBird API."""

    def __init__(self) -> None:
        """Initialize the eBird API interface by finding the eBird API key."""
        self.ebird_api_key = EBirdAPI.get_ebird_api_key()

    @staticmethod
    def get_ebird_api_key() -> str:
        """Find the eBird API key by loading it from a .env file."""
        load_dotenv()  # Read variables from a .env file and set them in os.environ
        ebird_api_key = os.getenv("EBIRD_API_KEY")
        assert ebird_api_key, "Unable to load an eBird API key from the .env file."
        return ebird_api_key

    def retrieve_nearby_hotspots(
        self,
        coord: Coordinate,
        distance_km: int = 25,
    ) -> list[EBirdHotspot]:
        """Retrieve the eBird hotspots near the given coordinate, using the local cache or the API.

        :param coord: GPS coordinate near which hotspots are found
        :param distance_km: Maximum distance (km) from the coordinate of included hotspots
        :return: List of data structures representing eBird hotspots
        """
        rounded_coord = coord.round_decimals()
        lat_round = rounded_coord.latitude
        lng_round = rounded_coord.longitude

        payload = get_cached(
            "nearby_hotspots_cache",
            lat_round=lat_round,
            lng_round=lng_round,
            dist_km=distance_km,
        )
        if payload is None:
            print(f"Calling eBird API to identify hotspots within {distance_km} km of {coord}...")
            fetched_at_s = int(time.time())
            payload = get_nearby_hotspots(
                self.ebird_api_key,
                coord.latitude,
                coord.longitude,
                dist=distance_km,
            )
            time.sleep(1)  # Sleep to avoid exceeding rate limits

            put_cached(
                "nearby_hotspots_cache",
                payload,
                fetched_at_s,
                lat_round=lat_round,
                lng_round=lng_round,
                dist_km=distance_km,
            )
        else:
            print(f"Nearby hotspots within {distance_km} km of {coord} were already cached.")

        return [EBirdHotspot.from_json(hotspot_data) for hotspot_data in payload]

    def find_nearest_hotspot(self, coord: Coordinate) -> EBirdHotspot | None:
        """Retrieve the eBird hotspot closest to the given coordinate.

        :param coord: GPS coordinate used for hotspot lookup
        :return: Nearest eBird hotspot to the coordinate, or None if no hotspot was near enough
        """
        hotspots = []
        for dist_km in range(0, 501, 5):
            hotspots = self.retrieve_nearby_hotspots(coord, distance_km=dist_km)
            if hotspots:
                break

        if not hotspots:
            return None

        return min(hotspots, key=lambda hs: geodesic(coord, hs.location.coord).mi)

    def retrieve_region_info(self, region_code: str) -> dict[str, Any]:
        """Retrieve information for the specified eBird region.

        :param region_code: Unique identifier used by eBird for a region
        :return: Dictionary containing eBird region information
        """
        cached = get_cached("region_info_cache", region_code=region_code)
        if cached is not None:
            print(f"Region information for code '{region_code}' was already cached.")
            return cached

        print(f"Calling eBird API for region information for code '{region_code}'...")
        fetched_at_s = int(time.time())
        payload = get_region(token=self.ebird_api_key, region=region_code)
        time.sleep(1)  # Sleep to avoid exceeding rate limits

        put_cached("region_info_cache", payload, fetched_at_s, region_code=region_code)

        return payload

    def find_region_code(self, region: str, coord: Coordinate) -> str | None:
        """Find the eBird region code for the specified region.

        :param region: Text description of the region (e.g., "California, USA")
        :param coord: GPS coordinate previously found for the region
        :return: String used by eBird to identify the region, or None if lookup failed
        """
        nearest_hotspot = self.find_nearest_hotspot(coord)
        if nearest_hotspot is None:
            return None

        loc = nearest_hotspot.location
        possible_codes: list[str | None] = [loc.country_code, loc.subnat1_code, loc.subnat2_code]
        r_infos = [self.retrieve_region_info(code) for code in possible_codes if code is not None]

        closest_match = max(r_infos, key=lambda info: fuzz.ratio(region, info["result"]))
        return str(closest_match["code"])

    def retrieve_hotspots_in_region(self, region_code: str) -> list[EBirdHotspot]:
        """Retrieve the eBird hotspots in the identified region.

        :param region_code: Unique identifier for a region on eBird
        :return: List of eBird hotspots in the region
        """
        payload = get_cached("hotspots_in_region_cache", region_code=region_code)
        if payload is not None:
            print(f"eBird hotspots in region '{region_code}' were already cached.")
        else:
            print(f"Calling eBird API for hotspots in region '{region_code}'...")
            fetched_at_s = int(time.time())
            payload = get_hotspots(token=self.ebird_api_key, region=region_code)
            time.sleep(1)  # Sleep to avoid exceeding rate limits

            put_cached("hotspots_in_region_cache", payload, fetched_at_s, region_code=region_code)

        return [EBirdHotspot.from_json(data) for data in payload]

    def retrieve_species_list(self, area_code: str) -> list[str]:
        """Retrieve the bird species list for an eBird location, using the local cache or the API.

        :param area_code: Code for a country, subnational region, or eBird location
        :return: List of identifiers for bird species observed at/in the location/area
        """
        payload = get_cached("species_list_cache", area_code=area_code)
        if payload is None:
            print(f"Calling eBird API for the species list in '{area_code}'...")
            fetched_at_s = int(time.time())
            payload = get_species_list(token=self.ebird_api_key, area=area_code)
            time.sleep(1)  # Sleep to avoid exceeding rate limits

            put_cached("species_list_cache", payload, fetched_at_s, area_code=area_code)
        else:
            print(f"Bird species list for '{area_code}' was already cached.")

        return payload

    def retrieve_species_taxons(self, species_codes: list[str]) -> dict[str, EBirdSpecies]:
        """Retrieve the taxonomy entries for the requested species.

        :param species_codes: List of eBird species codes identifying the entries to retrieve
        :return: Dictionary mapping species codes to species data structures
        """
        species_map: dict[str, EBirdSpecies | None] = {}
        for code in species_codes:
            cached = get_cached("species_taxonomy_cache", species_id=code)
            species_map[code] = None if cached is None else EBirdSpecies.from_json(cached)

        missing_species = [code for code, data in species_map.items() if data is None]
        if missing_species:
            print(f"Calling eBird API for the taxonomy entry of {len(missing_species)} species...")
            fetched_at_s = int(time.time())
            payload = get_taxonomy(self.ebird_api_key, species=missing_species)
            time.sleep(1)  # Sleep to avoid exceeding rate limits

            for raw_data in payload:
                s_code = raw_data.get("speciesCode")
                if s_code is None:
                    print(f"Unable to find species code in data: {raw_data}")
                    continue

                put_cached("species_taxonomy_cache", raw_data, fetched_at_s, species_id=s_code)
                species_map[s_code] = EBirdSpecies.from_json(raw_data)

        output_data = {code: data for code, data in species_map.items() if data is not None}
        for species_code in species_codes:
            assert species_code in output_data, f"Entry for species '{species_code}' is missing."

        return output_data

    def find_species_in_region(self, region_code: str) -> list[EBirdSpecies]:
        """Retrieve the list of species ever seen in the specified eBird region.

        :param region_code: Unique identifier for the region on eBird
        :return: List of species data structures
        """
        species_in_region = self.retrieve_species_list(region_code)
        species_taxons = self.retrieve_species_taxons(species_in_region)
        return [species_taxons[s] for s in species_in_region]
