"""Define an interface to a local SQLite database that caches previous API calls."""

import json
import sqlite3
import time
from pathlib import Path
from typing import Any

from birding.primitives import Coordinate

DB_PATH = Path(__file__).parent.parent.parent / "data/cache.sqlite"
"""Relative filepath to the SQLite database cache."""

SECONDS_PER_DAY = 60 * 60 * 24

HOTSPOT_TTL_S = 30 * SECONDS_PER_DAY
"""Time to live (seconds) for cached hotspot data (i.e., how long it's considered valid)."""

GEOCODE_TTL_S = 90 * SECONDS_PER_DAY
"""Time to live (in seconds) for cached geocode data (3 months because unlikely to change)."""

SPECIES_LIST_TTL_S = 7 * SECONDS_PER_DAY
"""Time to live (in seconds) for cached species lists (1 week for semi-frequent updates)."""

SPECIES_TAXONOMY_TTL_S = 90 * SECONDS_PER_DAY
"""Time to live (seconds) for cached eBird species taxonomy entries (3 months; changes slowly)."""

PHOTO_OBSERVATIONS_TTL_S = 7 * SECONDS_PER_DAY
"""Time to live (seconds) for cached iNaturalist observations providing species photos."""


def get_conn() -> sqlite3.Connection:
    """Create and return an active connection to the SQLite database."""
    if not DB_PATH.exists():
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        DB_PATH.touch(exist_ok=False)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # Return dictionary-like rows instead of tuples
    return conn


def init_db() -> None:
    """Initialize the database in case it doesn't exist."""
    with get_conn() as conn:
        conn.executescript(
            """
            --- Nearby eBird hotspots cache
            CREATE TABLE IF NOT EXISTS nearby_hotspots_cache (
              lat_round     REAL NOT NULL,      --- Rounded to 4 decimal places
              lng_round     REAL NOT NULL,
              dist_km       INTEGER NOT NULL,
              response_json TEXT NOT NULL,
              fetched_at    INTEGER NOT NULL,
              expires_at    INTEGER NOT NULL,
              PRIMARY KEY (lat_round, lng_round, dist_km)
            );

            --- GeoPy/Nominatim geocode cache
            CREATE TABLE IF NOT EXISTS geocode_cache (
              provider      TEXT NOT NULL,
              query         TEXT NOT NULL,
              response_json TEXT NOT NULL,
              fetched_at    INTEGER NOT NULL,
              expires_at    INTEGER NOT NULL,
              PRIMARY KEY (provider, query)
            );

            --- Location eBird species lists cache
            CREATE TABLE IF NOT EXISTS species_list_cache (
              area_code     TEXT NOT NULL,
              response_json TEXT NOT NULL,
              fetched_at    INTEGER NOT NULL,
              expires_at    INTEGER NOT NULL,
              PRIMARY KEY (area_code)
            );

            --- eBird species taxonomy entries cache
            CREATE TABLE IF NOT EXISTS species_taxonomy_cache (
              species_id    TEXT NOT NULL,
              response_json TEXT NOT NULL,
              fetched_at    INTEGER NOT NULL,
              expires_at    INTEGER NOT NULL,
              PRIMARY KEY (species_id)
            );

            --- iNaturalist observations with photos cache
            CREATE TABLE IF NOT EXISTS observation_photos_cache (
              taxon_name    TEXT NOT NULL,
              day_of_month  INTEGER NOT NULL,
              response_json TEXT NOT NULL,
              fetched_at    INTEGER NOT NULL,
              expires_at    INTEGER NOT NULL,
              PRIMARY KEY (taxon_name, day_of_month)
            );
            """,
        )


def _round_coordinate(coord: Coordinate, n_digits: int = 4) -> Coordinate:
    """Round the given coordinate to the given number of decimals and return the result."""
    lat_rounded = round(coord.latitude, ndigits=n_digits)
    lng_rounded = round(coord.longitude, ndigits=n_digits)
    return Coordinate(lat_rounded, lng_rounded)


def get_cached_nearby_hotspots(coord: Coordinate, distance_km: int) -> list[dict[str, Any]] | None:
    """Retrieve the nearby eBird hotspots for the given coordinate, if they are cached.

    :param coord: GPS coordinate for which nearby hotspots are retrieved
    :param distance_km: Maximum distance (km) from the coordinate of included hotspots
    :return: List of raw JSON data for the hotspots if cached, else None
    """
    now_s = int(time.time())
    r_coord = _round_coordinate(coord)

    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT response_json
            FROM nearby_hotspots_cache
            WHERE lat_round = ? AND lng_round = ? AND dist_km = ? AND expires_at >= ?
            """,
            (r_coord.latitude, r_coord.longitude, distance_km, now_s),
        ).fetchone()

    return None if row is None else json.loads(row["response_json"])


def put_cached_nearby_hotspots(
    coord: Coordinate,
    distance_km: int,
    payload: list[dict[str, Any]],
    fetched_at_s: int,
    ttl_s: int = HOTSPOT_TTL_S,
) -> None:
    """Save the given nearby hotspot data for a coordinate into the SQLite database.

    :param coord: GPS coordinate used as a key for the 'nearby hotspots' query
    :param distance_km: Maximum distance (km) from the coordinate of included hotspots
    :param payload: JSON data resulting from the eBird API call
    :param fetched_at_s: Unix time (in seconds; rounded) at which the data was fetched
    :param ttl_s: Duration (in seconds; rounded) that the data is considered valid
    """
    rounded_lat, rounded_lng = _round_coordinate(coord)
    json_dump = json.dumps(payload)
    expires_at_s = fetched_at_s + ttl_s

    with get_conn() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO nearby_hotspots_cache
            (lat_round, lng_round, dist_km, response_json, fetched_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (rounded_lat, rounded_lng, distance_km, json_dump, fetched_at_s, expires_at_s),
        )


def get_cached_geocode(query: str, provider: str = "Nominatim") -> dict[str, Any] | None:
    """Retrieve the geocode data for the given query, if it is cached.

    :param query: Text description of a location (e.g., "Providence, Rhode Island")
    :param provider: Geocoding service backend used for the query (defaults to 'Nominatim')
    :return: Raw JSON geocode data if cached, else None
    """
    now_s = int(time.time())

    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT response_json
            FROM geocode_cache
            WHERE provider = ? AND query = ? AND expires_at >= ?
            """,
            (provider, query, now_s),
        ).fetchone()

    return None if row is None else json.loads(row["response_json"])


def put_cached_geocode(
    query: str,
    payload: dict[str, Any],
    fetched_at_s: int,
    ttl_s: int = GEOCODE_TTL_S,
    provider: str = "Nominatim",
) -> None:
    """Save the given geocode data into the SQLite database.

    :param query: Text description of the location that was geocoded
    :param payload: JSON data sent as a response to the query
    :param fetched_at_s: Unix time (in seconds; rounded) at which the data was fetched
    :param ttl_s: Duration (in seconds; rounded) that the data is considered valid
    :param provider: Geocoding service backend used for the query (defaults to 'Nominatim')
    """
    json_dump = json.dumps(payload)
    expires_at_s = fetched_at_s + ttl_s

    with get_conn() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO geocode_cache
            (provider, query, response_json, fetched_at, expires_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (provider, query, json_dump, fetched_at_s, expires_at_s),
        )


def get_cached_species_list(area_code: str) -> list[str] | None:
    """Retrieve the eBird species list for the specified area, if the list is cached.

    :param area_code: Code for a country, subnational region, or eBird location
    :return: List of bird species identifiers if cached, else None
    """
    now_s = int(time.time())

    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT response_json
            FROM species_list_cache
            WHERE area_code = ? AND expires_at >= ?
            """,
            (area_code, now_s),
        ).fetchone()

    return None if row is None else json.loads(row["response_json"])


def put_cached_species_list(
    area_code: str,
    payload: list[str],
    fetched_at_s: int,
    ttl_s: int = SPECIES_LIST_TTL_S,
) -> None:
    """Save the given species list for an eBird area or location into the SQLite database.

    :param area_code: Code for a country, subnational region, or eBird location
    :param payload: JSON data resulting from the eBird API call
    :param fetched_at_s: Unix time (in seconds; rounded) at which the data was fetched
    :param ttl_s: Duration (in seconds; rounded) that the data is considered valid
    """
    json_dump = json.dumps(payload)
    expires_at_s = fetched_at_s + ttl_s

    with get_conn() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO species_list_cache
            (area_code, response_json, fetched_at, expires_at)
            VALUES (?, ?, ?, ?)
            """,
            (area_code, json_dump, fetched_at_s, expires_at_s),
        )


def get_cached_taxonomy_entry(species_id: str) -> dict[str, Any] | None:
    """Retrieve the specified eBird taxonomy species entry, if it is cached.

    :param species_id: eBird species code
    :return: Dicionary of species taxonomy data if cached, else None
    """
    now_s = int(time.time())

    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT response_json
            FROM species_taxonomy_cache
            WHERE species_id = ? AND expires_at >= ?
            """,
            (species_id, now_s),
        ).fetchone()

    return None if row is None else json.loads(row["response_json"])


def put_cached_taxonomy_entry(
    species_id: str,
    payload: dict[str, Any],
    fetched_at_s: int,
    ttl_s: int = SPECIES_TAXONOMY_TTL_S,
) -> None:
    """Save the given eBird taxonomy species entry into the SQLite database.

    :param species_id: eBird species code used to index the data
    :param payload: JSON data resulting from the eBird API call
    :param fetched_at_s: Unix time (in seconds; rounded) at which the data was fetched
    :param ttl_s: Duration (in seconds; rounded) that the data is considered valid
    """
    json_dump = json.dumps(payload)
    expires_at_s = fetched_at_s + ttl_s

    with get_conn() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO species_taxonomy_cache
            (species_id, response_json, fetched_at, expires_at)
            VALUES (?, ?, ?, ?)
            """,
            (species_id, json_dump, fetched_at_s, expires_at_s),
        )


def get_cached_photo_observations(taxon_name: str, day_of_month: int) -> dict[str, Any] | None:
    """Retrieve cached observations for the specified taxon on the requested day of the month.

    :param taxon_name: Scientific or common name of a taxon
    :param day_of_month: Day of month on which the cached observations occurred
    :return: Requested observations of the taxon if cached, else None
    """
    now_s = int(time.time())

    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT response_json
            FROM observation_photos_cache
            WHERE taxon_name = ? AND day_of_month = ? AND expires_at >= ?
            """,
            (taxon_name, day_of_month, now_s),
        ).fetchone()

    return None if row is None else json.loads(row["response_json"])


def put_cached_photo_observations(
    taxon_name: str,
    day_of_month: int,
    payload: dict[str, Any],
    fetched_at_s: int,
    ttl_s: int = PHOTO_OBSERVATIONS_TTL_S,
) -> None:
    """Save the given iNaturalist photo observations data into the SQLite database.

    :param taxon_name: Scientific or common name of a taxon
    :param day_of_month: Day of month on which the cached observations occurred
    :param payload: JSON data sent as a response to the query
    :param fetched_at_s: Unix time (in seconds; rounded) at which the data was fetched
    :param ttl_s: Duration (in seconds; rounded) that the data is considered valid
    """
    json_dump = json.dumps(payload, default=str)
    expires_at_s = fetched_at_s + ttl_s

    with get_conn() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO observation_photos_cache
            (taxon_name, day_of_month, response_json, fetched_at, expires_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (taxon_name, day_of_month, json_dump, fetched_at_s, expires_at_s),
        )


def main() -> None:
    """Demo the SQLite database."""
    init_db()


if __name__ == "__main__":
    main()
