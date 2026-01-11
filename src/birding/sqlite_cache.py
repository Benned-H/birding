"""Define an interface to a local SQLite database that caches previous API calls."""

import json
import sqlite3
import time
from pathlib import Path
from typing import Any

DB_PATH = Path(__file__).parent.parent.parent / "data/cache.sqlite"
"""Relative filepath to the SQLite database cache."""

SECONDS_PER_DAY = 60 * 60 * 24


SPECIES_TAXONOMY_TTL_S = 90 * SECONDS_PER_DAY
"""Time to live (seconds) for cached eBird species taxonomy entries (3 months; changes slowly)."""

PHOTO_OBSERVATIONS_TTL_S = 7 * SECONDS_PER_DAY
"""Time to live (seconds) for cached iNaturalist observations providing species photos."""


VALID_TABLES: dict[str, set[str]] = {
    "nearby_hotspots_cache": {"lat_round", "lng_round", "dist_km"},  # Nearby eBird hotspots
    "geocode_cache": {"provider", "query"},  # GeoPy/Nominatim geocodes
    "species_list_cache": {"area_code"},  # eBird species lists for locations
    "species_taxonomy_cache": {"species_id"},  # eBird species taxonomy entries
    "observation_photos_cache": {"taxon_name", "day_of_month"},  # iNaturalist photo observations
    "region_info_cache": {"region_code"},  # Information for eBird regions
    "hotspots_in_region_cache": {"region_code"},  # Hotspots in an eBird region
}
"""Valid table schemas used in the SQLite database."""


TABLE_TTL_S: dict[str, int] = {
    "nearby_hotspots_cache": 7 * SECONDS_PER_DAY,  # Update hotspots near a location weekly
    "geocode_cache": 90 * SECONDS_PER_DAY,  # 3 months because unlikely to change
    "species_list_cache": 7 * SECONDS_PER_DAY,  # 1 week for semi-frequent updates
    "species_taxonomy_cache": 90 * SECONDS_PER_DAY,  # 3 months; taxonomy entries change slowly
    "region_info_cache": 30 * SECONDS_PER_DAY,  # Update eBird region info after a month
    "hotspots_in_region_cache": 7 * SECONDS_PER_DAY,  # Check for new hotspots weekly
}
"""Time to live (seconds) for data cached in each SQLite table."""

TABLE_PAYLOAD_TYPES: dict[str, type] = {
    "nearby_hotspots_cache": list,  # List of dictionaries of hotspot data
    "geocode_cache": dict,
    "species_list_cache": list,
    "species_taxonomy_cache": dict,
    "region_info_cache": dict,
    "hotspots_in_region_cache": list,
}
"""Expected Python type of a payload loaded from each table in the SQLite database."""


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

            --- eBird region information cache
            CREATE TABLE IF NOT EXISTS region_info_cache (
              region_code   TEXT NOT NULL,
              response_json TEXT NOT NULL,
              fetched_at    INTEGER NOT NULL,
              expires_at    INTEGER NOT NULL,
              PRIMARY KEY (region_code)
            );

            --- Hotspots in an eBird region cache
            CREATE TABLE IF NOT EXISTS hotspots_in_region_cache (
              region_code   TEXT NOT NULL,
              response_json TEXT NOT NULL,
              fetched_at    INTEGER NOT NULL,
              expires_at    INTEGER NOT NULL,
              PRIMARY KEY (region_code)
            );
            """,
        )


def get_cached(table_name: str, **kwargs: str | float) -> dict[str, Any] | list[Any] | None:
    """Retrieve data from the SQLite database, if it is cached.

    :param table_name: Identifier for the relevant SQLite table
    :param kwargs: Maps database key names to their values
    :return: Dictionary or list of the requested data if cached, else None
    :raises ValueError: If the given table name or column names are invalid
    """
    if table_name not in VALID_TABLES or table_name not in TABLE_PAYLOAD_TYPES:
        raise ValueError(f"Invalid table name: '{table_name}'.")

    valid_columns = VALID_TABLES[table_name]
    for key in kwargs:
        if key not in valid_columns:
            raise ValueError(f"Invalid column '{key}' for table '{table_name}'.")

    now_s = int(time.time())
    query_keys = kwargs.keys()
    query_key_qmarks = " AND ".join(f"{k} = ?" for k in query_keys)
    query_values = kwargs.values()

    with get_conn() as conn:
        # We've already validated the table name and column names against VALID_TABLES
        row = conn.execute(
            f"""
            SELECT response_json
            FROM {table_name}
            WHERE {query_key_qmarks} AND expires_at >= ?
            """,  # noqa: S608
            (*query_values, now_s),
        ).fetchone()

    if row is None:
        return None

    loaded_payload = json.loads(row["response_json"])
    if not isinstance(loaded_payload, TABLE_PAYLOAD_TYPES[table_name]):
        raise TypeError(
            f"Unexpected Python type for payload loaded from table '{table_name}'.\n"
            f"\tExpected type: {TABLE_PAYLOAD_TYPES[table_name]}\n"
            f"\tFound type: {type(loaded_payload)}",
        )

    return loaded_payload


def put_cached(
    table_name: str,
    payload: dict[str, Any],
    fetched_at_s: int,
    **kwargs: str | float,
) -> None:
    """Save the given data into the specified SQLite database.

    :param table_name: Identifier for the relevant SQLite table
    :param payload: JSON data to be saved in the database
    :param fetched_at_s: Unix time (in seconds; rounded) at which the data was fetched
    :raises ValueError: If the given table name or column names are invalid
    """
    if table_name not in VALID_TABLES or table_name not in TABLE_TTL_S:
        raise ValueError(f"Invalid table name: '{table_name}'.")

    valid_columns = VALID_TABLES[table_name]
    for key in kwargs:
        if key not in valid_columns:
            raise ValueError(f"Invalid column '{key}' for table '{table_name}'.")

    ttl_s = TABLE_TTL_S[table_name]
    json_dump = json.dumps(payload, default=str)
    expires_at_s = fetched_at_s + ttl_s

    query_keys = kwargs.keys()
    keys_str = "(" + ", ".join(query_keys) + ", response_json, fetched_at, expires_at)"

    query_values = kwargs.values()
    values_str = "VALUES (" + ", ".join(["?"] * len(query_values)) + ", ?, ?, ?)"

    query_values = kwargs.values()

    with get_conn() as conn:
        conn.execute(
            f"""
            INSERT OR REPLACE INTO {table_name}
            {keys_str}
            {values_str}
            """,
            (*query_values, json_dump, fetched_at_s, expires_at_s),
        )


def get_cached_geocode(query: str, provider: str = "Nominatim") -> dict[str, Any] | None:
    """Retrieve the geocode data for the given query, if it is cached.

    :param query: Text description of a location (e.g., "Providence, Rhode Island")
    :param provider: Geocoding service backend used for the query (defaults to 'Nominatim')
    :return: Raw JSON geocode data if cached, else None
    """
    return get_cached("geocode_cache", provider=provider, query=query)


def put_cached_geocode(
    query: str,
    payload: dict[str, Any],
    fetched_at_s: int,
    provider: str = "Nominatim",
) -> None:
    """Save the given geocode data into the SQLite database.

    :param query: Text description of the location that was geocoded
    :param payload: JSON data sent as a response to the query
    :param fetched_at_s: Unix time (in seconds; rounded) at which the data was fetched
    :param provider: Geocoding service backend used for the query (defaults to 'Nominatim')
    """
    put_cached("geocode_cache", payload, fetched_at_s, provider=provider, query=query)


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
