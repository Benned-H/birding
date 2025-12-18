"""Primitive, few-dependency data structures for the codebase."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, NamedTuple


class Coordinate(NamedTuple):
    """A GPS coordinate comprised of a latitude and longitude."""

    latitude: float
    longitude: float

    def __str__(self) -> str:
        """Return a human-readable representation of the coordinate."""
        return f"({self.latitude:.4f}, {self.longitude:.4f})"

    @classmethod
    def from_geocode_data(cls, data: dict[str, Any]) -> Coordinate:
        """Construct a coordinate from a dictionary of geocode data about a location."""
        return Coordinate(float(data["lat"]), float(data["lon"]))


@dataclass(frozen=True)
class EBirdLocation:
    """A location in the hierarchical eBird location system."""

    id: str
    name: str
    coord: Coordinate
    country_code: str
    subnat1_code: str | None
    subnat1_name: str | None
    subnat2_code: str | None
    subnat2_name: str | None

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> EBirdLocation:
        """Construct an eBird location from a dictionary of JSON data."""
        return EBirdLocation(
            id=data["locId"],
            name=data["locName"],
            coord=Coordinate(data["lat"], data["lng"]),
            country_code=data["countryCode"],
            subnat1_code=data.get("subnational1Code"),
            subnat1_name=data.get("subnational1Name"),
            subnat2_code=data.get("subnational2Code"),
            subnat2_name=data.get("subnational2Name"),
        )


@dataclass(frozen=True)
class EBirdHotspot:
    """A location in the eBird system with a history of bird observations."""

    location: EBirdLocation
    all_time_species: int

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> EBirdHotspot:
        """Construct an eBird hotspot from a dictionary of JSON data."""
        location = EBirdLocation.from_json(data)
        return EBirdHotspot(location=location, all_time_species=data["numSpeciesAllTime"])
