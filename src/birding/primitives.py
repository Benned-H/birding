"""Primitive, few-dependency data structures for the codebase."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, NamedTuple


class Coordinate(NamedTuple):
    """A GPS coordinate comprised of a latitude and longitude."""

    latitude: float
    longitude: float

    def _key(self) -> tuple[float, float]:
        """Define a hash key to uniquely identify the coordinate."""
        rounded = self.round_decimals(n_digits=4)
        return (rounded.latitude, rounded.longitude)

    def __eq__(self, other: object) -> bool:
        """Evaluate whether another coordinate is (approximately) equal to this one."""
        if not isinstance(other, Coordinate):
            raise NotImplementedError

        return self._key() == other._key()

    def __hash__(self) -> int:
        """Compute a hash value for the coordinate."""
        return hash(self._key())

    def __str__(self) -> str:
        """Return a human-readable representation of the coordinate."""
        return f"({self.latitude:.4f}, {self.longitude:.4f})"

    @classmethod
    def from_geocode_data(cls, data: dict[str, Any]) -> Coordinate:
        """Construct a coordinate from a dictionary of geocode data about a location."""
        return Coordinate(float(data["lat"]), float(data["lon"]))

    def round_decimals(self, n_digits: int = 4) -> Coordinate:
        """Round the coordinate to the given number of decimals and return the result."""
        lat_rounded = round(self.latitude, ndigits=n_digits)
        lng_rounded = round(self.longitude, ndigits=n_digits)
        return Coordinate(lat_rounded, lng_rounded)


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

    def _key(self) -> tuple:
        """Define a hash key to uniquely identify the eBird location."""
        return (
            self.id,
            self.name,
            self.coord._key(),  # noqa: SLF001 (allow one _key method to access another)
            self.country_code,
            self.subnat1_code,
            self.subnat1_name,
            self.subnat2_code,
            self.subnat2_name,
        )

    def __eq__(self, other: object) -> bool:
        """Evaluate whether another eBird location is equal to this one."""
        if not isinstance(other, EBirdLocation):
            raise NotImplementedError

        return self._key() == other._key()

    def __hash__(self) -> int:
        """Compute a hash value for the eBird location."""
        return hash(self._key())

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
    all_time_species: int | None

    def _key(self) -> tuple:
        """Define a hash key to uniquely identify the eBird hotspot."""
        return (self.location._key(), self.all_time_species)  # noqa: SLF001

    def __eq__(self, other: object) -> bool:
        """Evaluate whether another eBird hotspot is equal to this one."""
        if not isinstance(other, EBirdHotspot):
            raise NotImplementedError

        return self._key() == other._key()

    def __hash__(self) -> int:
        """Compute a hash value for the eBird hotspot."""
        return hash(self._key())

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> EBirdHotspot:
        """Construct an eBird hotspot from a dictionary of JSON data."""
        location = EBirdLocation.from_json(data)
        all_time_species = data.get("numSpeciesAllTime")
        return EBirdHotspot(location, all_time_species)


@dataclass(frozen=True)
class Species:
    """A species within the Zoological Code taxonomy.

    Reference: https://en.wikipedia.org/wiki/International_Code_of_Zoological_Nomenclature
    """

    common_name: str
    """Colloquial name used for the species in everyday life."""

    specific_name: str
    """Specific epithet (i.e., second part of the scientific name) of the species."""

    generic_name: str
    """Genus (i.e., first part of the scientific name) of the species."""

    family_common_name: str
    """Colloquial name used for the species' family."""

    family: str
    """Family taxon of the species."""

    order: str
    """Order taxon of the species."""

    @property
    def scientific_name(self) -> str:
        """Retrieve the binomial name of the species."""
        return f"{self.generic_name.title()} {self.specific_name.lower()}"

    @classmethod
    def from_yaml(cls, yaml_data: dict[str, str]) -> Species:
        """Construct a Species instance from data loaded from YAML."""
        specific, generic = yaml_data["scientific_name"].split()
        return Species(
            common_name=yaml_data["common_name"],
            specific_name=specific,
            generic_name=generic,
            family_common_name=yaml_data["family_common_name"],
            family=yaml_data["family"],
            order=yaml_data["order"],
        )

    def to_yaml(self) -> dict[str, str]:
        """Convert the species into a dictionary of YAML data."""
        return {
            "common_name": self.common_name,
            "scientific_name": self.scientific_name,
            "family_common_name": self.family_common_name,
            "family": self.family,
            "order": self.order,
        }


@dataclass(frozen=True)
class EBirdSpecies(Species):
    """A taxonomic species of bird with an associated eBird species code."""

    ebird_species_code: str
    """Unique alphanumeric identifier for the species used by eBird."""

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> EBirdSpecies:
        """Construct an eBird species from a dictionary of JSON data from the eBird API."""
        if not isinstance(data, dict):
            raise TypeError(f"Unexpected data type: {data} (type {type(data)})")

        scientific_name_parts = str(data["sciName"]).split()
        generic = scientific_name_parts[0]
        specific = " ".join(scientific_name_parts[1:])

        return EBirdSpecies(
            common_name=data["comName"],
            specific_name=specific,
            generic_name=generic,
            family_common_name=data["familyComName"],
            family=data["familySciName"],
            order=data["order"],
            ebird_species_code=data["speciesCode"],
        )

    @classmethod
    def from_yaml(cls, yaml_data: dict[str, str]) -> EBirdSpecies:
        """Construct an EBirdSpecies instance from data loaded from YAML."""
        species = Species.from_yaml(yaml_data)
        return EBirdSpecies(
            **asdict(species),
            ebird_species_code=yaml_data["ebird_species_code"],
        )

    def to_yaml(self) -> dict[str, str]:
        """Convert the eBird species into a dictionary of YAML data."""
        species_data = super().to_yaml()
        species_data["ebird_species_code"] = self.ebird_species_code
        return species_data
