"""Primitive data structures exportable to and importable from YAML."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


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
    def from_api_json(cls, data: dict[str, Any]) -> EBirdSpecies:
        """Construct an eBird species from a dictionary of JSON data from the eBird API."""
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
