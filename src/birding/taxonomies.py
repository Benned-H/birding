"""Define data structures to represent biological taxonomies."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from birding.primitives import Species


@dataclass
class TaxonomicClass:
    """A class-level taxonomic group of related taxonomic orders."""

    class_name: str
    """Latin name of the class."""

    orders: dict[str, TaxonomicOrder] = field(default_factory=dict)
    """Orders in the class."""

    def to_yaml(self) -> dict[str, Any]:
        """Convert the class into a dictionary of YAML data."""
        orders_list = [o.to_yaml() for o in self.orders.values()]
        return {"class_name": self.class_name, "orders": orders_list}

    def insert_species(self, s: Species) -> None:
        """Insert the given species into the class."""
        if s.order not in self.orders:
            self.orders[s.order] = TaxonomicOrder(s.order)
        self.orders[s.order].insert_species(s)


@dataclass
class TaxonomicOrder:
    """An order-level taxonomic group of related taxonomic families."""

    order_name: str
    """Latin name of the order."""

    families: dict[str, TaxonomicFamily] = field(default_factory=dict)
    """Families in the order."""

    def to_yaml(self) -> dict[str, Any]:
        """Convert the order into a dictionary of YAML data."""
        families_list = [f.to_yaml() for f in self.families.values()]
        return {"order_name": self.order_name, "families": families_list}

    def insert_species(self, s: Species) -> None:
        """Insert the given species into the order."""
        if s.family not in self.families:
            self.families[s.family] = TaxonomicFamily(s.family)
        self.families[s.family].insert_species(s)


@dataclass
class TaxonomicFamily:
    """A family-level taxonomic group of related genera."""

    family_name: str
    """Latin name of the family."""

    genera: dict[str, TaxonomicGenus] = field(default_factory=dict)
    """Genera in the family."""

    def to_yaml(self) -> dict[str, Any]:
        """Convert the family into a dictionary of YAML data."""
        genera_list = [g.to_yaml() for g in self.genera.values()]
        return {"family_name": self.family_name, "genera": genera_list}

    def insert_species(self, s: Species) -> None:
        """Insert the given species into the family."""
        if s.generic_name not in self.genera:
            self.genera[s.generic_name] = TaxonomicGenus(s.generic_name)
        self.genera[s.generic_name].insert_species(s)


@dataclass
class TaxonomicGenus:
    """A genus-level taxonomic group of related species."""

    generic_name: str
    """The first part of the binomial name of all species in the genus."""

    species: dict[str, TaxonomicSpecies] = field(default_factory=dict)
    """Species in the genus."""

    def to_yaml(self) -> dict[str, Any]:
        """Convert the genus into a dictionary of YAML data."""
        species_list = [s.to_yaml() for s in self.species.values()]
        return {"generic_name": self.generic_name, "species": species_list}

    def insert_species(self, s: Species) -> None:
        """Insert the given species into the genus."""
        self.species[s.specific_name] = TaxonomicSpecies(s.specific_name)


@dataclass
class TaxonomicSpecies:
    """A basic taxonomic unit of classification for organisms."""

    specific_name: str
    """The second part of the species' binomial name."""

    def to_yaml(self) -> dict[str, str]:
        """Convert the species into a dictionary of YAML data."""
        return {"specific_name": self.specific_name}
