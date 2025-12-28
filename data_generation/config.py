"""
Configuration for data generation.
"""

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass
class DataGenConfig:
    seed: int
    num_stores: int
    num_products: int
    num_transactions: int
    data_dir: Path
    landing_dir: Path = None
    compression: str = None
    compression_level: int = None
    locale: str = None
    categories: list = None
    regions: list = None

    def __post_init__(self):
        config_path = Path(__file__).parent.parent / "config.json"
        with open(config_path) as f:
            global_config = json.load(f)

        if self.landing_dir is None:
            self.landing_dir = Path(global_config["landing_dir"])

        if self.compression is None:
            self.compression = global_config["compression"]

        if self.compression_level is None:
            self.compression_level = global_config["compression_level"]

        if self.locale is None:
            self.locale = "de_DE"

        if self.categories is None:
            self.categories = [
                "Obst & Gemüse",
                "Brot & Backwaren",
                "Molkereiprodukte",
                "Fleisch & Wurst",
                "Tiefkühlkost",
                "Getränke",
                "Süßwaren & Snacks",
                "Konserven & Fertiggerichte",
                "Haushaltswaren",
                "Drogerie & Kosmetik",
                "Aktionswaren",
            ]

        if self.regions is None:
            self.regions = [
                "Baden-Württemberg",
                "Bayern",
                "Berlin",
                "Brandenburg",
                "Bremen",
                "Hamburg",
                "Hessen",
                "Mecklenburg-Vorpommern",
                "Niedersachsen",
                "Nordrhein-Westfalen",
                "Rheinland-Pfalz",
                "Saarland",
                "Sachsen",
                "Sachsen-Anhalt",
                "Schleswig-Holstein",
                "Thüringen",
            ]

    def ensure_directories(self):
        config_path = Path(__file__).parent.parent / "config.json"
        with open(config_path) as f:
            global_config = json.load(f)

        self.landing_dir.mkdir(parents=True, exist_ok=True)
        Path(global_config["raw_dir"]).mkdir(parents=True, exist_ok=True)
        Path(global_config["cleaned_dir"]).mkdir(parents=True, exist_ok=True)
        Path(global_config["aggregated_dir"]).mkdir(parents=True, exist_ok=True)
