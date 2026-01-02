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
    # New control parameters
    start_date: str = None  # ISO format: YYYY-MM-DD
    end_date: str = None    # ISO format: YYYY-MM-DD
    yoy_growth: float = None  # Year-over-year growth rate (0.15 = 15%)
    enable_seasonality: bool = None  # Enable/disable seasonal patterns
    enable_holidays: bool = None  # Enable/disable holiday effects
    avg_items_per_transaction: int = None  # Average items per transaction
    peak_hour_multiplier: float = None  # Multiplier for peak hours (1.0 = no effect)

    def __post_init__(self):
        from datetime import datetime

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

        # New parameter defaults
        if self.start_date is None:
            self.start_date = "2020-01-01"

        if self.end_date is None:
            self.end_date = datetime.now().strftime("%Y-%m-%d")

        if self.yoy_growth is None:
            self.yoy_growth = 0.15  # 15% year-over-year growth

        if self.enable_seasonality is None:
            self.enable_seasonality = True

        if self.enable_holidays is None:
            self.enable_holidays = True

        if self.avg_items_per_transaction is None:
            self.avg_items_per_transaction = 3

        if self.peak_hour_multiplier is None:
            self.peak_hour_multiplier = 1.0

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
