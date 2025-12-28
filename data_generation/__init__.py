"""
Retail Lakehouse - Data Generation Module

Deterministic synthetic data generation for German retail environment.
Uses Faker with German locale and fixed seed for reproducibility.
"""

from .config import DataGenConfig
from .generators import ProductGenerator, StoreGenerator, TransactionGenerator

__version__ = "0.1.0"
__all__ = [
    "StoreGenerator",
    "ProductGenerator",
    "TransactionGenerator",
    "DataGenConfig",
]
