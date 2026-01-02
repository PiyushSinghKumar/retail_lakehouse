"""
Deterministic data generators for German retail environment.
"""

from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
from config import DataGenConfig
from faker import Faker


class StoreGenerator:

    def __init__(self, config: DataGenConfig):
        self.config = config
        self.fake = Faker(config.locale)
        Faker.seed(config.seed)
        np.random.seed(config.seed)

    def generate(self, num_stores: int) -> pl.DataFrame:
        stores = []
        for store_id in range(1, num_stores + 1):
            region = np.random.choice(self.config.regions)

            store = {
                "store_id": store_id,
                "store_name": f"ALDI SÜD {self.fake.city()}",
                "street": self.fake.street_address(),
                "city": self.fake.city(),
                "postal_code": self.fake.postcode(),
                "region": region,
                "latitude": float(self.fake.latitude()),
                "longitude": float(self.fake.longitude()),
                "store_size_sqm": np.random.randint(800, 1500),
                "opening_date": self.fake.date_between(
                    start_date="-10y", end_date="-1y"
                ),
            }
            stores.append(store)

        return pl.DataFrame(stores)

    def save(self, df: pl.DataFrame, filename: str = "stores.parquet"):
        output_path = self.config.landing_dir / filename
        df.write_parquet(
            output_path,
            compression=self.config.compression,
            compression_level=self.config.compression_level,
        )
        return output_path


class ProductGenerator:

    def __init__(self, config: DataGenConfig):
        self.config = config
        self.fake = Faker(config.locale)
        Faker.seed(config.seed)
        np.random.seed(config.seed)

    def _generate_ean13(self, product_id: int) -> str:
        country_code = "400"  # Germany
        manufacturer = f"{product_id % 10000:04d}"
        product = f"{product_id % 100000:05d}"

        ean_without_check = country_code + manufacturer + product
        odd_sum = sum(int(ean_without_check[i]) for i in range(0, 12, 2))
        even_sum = sum(int(ean_without_check[i]) for i in range(1, 12, 2))
        check_digit = (10 - ((odd_sum + even_sum * 3) % 10)) % 10

        return ean_without_check + str(check_digit)

    def generate(self, num_products: int) -> pl.DataFrame:
        products = []
        for product_id in range(1, num_products + 1):
            category = np.random.choice(self.config.categories)

            if "Getränke" in category or "Brot" in category:
                base_price = np.random.uniform(0.49, 3.99)
            elif "Fleisch" in category or "Molkerei" in category:
                base_price = np.random.uniform(1.99, 12.99)
            elif "Aktionswaren" in category:
                base_price = np.random.uniform(4.99, 49.99)
            else:
                base_price = np.random.uniform(0.99, 9.99)

            product = {
                "product_id": product_id,
                "ean": self._generate_ean13(product_id),
                "product_name": f"{self.fake.word().capitalize()} {category.split()[0]}",
                "category": category,
                "subcategory": f"{category} - {self.fake.word().capitalize()}",
                "brand": (
                    self.fake.company()
                    if np.random.random() > 0.3
                    else "ALDI Eigenmarke"
                ),
                "unit_price": round(base_price, 2),
                "unit_size": np.random.choice(
                    ["250g", "500g", "1kg", "1L", "500ml", "100g"]
                ),
                "vat_rate": 0.19 if "Getränke" not in category else 0.07,  # German VAT
                "is_active": np.random.random() > 0.05,  # 95% active products
            }
            products.append(product)

        return pl.DataFrame(products)

    def save(self, df: pl.DataFrame, filename: str = "products.parquet"):
        output_path = self.config.landing_dir / filename
        df.write_parquet(
            output_path,
            compression=self.config.compression,
            compression_level=self.config.compression_level,
        )
        return output_path


class TransactionGenerator:

    def __init__(self, config: DataGenConfig):
        self.config = config
        self.fake = Faker(config.locale)
        Faker.seed(config.seed)
        np.random.seed(config.seed)

    def _is_german_holiday(self, date: datetime) -> bool:
        year = date.year
        month = date.month
        day = date.day

        # Fixed holidays
        fixed_holidays = [
            (1, 1),   # New Year
            (5, 1),   # Labor Day
            (10, 3),  # German Unity Day
            (12, 25), # Christmas Day
            (12, 26), # Boxing Day
        ]

        if (month, day) in fixed_holidays:
            return True

        # Easter calculation (using simple approximation)
        if year == 2020 and month == 4 and day in [10, 12, 13]:  # Easter 2020
            return True
        if year == 2021 and month == 4 and day in [2, 4, 5]:     # Easter 2021
            return True
        if year == 2022 and month == 4 and day in [15, 17, 18]:  # Easter 2022
            return True
        if year == 2023 and month == 4 and day in [7, 9, 10]:    # Easter 2023
            return True
        if year == 2024 and month == 3 and day in [29, 31]:      # Easter 2024 (March)
            return True
        if year == 2024 and month == 4 and day == 1:             # Easter Monday 2024
            return True
        if year == 2025 and month == 4 and day in [18, 20, 21]:  # Easter 2025
            return True

        return False

    def _get_hourly_traffic_pattern(self, hour: int, is_weekend: bool) -> float:
        if is_weekend:
            # Weekend pattern: slower start, peak 12-16
            if hour < 8:
                return 0.05
            elif hour < 10:
                return 0.3
            elif hour < 12:
                return 0.7
            elif hour < 16:
                return 1.0
            elif hour < 18:
                return 0.8
            elif hour < 20:
                return 0.5
            else:
                return 0.1
        else:
            # Weekday pattern: morning rush, lunch, evening
            if hour < 7:
                return 0.05
            elif hour < 9:
                return 0.5
            elif hour < 11:
                return 0.6
            elif hour < 14:
                return 0.8
            elif hour < 17:
                return 0.9
            elif hour < 19:
                return 1.0
            elif hour < 21:
                return 0.6
            else:
                return 0.1

    def _get_seasonal_multiplier(self, month: int) -> float:
        seasonal_multipliers = {
            1: 0.85,  # January (post-holiday)
            2: 0.90,  # February
            3: 0.95,  # March
            4: 1.00,  # April
            5: 1.05,  # May
            6: 1.10,  # June
            7: 1.15,  # July (summer)
            8: 1.10,  # August
            9: 1.05,  # September
            10: 1.10, # October
            11: 1.20, # November (pre-Christmas)
            12: 1.40, # December (Christmas shopping)
        }
        return seasonal_multipliers.get(month, 1.0)

    def generate(
        self,
        num_transactions: int,
        store_ids: list,
        product_ids: list,
    ) -> pl.DataFrame:
        # Parse dates from config
        start_date = datetime.strptime(self.config.start_date, "%Y-%m-%d")
        end_date = datetime.strptime(self.config.end_date, "%Y-%m-%d")

        base_year = start_date.year
        num_years = end_date.year - start_date.year + 1
        annual_growth_rate = self.config.yoy_growth

        year_multipliers = [1 + (i * annual_growth_rate) for i in range(num_years)]
        total_multiplier = sum(year_multipliers)
        transactions_per_year = [int(num_transactions * (m / total_multiplier)) for m in year_multipliers]

        remaining = num_transactions - sum(transactions_per_year)
        if remaining > 0:
            transactions_per_year[-1] += remaining

        all_transaction_dates = []

        for year_idx, year in enumerate(range(start_date.year, end_date.year + 1)):
            year_start = datetime(year, 1, 1) if year > start_date.year else start_date
            year_end = datetime(year, 12, 31, 23, 59, 59) if year < end_date.year else end_date

            year_dates = pd.date_range(start=year_start, end=year_end, freq='h')
            year_weights = []

            for dt in year_dates:
                is_holiday = self._is_german_holiday(dt) if self.config.enable_holidays else False
                is_weekend = dt.weekday() >= 5

                if is_holiday and self.config.enable_holidays:
                    weight = 0.1
                else:
                    hour_weight = self._get_hourly_traffic_pattern(dt.hour, is_weekend) * self.config.peak_hour_multiplier
                    seasonal_weight = self._get_seasonal_multiplier(dt.month) if self.config.enable_seasonality else 1.0
                    weight = hour_weight * seasonal_weight

                year_weights.append(weight)

            year_weights = np.array(year_weights)
            year_weights = year_weights / year_weights.sum()

            num_year_transactions = transactions_per_year[year_idx]

            sampled_indices = np.random.choice(
                len(year_dates), size=num_year_transactions, p=year_weights, replace=True
            )
            year_transaction_dates = year_dates[sampled_indices]

            random_minutes = np.random.randint(0, 60, size=num_year_transactions)
            random_seconds = np.random.randint(0, 60, size=num_year_transactions)
            year_transaction_dates = year_transaction_dates + pd.to_timedelta(random_minutes, unit='m') + pd.to_timedelta(random_seconds, unit='s')

            all_transaction_dates.append(year_transaction_dates.to_numpy())

        transaction_dates = np.concatenate(all_transaction_dates)
        num_transactions = len(transaction_dates)

        transactions = []
        batch_size = 100_000

        for batch_start in range(0, num_transactions, batch_size):
            batch_end = min(batch_start + batch_size, num_transactions)
            batch_size_actual = batch_end - batch_start

            # Use Poisson distribution for items per transaction
            quantities = np.random.poisson(lam=self.config.avg_items_per_transaction, size=batch_size_actual)
            quantities = np.clip(quantities, 1, 10)  # Ensure at least 1, max 10

            batch = {
                "transaction_id": np.arange(batch_start + 1, batch_end + 1),
                "store_id": np.random.choice(store_ids, size=batch_size_actual),
                "product_id": np.random.choice(product_ids, size=batch_size_actual),
                "transaction_datetime": transaction_dates[batch_start:batch_end],
                "quantity": quantities,
                "discount_percent": np.random.choice(
                    [0.0, 0.0, 0.0, 0.10, 0.15, 0.20, 0.25], size=batch_size_actual
                ),
                "payment_method": np.random.choice(
                    ["EC-Karte", "Bargeld", "Kreditkarte"],
                    p=[0.6, 0.3, 0.1],
                    size=batch_size_actual,
                ),
            }

            transactions.append(pl.DataFrame(batch))

        return pl.concat(transactions)

    def save(self, df: pl.DataFrame, filename: str = "transactions.parquet"):
        output_path = self.config.landing_dir / filename
        df.write_parquet(
            output_path,
            compression=self.config.compression,
            compression_level=self.config.compression_level,
        )
        return output_path
