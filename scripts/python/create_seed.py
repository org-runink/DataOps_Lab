#!/usr/bin/env python3
"""
create_seed.py

Generate CSV seed files for the dbt project.

- Output directory: scripts/dbt/seeds
- Files:
    products.csv
    warehouses.csv
    inventory_movements.csv
    customer_orders.csv

Environment variables:
- DBT_SEED_ROWS: number of rows for large seeds (default: 9999)
- SEED_RANDOM_SEED: optional, integer; if provided, makes generation deterministic
"""

from __future__ import annotations

import csv
import os
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from faker import Faker

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------

# Base dir is "<repo_root>/scripts"
BASE_DIR = Path(__file__).resolve().parents[1]

# Always put the seed files into the dbt project's seeds directory
# i.e., "<repo_root>/scripts/dbt/seeds"
SEED_DIR = BASE_DIR / "dbt" / "seeds"
SEED_DIR.mkdir(parents=True, exist_ok=True)

# Row count for the large, row-heavy seeds
NUM_ROWS = int(os.getenv("DBT_SEED_ROWS", "9999"))

# Optional deterministic seed for reproducibility (CI/debug)
RANDOM_SEED: int | None = None
try:
    RANDOM_SEED = (
        int(os.getenv("SEED_RANDOM_SEED")) if os.getenv("SEED_RANDOM_SEED") else None
    )
except (TypeError, ValueError):
    RANDOM_SEED = None

if RANDOM_SEED is not None:
    random.seed(RANDOM_SEED)

fake = Faker()
if RANDOM_SEED is not None:
    Faker.seed(RANDOM_SEED)

# ------------------------------------------------------------------------------
# Generators
# ------------------------------------------------------------------------------


def generate_products() -> None:
    """Generate a products.csv file with synthetic product data."""
    categories = ["Electronics", "Sports", "Home & Kitchen", "Books", "Toys"]
    out_path = SEED_DIR / "products.csv"
    with out_path.open(mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "product_id",
                "product_name",
                "category",
                "unit_cost",
                "weight_kg",
                "dimensions_cm",
            ]
        )
        for i in range(NUM_ROWS):
            product_id = f"PRD{i + 1:05d}"
            product_name = f"{fake.word().title()} {random.choice(['Pro', 'Max', 'Lite', 'Standard'])}"
            category = random.choice(categories)
            unit_cost = round(random.uniform(10.0, 500.0), 2)
            weight_kg = round(random.uniform(0.1, 20.0), 2)
            dimensions_cm = (
                f"{random.randint(5, 100)}x"
                f"{random.randint(5, 100)}x"
                f"{random.randint(5, 100)}"
            )
            writer.writerow(
                [
                    product_id,
                    product_name,
                    category,
                    unit_cost,
                    weight_kg,
                    dimensions_cm,
                ]
            )


def generate_warehouses() -> None:
    """Generate a warehouses.csv file with warehouse metadata."""
    out_path = SEED_DIR / "warehouses.csv"
    with out_path.open(mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "warehouse_id",
                "warehouse_name",
                "location",
                "capacity_units",
                "manager_name",
            ]
        )
        for i in range(1, 21):  # 20 warehouses
            warehouse_id = f"WH{i:03d}"
            warehouse_name = f"{fake.city()} Warehouse"
            location = f"{fake.city()}, {fake.country_code()}"
            capacity_units = random.randint(5_000, 100_000)
            manager_name = fake.name()
            writer.writerow(
                [warehouse_id, warehouse_name, location, capacity_units, manager_name]
            )


def generate_inventory_movements() -> None:
    """Generate an inventory_movements.csv file with stock movement events."""
    movement_types = ["replenishment", "outbound", "adjustment"]
    out_path = SEED_DIR / "inventory_movements.csv"
    with out_path.open(mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "movement_id",
                "product_id",
                "warehouse_id",
                "movement_date",
                "quantity",
                "movement_type",
            ]
        )
        for i in range(NUM_ROWS):
            movement_id = f"MV{i + 1:07d}"
            product_id = f"PRD{random.randint(1, NUM_ROWS):05d}"
            warehouse_id = f"WH{random.randint(1, 20):03d}"
            movement_date = (
                datetime.today() - timedelta(days=random.randint(0, 365))
            ).strftime("%Y-%m-%d")
            quantity = random.randint(-500, 1000)
            movement_type = random.choice(movement_types)
            writer.writerow(
                [
                    movement_id,
                    product_id,
                    warehouse_id,
                    movement_date,
                    quantity,
                    movement_type,
                ]
            )


def generate_customer_orders() -> None:
    """Generate a customer_orders.csv file with order data."""
    sales_channels = ["online", "retail", "wholesale"]
    out_path = SEED_DIR / "customer_orders.csv"
    with out_path.open(mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "order_id",
                "product_id",
                "warehouse_id",
                "order_date",
                "quantity",
                "sales_channel",
            ]
        )
        for i in range(NUM_ROWS):
            order_id = f"ORD{i + 1:07d}"
            product_id = f"PRD{random.randint(1, NUM_ROWS):05d}"
            warehouse_id = f"WH{random.randint(1, 20):03d}"
            order_date = (
                datetime.today() - timedelta(days=random.randint(0, 365))
            ).strftime("%Y-%m-%d")
            quantity = random.randint(1, 50)
            sales_channel = random.choice(sales_channels)
            writer.writerow(
                [
                    order_id,
                    product_id,
                    warehouse_id,
                    order_date,
                    quantity,
                    sales_channel,
                ]
            )


# ------------------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------------------


def main() -> None:
    print(f"[create_seed] Writing seeds to: {SEED_DIR}")
    print(f"[create_seed] NUM_ROWS={NUM_ROWS}  RANDOM_SEED={RANDOM_SEED}")

    print("Generating products.csv...")
    generate_products()
    print("products.csv generated successfully.")

    print("Generating warehouses.csv...")
    generate_warehouses()
    print("warehouses.csv generated successfully.")

    print("Generating inventory_movements.csv...")
    generate_inventory_movements()
    print("inventory_movements.csv generated successfully.")

    print("Generating customer_orders.csv...")
    generate_customer_orders()
    print("customer_orders.csv generated successfully.")


if __name__ == "__main__":
    main()
