"""Utility functions for the ETL pipeline."""

import pandas as pd
import geopandas as gpd
from typing import Tuple


def load_hex_data(hex_file_path: str = 'config/metro-hex.geojson') -> gpd.GeoDataFrame:
    """Load and prepare hex data for spatial joins."""
    hex_gdf = gpd.read_file(hex_file_path)
    hex_gdf = hex_gdf.to_crs('EPSG:4326')
    return hex_gdf


def combine_hex_and_listings_summaries(sales_hex_summary: pd.DataFrame, 
                                     listings_hex_summary: pd.DataFrame) -> pd.DataFrame:
    """Combine sales and listings hex summaries into final summary."""
    final_hex_summary = pd.merge(sales_hex_summary, listings_hex_summary, on='h3_id', how='outer')
    return final_hex_summary


def validate_data_quality(df: pd.DataFrame, data_type: str) -> None:
    """Perform basic data quality validation."""
    print(f"\n--- Data Quality Check for {data_type} ---")
    print(f"Total records: {len(df):,}")
    print(f"Null values per column:")
    null_counts = df.isnull().sum()
    for col, count in null_counts.items():
        if count > 0:
            print(f"  {col}: {count:,} ({count/len(df)*100:.1f}%)")
    
    if data_type == "sales" and 'price_sf' in df.columns:
        print(f"Price per sq ft stats:")
        print(f"  Min: ${df['price_sf'].min():.2f}")
        print(f"  Max: ${df['price_sf'].max():.2f}")
        print(f"  Median: ${df['price_sf'].median():.2f}")
        
    print("--- End Data Quality Check ---\n")
