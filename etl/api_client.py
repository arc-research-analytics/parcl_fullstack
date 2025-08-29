"""API client wrapper for Parcl Labs API interactions."""

import pandas as pd
from parcllabs import ParclLabsClient
from typing import List, Dict, Any


class ParclAPIClient:
    """Wrapper class for Parcl Labs API with common patterns."""
    
    def __init__(self, api_key: str):
        """Initialize the API client."""
        self.client = ParclLabsClient(api_key=api_key)
    
    def fetch_listings_for_county(self, parcl_id: int, county_name: str, config) -> pd.DataFrame:
        """
        Fetch current listings for a specific county.
        
        Args:
            parcl_id: Parcl ID for the county
            county_name: Name of the county for logging
            config: ETL configuration object
            
        Returns:
            DataFrame with listing data
        """
        # print(f'Getting listings for {county_name}...')  # COMMENTED: Verbose for production
        
        listings = self.client.property_v2.search.retrieve(
            parcl_ids=[parcl_id],
            event_names=["ALL_LISTINGS"],
            property_types=config.property_types,
            current_on_market_flag=True,
            limit=config.api_limit,
            include_property_details=True,
            include_full_event_history=True,
            min_price=config.min_price,
            min_sqft=config.min_sqft
        )
        
        listings_df = listings[0]
        # print(f'-#-#-#-# Found {len(listings_df):,} raw listing records in {county_name}')  # COMMENTED: Verbose for production
        
        return listings_df
    
    def fetch_sales_for_county(self, parcl_id: int, county_name: str, config) -> pd.DataFrame:
        """
        Fetch sales data for a specific county within the configured date range.
        
        Args:
            parcl_id: Parcl ID for the county
            county_name: Name of the county for logging
            config: ETL configuration object
            
        Returns:
            DataFrame with sales data
        """
        # print(f'Getting sales for {county_name}...')  # COMMENTED: Verbose for production
        
        sales = self.client.property_v2.search.retrieve(
            parcl_ids=[parcl_id],
            event_names=["SOLD"],
            property_types=config.property_types,
            current_on_market_flag=False,
            limit=config.api_limit,
            include_property_details=True,
            min_event_date=config.min_date_formatted,
            max_event_date=config.max_date_formatted,
            min_price=config.min_price,
            min_sqft=config.min_sqft
        )
        
        sales_df = sales[0]
        # print(f'-#-#-#-# Found {len(sales_df):,} sales in {county_name}')  # COMMENTED: Verbose for production
        
        return sales_df
    
    def fetch_all_listings(self, config) -> List[pd.DataFrame]:
        """
        Fetch listings for all counties in the configuration.
        
        Args:
            config: ETL configuration object
            
        Returns:
            List of DataFrames, one for each county
        """
        all_listings = []
        
        for parcl_id, county_name in config.county_id_map.items():
            listings_df = self.fetch_listings_for_county(parcl_id, county_name, config)
            all_listings.append(listings_df)
        
        return all_listings
    
    def fetch_all_sales(self, config) -> List[pd.DataFrame]:
        """
        Fetch sales data for all counties in the configuration.
        
        Args:
            config: ETL configuration object
            
        Returns:
            List of DataFrames, one for each county
        """
        all_sales = []
        
        for parcl_id, county_name in config.county_id_map.items():
            sales_df = self.fetch_sales_for_county(parcl_id, county_name, config)
            all_sales.append(sales_df)
        
        return all_sales
