"""Data processing classes for listings and sales data."""

import pandas as pd
import geopandas as gpd
from typing import List, Tuple
from datetime import datetime
from dateutil.relativedelta import relativedelta


class ListingsProcessor:
    """Handles processing and cleaning of listings data."""
    
    def __init__(self, config, hex_gdf: gpd.GeoDataFrame):
        """Initialize the processor with configuration and hex data."""
        self.config = config
        self.hex_gdf = hex_gdf
    
    def get_current_listings(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform Parcl Labs property search V2 endpoint data to show only current listings 
        with original and current pricing. Vectorized implementation for performance.
        
        Args:
            df: Raw listings DataFrame from API
            
        Returns:
            Cleaned DataFrame with current listings info
        """
        # Step 1: For each property, keep only records with the max event_true_sale_index
        max_sale_index = df.groupby('parcl_property_id')['event_true_sale_index'].transform('max')
        current_cycle = df[df['event_true_sale_index'] == max_sale_index].copy()
        
        # Step 2: Split listings and sales
        listings_only = current_cycle[current_cycle['event_event_type'] == 'LISTING'].copy()
        sales_only = current_cycle[current_cycle['event_event_type'] == 'SALE'].copy()
        
        # Step 3: For each property, find original (min date) and current (max date) listings
        list_dates = listings_only.groupby('parcl_property_id')['event_event_date'].agg(
            original_list_date='min',
            current_list_date='max'
        ).reset_index()
        
        # Step 4: Vectorized join to get original/current prices
        original_prices = listings_only.merge(
            list_dates[['parcl_property_id', 'original_list_date']],
            left_on=['parcl_property_id', 'event_event_date'],
            right_on=['parcl_property_id', 'original_list_date'],
            how='inner'
        )[["parcl_property_id", "event_price"]].rename(columns={'event_price': 'original_list_price'})
        
        current_prices = listings_only.merge(
            list_dates[['parcl_property_id', 'current_list_date']],
            left_on=['parcl_property_id', 'event_event_date'],
            right_on=['parcl_property_id', 'current_list_date'],
            how='inner'
        )[["parcl_property_id", "event_price"]].rename(columns={'event_price': 'current_list_price'})
        
        grouped = list_dates.merge(original_prices, on='parcl_property_id') \
                            .merge(current_prices, on='parcl_property_id')
        
        # Step 4.5: Sale info (most recent sale date + price)
        if not sales_only.empty:
            sale_info = sales_only.loc[
                sales_only.groupby('parcl_property_id')['event_event_date'].idxmax(),
                ['parcl_property_id', 'event_event_date', 'event_price']
            ].rename(columns={
                'event_event_date': 'most_recent_sale_date',
                'event_price': 'most_recent_sale_price'
            })
            
            grouped = grouped.merge(sale_info, on='parcl_property_id', how='left')
        else:
            grouped['most_recent_sale_date'] = None
            grouped['most_recent_sale_price'] = None
        
        # Step 5: Finalize result
        result = grouped[['parcl_property_id', 'original_list_date', 'original_list_price','current_list_price',
                        'most_recent_sale_date', 'most_recent_sale_price']].copy()
        
        # Calculate days_on_market = today - original_list_date
        result['days_on_market'] = (pd.to_datetime('today') - pd.to_datetime(result['original_list_date'])).dt.days
        
        # Optional: Add other property information from most recent listing
        latest_records = listings_only.loc[
            listings_only.groupby('parcl_property_id')['event_event_date'].idxmax()
        ]
        other_columns = [col for col in latest_records.columns 
                        if col not in ['parcl_property_id', 'event_event_date', 'event_price', 
                                      'event_event_type', 'event_true_sale_index']]
        
        if other_columns:
            additional_info = latest_records[['parcl_property_id'] + other_columns]
            result = result.merge(additional_info, on='parcl_property_id', how='left')
        
        return result
    
    def clean_and_standardize_listings(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize listings data."""
        # Rename columns
        df = df.rename(columns=self.config.listings_column_mapping)
        
        # Remove ' County' from the 'county_name' column
        df['county_name'] = df['county_name'].str.replace(' County', '')
        
        # Create price per square foot
        df['list_per_sq_ft'] = df['current_list_price'] / df['square_feet']
        
        # Create listing to sale ratio
        df['listing_to_sale_ratio'] = df['current_list_price'] / df['most_recent_sale_price']
        
        # Reorder columns
        df = df[[
            'parcl_property_id', # unique identifier
            'original_list_date', 'original_list_price', 'current_list_price', 'list_per_sq_ft', 'days_on_market', # listing info
            'most_recent_sale_date', 'most_recent_sale_price', 'listing_to_sale_ratio', # sales info
            'address', # property-specific metadata
            'county_name',
            'property_type', 
            'square_feet',
            'year_built',
            'latitude',
            'longitude',
            'institutional_investor'
        ]]
        
        # Remove duplicates based on parcl_property_id
        df = df.drop_duplicates(subset='parcl_property_id', keep='first')
        
        return df
    
    def process_all_listings(self, raw_listings: List[pd.DataFrame]) -> pd.DataFrame:
        """Process all raw listings data into clean format."""
        cleaned_listings = []
        
        for listings_df in raw_listings:
            # Clean the listings
            listings_cleaned = self.get_current_listings(listings_df)
            listings_cleaned = self.clean_and_standardize_listings(listings_cleaned)
            cleaned_listings.append(listings_cleaned)
        
        # Combine all listings
        master_listings = pd.concat(cleaned_listings, ignore_index=True)
        return master_listings
    
    def create_spatial_listings(self, listings_df: pd.DataFrame) -> pd.DataFrame:
        """Create GeoDataFrame and perform spatial join with hex data."""
        # Create GeoPandas geodataframe
        listings_gdf = gpd.GeoDataFrame(
            listings_df, 
            geometry=gpd.points_from_xy(listings_df['longitude'], listings_df['latitude']),
            crs='EPSG:4326'
        )
        
        # Run spatial join
        listings_joined = gpd.sjoin(listings_gdf, self.hex_gdf, how='left', predicate='within')
        return listings_joined.drop(columns=['index_right', 'resolution', 'geometry'])
    
    def aggregate_listings_by_hex(self, listings_df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate listings data by H3 hexagon ID."""
        # Regular listings aggregation
        listings_summary1 = listings_df.groupby('h3_id').agg(
            total_listings=('parcl_property_id','count'),
            median_list_price_sqft=('list_per_sq_ft','median'),
        ).reset_index()
        
        # Institutional investor listings aggregation
        listings_summary2 = listings_df[~listings_df['institutional_investor'].isna()].groupby('h3_id').agg(
            inst_listings=('parcl_property_id','count'),
        ).reset_index()
        
        # Merge the two summaries
        listings_hex_summary = pd.merge(listings_summary1, listings_summary2, on='h3_id', how='left')
        return listings_hex_summary


class SalesProcessor:
    """Handles processing and cleaning of sales data."""
    
    def __init__(self, config, hex_gdf: gpd.GeoDataFrame):
        """Initialize the processor with configuration and hex data."""
        self.config = config
        self.hex_gdf = hex_gdf
    
    def clean_and_standardize_sales(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize sales data."""
        # Rename columns
        df = df.rename(columns=self.config.sales_column_mapping)
        
        # Reorder columns
        df = df[[
            'parcl_property_id',
            'address',
            'county',
            'sale_date',
            'sale_price',
            'buyer',
            'seller',
            'property_type',
            'square_feet',
            'year_built',
            'latitude',
            'longitude'
        ]]
        
        # Clean up 'county' column
        df['county'] = df['county'].str.replace(' County', '')
        
        # Standardize property types
        for old_type, new_type in self.config.property_type_mapping.items():
            df['property_type'] = df['property_type'].str.replace(old_type, new_type)
        
        return df
    
    def apply_data_quality_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply data quality filters to sales data."""
        # Remove duplicates based on county, sale_date, and sale_price
        df = df.drop_duplicates(subset=['county', 'sale_date', 'sale_price'])
        
        # Calculate price per square foot
        df = df.copy()
        df['price_sf'] = df['sale_price'] / df['square_feet']
        
        # Remove excessive price per square foot
        df = df[df['price_sf'] < self.config.max_price_per_sqft]
        
        return df
    
    def process_all_sales(self, raw_sales: List[pd.DataFrame]) -> pd.DataFrame:
        """Process all raw sales data into clean format."""
        # Combine all sales
        master_sales = pd.concat(raw_sales, ignore_index=True)
        
        # Clean and standardize
        master_sales = self.clean_and_standardize_sales(master_sales)
        
        # Apply data quality filters
        master_sales = self.apply_data_quality_filters(master_sales)
        
        return master_sales
    
    def create_spatial_sales(self, sales_df: pd.DataFrame) -> pd.DataFrame:
        """Create GeoDataFrame and perform spatial join with hex data."""
        # Create GeoDataFrame
        sales_gdf = gpd.GeoDataFrame(
            sales_df, 
            geometry=gpd.points_from_xy(sales_df['longitude'], sales_df['latitude']),
            crs='EPSG:4326'
        )
        
        # Run spatial join
        sales_joined = gpd.sjoin(sales_gdf, self.hex_gdf, how='left', predicate='within')
        return sales_joined.drop(columns=['index_right', 'resolution', 'geometry'])
    
    def create_investor_sales_data(self, sales_df: pd.DataFrame) -> pd.DataFrame:
        """Create investor sales dataset (where buyer or seller is institutional)."""
        sales_investor = sales_df[sales_df['buyer'].notna() | sales_df['seller'].notna()].copy()
        sales_investor['sale_date'] = pd.to_datetime(sales_investor['sale_date'])
        sales_investor['year_month'] = sales_investor['sale_date'].dt.to_period('M')
        sales_investor['year_month'] = sales_investor['year_month'].astype(str)
        return sales_investor
    
    def aggregate_sales_by_hex(self, sales_df: pd.DataFrame, sales_investor_df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate sales data by H3 hexagon ID (12-month window only)."""
        # Filter to 12 months for hex aggregation
        hex_date_filter = self.config.get_hex_date_filter()
        
        sales_hex_agg = sales_df.copy()
        sales_hex_agg['sale_date'] = pd.to_datetime(sales_hex_agg['sale_date'])
        sales_hex_agg = sales_hex_agg[sales_hex_agg['sale_date'] > hex_date_filter]
        
        sales_investor_hex_agg = sales_investor_df.copy()
        sales_investor_hex_agg = sales_investor_hex_agg[sales_investor_hex_agg['sale_date'] > hex_date_filter]
        
        # Regular sales aggregation
        sales_hex_summary1 = sales_hex_agg.groupby('h3_id').agg(
            total_sales=('parcl_property_id','count'),
            median_vintage=('year_built','median'),
            median_size=('square_feet','median'),
            median_price_sf=('price_sf','median'),
        ).reset_index()
        
        # Investor sales aggregation
        sales_hex_summary2 = sales_investor_hex_agg.groupby('h3_id').agg(
            inst_acquisitions=('buyer','count'),
            inst_dispositions=('seller','count'),
        ).reset_index()
        
        # Merge the summaries
        sales_hex_summary = pd.merge(sales_hex_summary1, sales_hex_summary2, on='h3_id', how='outer')
        return sales_hex_summary
    
    def aggregate_sales_by_county(self, sales_df: pd.DataFrame, sales_investor_df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate sales data by county (full 36-month window)."""
        # Prepare sales data with year_month
        sales_county = sales_df.copy()
        sales_county['sale_date'] = pd.to_datetime(sales_county['sale_date'])
        sales_county['year_month'] = sales_county['sale_date'].dt.to_period('M')
        sales_county['year_month'] = sales_county['year_month'].astype(str)
        
        # Regular sales aggregation
        sales_county_summary1 = sales_county.groupby(['county', 'year_month']).agg(
            total_sales=('parcl_property_id','count'),
            median_vintage=('year_built','median'),
            median_size=('square_feet','median'),
            median_price_sf=('price_sf','median'),
        ).reset_index()
        
        # Investor sales aggregation
        sales_county_summary2 = sales_investor_df.groupby(['county', 'year_month']).agg(
            inst_acquisitions=('buyer','count'),
            inst_dispositions=('seller','count'),
        ).reset_index()
        
        # Merge the summaries
        sales_county_summary = pd.merge(sales_county_summary1, sales_county_summary2, 
                                      on=['county', 'year_month'], how='outer')
        return sales_county_summary
