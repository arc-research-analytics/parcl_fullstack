"""Configuration settings for the ETL pipeline."""

import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv


class ETLConfig:
    """Configuration class for ETL pipeline settings."""
    
    def __init__(self):
        # Load environment variables
        load_dotenv('config/.env')
        
        # Time parameters
        self.lookback_lag = 2  # this sets the max date for the Parcl API
        self.lookback_window = 6  # months for incremental data fetching 
        self.retention_window = 36  # months to retain in Supabase (FIFO cutoff) - EASILY ADJUSTABLE!
        self.hex_aggregation_window = 12  # months for hex-level data
        
        # Date calculations
        self.today = datetime.now()
        self.today_formatted = self.today.strftime('%Y.%m.%d')
        
        three_months_ago = self.today - relativedelta(months=self.lookback_lag)
        self.max_date = (three_months_ago.replace(day=1) + relativedelta(months=1, days=-1))
        self.max_date_formatted = self.max_date.strftime('%Y-%m-%d')
        
        min_date = (self.today - relativedelta(months=self.lookback_lag + self.lookback_window - 1)).replace(day=1)
        self.min_date_formatted = min_date.strftime('%Y-%m-%d')
        
        # API settings
        self.parcl_api_key = os.getenv('PARCL_API_KEY')
        if not self.parcl_api_key:
            raise ValueError("PARCL_API_KEY is not set in the environment variables")
        
        # Database settings
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
        self.batch_size = 500
        
        # Data filters
        self.min_price = 50000
        self.min_sqft = 500
        self.max_price_per_sqft = 2500
        self.api_limit = 50000
        
        # Property types
        self.property_types = ["SINGLE_FAMILY", "CONDO", "TOWNHOUSE"]
        
        # County ID mapping
        self.county_id_map = {
            5821775: 'Barrow', 
            5823208: 'Bartow', 
            5824489: 'Butts', 
            5821127: 'Carroll', 
            5822987: 'Cherokee', 
            5821000: 'Clayton', 
            5822520: 'Cobb', 
            5820743: 'Coweta', 
            5820885: 'Dawson', 
            5821075: 'DeKalb', 
            5822002: 'Douglas', 
            5822843: 'Fayette', 
            5824605: 'Forsyth', 
            5823604: 'Fulton', 
            5822064: 'Gwinnett', 
            5823136: 'Haralson', 
            5821562: 'Heard', 
            5820830: 'Henry', 
            5820767: 'Jasper', 
            5824502: 'Lumpkin', 
            5822765: 'Meriwether', 
            5822014: 'Morgan', 
            5823086: 'Newton', 
            5822617: 'Paulding', 
            5821076: 'Pickens', 
            5822152: 'Pike', 
            5823393: 'Rockdale', 
            5824484: 'Spalding', 
            5821707: 'Walton'
        }
        
        # Column mappings
        self.listings_column_mapping = {
            'property_metadata_address1': 'address',
            'property_metadata_property_type': 'property_type',
            'property_metadata_sq_ft': 'square_feet',
            'property_metadata_year_built': 'year_built',
            'property_metadata_latitude': 'latitude',
            'property_metadata_longitude': 'longitude',
            'property_metadata_current_entity_owner_name': 'institutional_investor',
            'property_metadata_county_name': 'county_name'
        }
        
        self.sales_column_mapping = {
            'property_metadata_address1': 'address',
            'property_metadata_property_type': 'property_type',
            'property_metadata_sq_ft': 'square_feet',
            'property_metadata_year_built': 'year_built',
            'property_metadata_latitude': 'latitude',
            'property_metadata_longitude': 'longitude',
            'event_entity_owner_name': 'buyer',
            'event_entity_seller_name': 'seller',
            'property_metadata_county_name': 'county',
            'event_event_date': 'sale_date',
            'event_price': 'sale_price'
        }
        
        # Property type standardization
        self.property_type_mapping = {
            'SINGLE_FAMILY': 'SFR',
            'TOWNHOUSE': 'Townhouse',
            'CONDO': 'Condo'
        }

    def get_hex_date_filter(self):
        """Get the date filter for hex-level aggregations (12 months)."""
        return self.max_date - relativedelta(months=self.hex_aggregation_window)

    def get_retention_cutoff_date(self):
        """Get the cutoff date for data retention (FIFO)."""
        cutoff_date = self.today - relativedelta(months=self.retention_window)
        return cutoff_date.replace(day=1)  # First day of the month

    def get_retention_cutoff_formatted(self):
        """Get formatted retention cutoff date."""
        return self.get_retention_cutoff_date().strftime('%Y-%m-%d')
