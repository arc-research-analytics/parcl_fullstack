"""Database operations for Supabase interactions."""

import pandas as pd
from supabase import create_client
from typing import List, Dict, Any


class SupabaseManager:
    """Handles all Supabase database operations."""
    
    def __init__(self, config):
        """Initialize Supabase client."""
        self.config = config
        self.supabase = create_client(config.supabase_url, config.supabase_key)
        print('Connected to Supabase...')
    
    def prepare_dataframe_for_supabase(self, df: pd.DataFrame, today_formatted: str) -> pd.DataFrame:
        """Prepare DataFrame for Supabase insertion by handling data types and dates."""
        df = df.copy()
        
        # Add as_of_date column
        df['as_of_date'] = today_formatted
        
        # Handle datetime columns - convert to strings first
        datetime_columns = df.select_dtypes(include=['datetime64[ns]']).columns
        for col in datetime_columns:
            print(f"Converting datetime column {col} to string")
            df[col] = df[col].dt.strftime('%Y-%m-%d')
            df[col] = df[col].replace('NaT', None)
        
        # Replace specific problematic values with None for date columns
        date_columns = ['original_list_date', 'most_recent_sale_date', 'sale_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = df[col].replace(['0', 'NaT'], None)
        
        # Fill NaN values with appropriate defaults
        df = df.fillna(0)
        
        return df
    
    def prepare_listings_for_supabase(self, listings_df: pd.DataFrame, today_formatted: str) -> List[Dict[str, Any]]:
        """Prepare listings data for Supabase insertion."""
        # Select and rename columns for unaggregated listings
        listings_clean = listings_df.copy()
        
        # Select specific columns
        listings_clean = listings_clean[[
            'address',
            'h3_id',
            'county_name',
            'property_type',
            'square_feet',
            'year_built',
            'latitude',
            'longitude',
            'institutional_investor',
            'original_list_date',
            'original_list_price',
            'current_list_price',
            'list_per_sq_ft',
            'days_on_market',
            'most_recent_sale_date',
            'most_recent_sale_price'
        ]]
        
        # Rename columns
        listings_clean = listings_clean.rename(columns={
            'county_name': 'county',
            'institutional_investor': 'inst_owner'
        })
        
        # Standardize property types
        for old_type, new_type in self.config.property_type_mapping.items():
            listings_clean['property_type'] = listings_clean['property_type'].str.replace(old_type, new_type)
        
        # Prepare for Supabase
        listings_clean = self.prepare_dataframe_for_supabase(listings_clean, today_formatted)
        
        # Cast year_built to int
        listings_clean['year_built'] = listings_clean['year_built'].astype(int)
        
        return listings_clean.to_dict(orient='records')
    
    def prepare_sales_for_supabase(self, sales_df: pd.DataFrame, today_formatted: str) -> List[Dict[str, Any]]:
        """Prepare sales data for Supabase insertion."""
        # Select specific columns
        sales_clean = sales_df[[
            'address',
            'h3_id',
            'county',
            'property_type',
            'square_feet',
            'year_built',
            'latitude',
            'longitude',
            'sale_date',
            'sale_price',
            'price_sf',
            'buyer',
            'seller'
        ]].copy()
        
        # Prepare for Supabase
        sales_clean = self.prepare_dataframe_for_supabase(sales_clean, today_formatted)
        
        # Cast year_built to int
        sales_clean['year_built'] = sales_clean['year_built'].astype(int)
        
        return sales_clean.to_dict(orient='records')
    
    def prepare_hex_summary_for_supabase(self, hex_summary_df: pd.DataFrame, today_formatted: str) -> List[Dict[str, Any]]:
        """Prepare hex summary data for Supabase insertion."""
        hex_clean = hex_summary_df.copy()
        
        # Add as_of_date and filter to today only
        hex_clean['as_of_date'] = today_formatted
        hex_clean = hex_clean[hex_clean['as_of_date'] == today_formatted]
        
        # Reorder columns
        hex_clean = hex_clean[[
            'h3_id',
            'as_of_date',
            'total_sales',
            'inst_acquisitions',
            'inst_dispositions',
            'median_vintage',
            'median_size',
            'median_price_sf',
            'total_listings',
            'inst_listings',
            'median_list_price_sqft'
        ]]
        
        # Fill NaN values and cast integer columns
        hex_clean = hex_clean.fillna(0)
        integer_columns = ['total_sales', 'inst_acquisitions', 'inst_dispositions', 
                          'total_listings', 'inst_listings']
        for col in integer_columns:
            hex_clean[col] = hex_clean[col].astype(int)
        
        return hex_clean.to_dict(orient='records')
    
    def prepare_county_summary_for_supabase(self, county_summary_df: pd.DataFrame, today_formatted: str) -> List[Dict[str, Any]]:
        """Prepare county summary data for Supabase insertion."""
        county_clean = county_summary_df.copy()
        
        # Add as_of_date and filter to today only
        county_clean['as_of_date'] = today_formatted
        county_clean = county_clean[county_clean['as_of_date'] == today_formatted]
        
        # Reorder columns
        county_clean = county_clean[[
            'county',
            'year_month',
            'as_of_date',
            'total_sales',
            'inst_acquisitions',
            'inst_dispositions',
            'median_vintage',
            'median_size',
            'median_price_sf'
        ]]
        
        # Fill NaN values and cast integer columns
        county_clean = county_clean.fillna(0)
        integer_columns = ['total_sales', 'inst_acquisitions', 'inst_dispositions', 
                          'median_vintage', 'median_size']
        for col in integer_columns:
            county_clean[col] = county_clean[col].astype(int)
        
        # Sort by county
        county_clean = county_clean.sort_values(by='county', ascending=True)
        
        return county_clean.to_dict(orient='records')
    
    def clear_table(self, table_name: str, condition_column: str = None, condition_value: str = None):
        """Clear existing data from a table using batch deletion to avoid timeouts."""
        print(f"Clearing table '{table_name}'...")
        
        if condition_column and condition_value:
            self.supabase.table(table_name).delete().neq(condition_column, condition_value).execute()
        else:
            # For large tables like sales_unagg and listings_unagg, use batch deletion
            if table_name in ["sales_unagg", "listings_unagg"]:
                self._clear_large_table_in_batches(table_name)
            elif table_name == "hex_summary":
                self.supabase.table(table_name).delete().neq("h3_id", 0).execute()
            else:
                # For smaller tables, use regular deletion
                self.supabase.table(table_name).delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
    
    def _clear_large_table_in_batches(self, table_name: str):
        """Clear large tables by deleting records older than today to avoid timeout errors."""
        print(f"  Attempting to clear all existing data from {table_name}...")
        
        # Strategy 1: Delete by date ranges (more reliable than LIMIT on DELETE)
        try:
            # First, try to delete all records that are NOT from today (older data)
            today = self.config.today_formatted
            result = self.supabase.table(table_name).delete().neq("as_of_date", today).execute()
            old_deleted = len(result.data) if result.data else 0
            print(f"  Deleted {old_deleted} old records (not from {today})")
            
            # Then delete any remaining records from today (if they exist)
            result = self.supabase.table(table_name).delete().eq("as_of_date", today).execute()
            today_deleted = len(result.data) if result.data else 0
            print(f"  Deleted {today_deleted} records from {today}")
            
            total_deleted = old_deleted + today_deleted
            print(f"  Total rows deleted from {table_name}: {total_deleted}")
            
        except Exception as e:
            print(f"  Date-based deletion failed: {e}")
            print(f"  Falling back to direct deletion...")
            
            # Strategy 2: Fallback - try direct deletion with smaller timeout expectation
            try:
                result = self.supabase.table(table_name).delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
                deleted_count = len(result.data) if result.data else 0
                print(f"  Direct deletion completed: {deleted_count} rows deleted")
            except Exception as e2:
                print(f"  ERROR: Could not clear table {table_name}: {e2}")
                print(f"  Proceeding anyway - new data will be appended")
    
    def insert_data_in_batches(self, table_name: str, data: List[Dict[str, Any]], description: str = ""):
        """Insert data into Supabase table in batches."""
        if not data:
            print(f"No data to insert into {table_name}")
            return
        
        batch_size = self.config.batch_size
        total_batches = len(data) // batch_size + (1 if len(data) % batch_size > 0 else 0)
        
        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            self.supabase.table(table_name).insert(batch).execute()
            batch_number = i // batch_size + 1
            print(f"Inserted batch {batch_number} of {total_batches} for {description}")
    
    def upload_all_data(self, hex_summary: pd.DataFrame, county_summary: pd.DataFrame, 
                       listings_df: pd.DataFrame, sales_df: pd.DataFrame, today_formatted: str):
        """Upload all processed data to Supabase."""
        
        # Prepare data for Supabase
        print("Preparing data for Supabase...")
        supabase_hex_summary = self.prepare_hex_summary_for_supabase(hex_summary, today_formatted)
        supabase_county_summary = self.prepare_county_summary_for_supabase(county_summary, today_formatted)
        supabase_listings = self.prepare_listings_for_supabase(listings_df, today_formatted)
        supabase_sales = self.prepare_sales_for_supabase(sales_df, today_formatted)
        
        # Upload hex summary
        print("Uploading hex summary data...")
        self.clear_table("hex_summary")
        self.insert_data_in_batches("hex_summary", supabase_hex_summary, "hex summary")
        print('Hex summary rows added to Supabase🎉')
        
        # Upload county summary
        print("Uploading county summary data...")
        self.clear_table("county_summary")
        self.insert_data_in_batches("county_summary", supabase_county_summary, "county summary")
        print('County summary rows added to Supabase🎉')
        
        # Upload unaggregated listings
        print("Uploading unaggregated listings data...")
        self.clear_table("listings_unagg")
        self.insert_data_in_batches("listings_unagg", supabase_listings, "unaggregated listings")
        print('Unaggregated listings added to Supabase🎉')
        
        # Upload unaggregated sales
        print("Uploading unaggregated sales data...")
        self.clear_table("sales_unagg")
        self.insert_data_in_batches("sales_unagg", supabase_sales, "unaggregated sales")
        print('Unaggregated sales added to Supabase🎉')
