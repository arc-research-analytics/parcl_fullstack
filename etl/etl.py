"""
Refactored ETL Pipeline for Real Estate Data Processing

This module orchestrates the entire ETL pipeline for real estate data from Parcl Labs API.
It processes listings and sales data, performs spatial joins with H3 hexagon data,
and uploads the results to Supabase.

Key Features:
- Configurable lookback periods (36 months for county trends, 12 months for hex aggregations)
- Institutional investor tracking
- Data quality filtering 
- Spatial aggregation by H3 hexagons and counties
- Batch uploads to Supabase database
"""

import sys
import traceback
from config import ETLConfig
from api_client import ParclAPIClient
from data_processors import ListingsProcessor, SalesProcessor
from database_operations import SupabaseManager
from utils import load_hex_data, combine_hex_and_listings_summaries, validate_data_quality


def main():
    """Main ETL pipeline orchestrator."""
    try:
        print("🚀 Starting ETL Pipeline...")
        
        # Initialize configuration
        print("📋 Loading configuration...")
        config = ETLConfig()
        print(f"📅 Data range: {config.min_date_formatted} to {config.max_date_formatted}")
        print(f"⏰ Lookback window: {config.lookback_window} months")
        print(f"🔶 Hex aggregation window: {config.hex_aggregation_window} months")
        
        # Initialize API client
        print("🔌 Connecting to Parcl Labs API...")
        api_client = ParclAPIClient(config.parcl_api_key)
        
        # Load hex data for spatial joins
        print("🗺️ Loading hex geodata...")
        hex_gdf = load_hex_data()
        print(f"📍 Loaded {len(hex_gdf):,} hex polygons")
        
        # Initialize data processors
        listings_processor = ListingsProcessor(config, hex_gdf)
        sales_processor = SalesProcessor(config, hex_gdf)
        
        # ========== LISTINGS PROCESSING ==========
        print("\n📋 Processing Listings Data...")
        
        # Fetch raw listings data
        raw_listings = api_client.fetch_all_listings(config)
        
        # Process listings
        master_listings = listings_processor.process_all_listings(raw_listings)
        print(f"✅ Processed {len(master_listings):,} current listings")
        # validate_data_quality(master_listings, "listings")
        
        # Create spatial listings and aggregate
        listings_with_hex = listings_processor.create_spatial_listings(master_listings)
        listings_hex_summary = listings_processor.aggregate_listings_by_hex(listings_with_hex)
        print(f"📊 Created hex aggregations for {len(listings_hex_summary):,} hexagons")
        
        # ========== SALES PROCESSING ==========
        print("\n💰 Processing Sales Data...")
        
        # Fetch raw sales data
        raw_sales = api_client.fetch_all_sales(config)
        
        # Process sales
        master_sales = sales_processor.process_all_sales(raw_sales)
        print(f"✅ Processed {len(master_sales):,} sales transactions")
        # validate_data_quality(master_sales, "sales")
        
        # Create spatial sales data
        sales_with_hex = sales_processor.create_spatial_sales(master_sales)
        
        # Create investor sales subset
        sales_investor = sales_processor.create_investor_sales_data(sales_with_hex)
        print(f"🏢 Identified {len(sales_investor):,} institutional investor transactions")
        
        # Aggregate by hex (12-month window)
        sales_hex_summary = sales_processor.aggregate_sales_by_hex(sales_with_hex, sales_investor)
        print(f"📊 Created hex sales aggregations for {len(sales_hex_summary):,} hexagons (12-month window)")
        
        # Aggregate by county (36-month window)
        sales_county_summary = sales_processor.aggregate_sales_by_county(sales_with_hex, sales_investor)
        print(f"📈 Created county sales aggregations for {len(sales_county_summary):,} county-month records (36-month window)")
        
        # ========== COMBINE SUMMARIES ==========
        print("\n🔄 Combining aggregated data...")
        
        # Combine hex summaries
        final_hex_summary = combine_hex_and_listings_summaries(sales_hex_summary, listings_hex_summary)
        print(f"📋 Final hex summary: {len(final_hex_summary):,} records")
        
        # ========== DATABASE UPLOAD ==========
        print("\n☁️ Uploading to Supabase...")
        
        # Initialize database manager
        db_manager = SupabaseManager(config)
        
        # Upload all data
        db_manager.upload_all_data(
            hex_summary=final_hex_summary,
            county_summary=sales_county_summary,
            listings_df=listings_with_hex,
            sales_df=sales_with_hex,
            today_formatted=config.today_formatted
        )
        
        print("\n🎉 ETL Pipeline completed successfully!")
        print(f"📊 Summary:")
        print(f"  • Listings processed: {len(master_listings):,}")
        print(f"  • Sales processed: {len(master_sales):,}")
        print(f"  • Hex aggregations: {len(final_hex_summary):,}")
        print(f"  • County-month records: {len(sales_county_summary):,}")
        
    except Exception as e:
        print(f"❌ Error in ETL pipeline: {str(e)}")
        print("🔍 Full traceback:")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
