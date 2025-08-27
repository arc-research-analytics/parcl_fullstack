import pandas as pd
import geopandas as gpd
from parcllabs import ParclLabsClient
from dotenv import load_dotenv
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from supabase import create_client
import traceback
import sys

def main():
    try:
        # Get today's date in the format 'YYYY.MM.DD'
        today = datetime.now()
        today_formatted = datetime.now().strftime('%Y.%m.%d')

        # set lookback lag & window size in months 
        lookback_lag = 4
        lookback_window = 12

        # calculate the max date for Parcl API
        three_months_ago = datetime.now() - relativedelta(months=lookback_lag)
        max_date = (three_months_ago.replace(day=1) + relativedelta(months=1, days=-1))
        max_date_formatted = max_date.strftime('%Y-%m-%d')

        # get min date in the format of 'YYYY-MM-DD'
        min_date = (today - relativedelta(months=lookback_lag+lookback_window - 1)).replace(day=1)
        min_date_formatted = min_date.strftime('%Y-%m-%d')

        # load API key
        load_dotenv('config/.env')
        parcl_api_key = os.getenv('PARCL_API_KEY')

        if not parcl_api_key:
            raise ValueError("PARCL_API_KEY is not set in the environment variables")

        client = ParclLabsClient(
            api_key=parcl_api_key
        )

        # dictionary to map the parcl_id to the county name
        county_id_map = {
            5821775: 'Barrow', 
            # 5823208: 'Bartow', 
            # 5824489: 'Butts', 
            # 5821127: 'Carroll', 
            # 5822987: 'Cherokee', 
            # 5821000: 'Clayton', 
            # 5822520: 'Cobb', 
            # 5820743: 'Coweta', 
            # 5820885: 'Dawson', 
            # 5821075: 'DeKalb', 
            # 5822002: 'Douglas', 
            # 5822843: 'Fayette', 
            # 5824605: 'Forsyth', 
            # 5823604: 'Fulton', 
            # 5822064: 'Gwinnett', 
            # 5823136: 'Haralson', 
            # 5821562: 'Heard', 
            # 5820830: 'Henry', 
            # 5820767: 'Jasper', 
            # 5824502: 'Lumpkin', 
            # 5822765: 'Meriwether', 
            # 5822014: 'Morgan', 
            # 5823086: 'Newton', 
            # 5822617: 'Paulding', 
            # 5821076: 'Pickens', 
            # 5822152: 'Pike', 
            # 5823393: 'Rockdale', 
            # 5824484: 'Spalding', 
            # 5821707: 'Walton'
            }

        # read in hex values for spatial joins
        hex_gdf = gpd.read_file('config/metro-hex.geojson')
        hex_gdf = hex_gdf.to_crs('EPSG:4326')

        # function to get and clean current listings from the ParclLabs API search V2 endpoint
        def get_current_listings(df):
            """
            Transform Parcl Labs property search V2 endpoint data to show only current listings with original and current pricing
            Vectorized implementation for performance
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

            result = result.rename(columns={
                'property_metadata_address1': 'address',
                'property_metadata_property_type': 'property_type',
                'property_metadata_sq_ft': 'square_feet',
                'property_metadata_year_built': 'year_built',
                'property_metadata_latitude': 'latitude',
                'property_metadata_longitude': 'longitude',
                'property_metadata_current_entity_owner_name': 'institutional_investor',
                'property_metadata_county_name': 'county_name'
            })

            # remove ' County' from the 'county_name' column
            result['county_name'] = result['county_name'].str.replace(' County', '')

            # create a column called 'list_per_SF' that is the 'current_list_price' divided by the 'sq_ft'
            result['list_per_sq_ft'] = result['current_list_price'] / result['square_feet']

            # create a column called 'unrealized_profit_ratio' that is the 'current_list_price' divided by the 'most_recent_sale_price'
            result['listing_to_sale_ratio'] = result['current_list_price'] / result['most_recent_sale_price']

            result = result[[
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

            # finally, remove duplicates based on parcl_property_id, keeping the first occurrence
            result = result.drop_duplicates(subset='parcl_property_id', keep='first')
            
            return result


        # ---------- listings by hex ----------
        # create empty list to store all the cleaned listings
        cleaned_listings = []

        # get all the parcl ids for the counties in the metro
        for parcl_id in county_id_map.keys():
            print(f'Getting listings for {county_id_map[parcl_id]}...')
            listings = client.property_v2.search.retrieve(
                parcl_ids=[parcl_id],
                event_names=["ALL_LISTINGS"],
                property_types=["SINGLE_FAMILY", "CONDO", "TOWNHOUSE"],
                current_on_market_flag=True,
                limit=50000,
                include_property_details=True,
                include_full_event_history=True,
                min_price=50000,
                min_sqft=500
            )

            listings = listings[0]

            # clean the listings
            listings_cleaned = get_current_listings(listings)
            cleaned_listings.append(listings_cleaned)
            print(f'-#-#-#-# Found {len(listings_cleaned):,} listings in {county_id_map[parcl_id]}')

        # create master_listing dataframe
        master_listings = pd.concat(cleaned_listings, ignore_index=True)

        # create GeoPandas geodataframe from master_listings and set crs to EPSG:4326
        master_listings_gdf = gpd.GeoDataFrame(
            master_listings, 
            geometry=gpd.points_from_xy(master_listings['longitude'], master_listings['latitude']),
            crs='EPSG:4326'
            )

        # run spatial join
        listings_joined = gpd.sjoin(master_listings_gdf, hex_gdf, how='left', predicate='within')
        listings_final = listings_joined.drop(columns=['index_right', 'resolution', 'geometry'])

        # aggregate listings by hex_id
        listings_summary1 = listings_final.groupby('h3_id').agg(
            total_listings=('parcl_property_id','count'),
            # median_list_price=('current_list_price','median'),
            median_list_price_sqft=('list_per_sq_ft','median'),
            # median_DOM=('days_on_market','median'),
        ).reset_index()

        listings_summary2 = listings_final[~listings_final['institutional_investor'].isna()].groupby('h3_id').agg(
            inst_listings=('parcl_property_id','count'),
        ).reset_index()

        listings_hex_summary = pd.merge(listings_summary1, listings_summary2, on='h3_id', how='left')

        # ---------- sales by hex ----------
        cleaned_sales = []

        for parcl_id in county_id_map.keys():
            print(f'Getting sales for {county_id_map[parcl_id]}...')
            raw_sales = client.property_v2.search.retrieve(
                parcl_ids=[parcl_id],
                event_names=["SOLD"],
                property_types=["SINGLE_FAMILY", "CONDO", "TOWNHOUSE"],
                current_on_market_flag=False,
                limit=50000,
                include_property_details=True,
                min_event_date=min_date_formatted,
                max_event_date=max_date_formatted,
                min_price=50000,
                min_sqft=500
            )

            raw_sales = raw_sales[0]

            print(f'-#-#-#-# Found {len(raw_sales):,} sales in {county_id_map[parcl_id]}')

            cleaned_sales.append(raw_sales)

        master_sales = pd.concat(cleaned_sales, ignore_index=True)

        # rename columns
        master_sales = master_sales.rename(columns={
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
        })

        # rearrange columns
        master_sales = master_sales[[
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

        # clean up 'county' column
        master_sales['county'] = master_sales['county'].str.replace(' County', '')

        # clean up 'property_type' column
        master_sales['property_type'] = master_sales['property_type'].str.replace('SINGLE_FAMILY', 'SFR')
        master_sales['property_type'] = master_sales['property_type'].str.replace('TOWNHOUSE', 'Townhouse')
        master_sales['property_type'] = master_sales['property_type'].str.replace('CONDO', 'Condo')

        # create a GeoDataframe from master_sales and set crs to EPSG:4326
        master_sales_gdf = gpd.GeoDataFrame(
            master_sales, 
            geometry=gpd.points_from_xy(master_sales['longitude'], master_sales['latitude']),
            crs='EPSG:4326'
        )

        # run spatial join on master sales
        sales_joined = gpd.sjoin(master_sales_gdf, hex_gdf, how='left', predicate='within')
        sales_joined = sales_joined.drop(columns=['index_right', 'resolution', 'geometry'])

        # create a new dataframe that filters to include only rows with a value in either 'buyer' or 'seller'
        sales_investor = sales_joined[sales_joined['buyer'].notna() | sales_joined['seller'].notna()]
        sales_investor = sales_investor.copy()
        sales_investor['sale_date'] = pd.to_datetime(sales_investor['sale_date'])
        sales_investor['year_month'] = sales_investor['sale_date'].dt.to_period('M')
        sales_investor['year_month'] = sales_investor['year_month'].astype(str)

        # remove any duplicate rows where 'county', 'sale_date', and 'sale_price' are the same
        sales_joined = sales_joined.drop_duplicates(subset=['county', 'sale_date', 'sale_price'])

        # calculate sale_per_sq_ft
        sales_joined['price_sf'] = sales_joined['sale_price'] / sales_joined['square_feet']

        # remove any rows where price_sf is excessive
        sales_joined = sales_joined[sales_joined['price_sf'] < 2500]

        # aggregate sales by hex_id
        sales_hex_summary1 = sales_joined.groupby('h3_id').agg(
            total_sales=('parcl_property_id','count'),
            median_vintage=('year_built','median'),
            median_size=('square_feet','median'),
            median_price_sf=('price_sf','median'),
        ).reset_index()

        # aggregate sales_investor by hex_id and count the number of 'buyer' and 'seller' values that aren't NA
        sales_hex_summary2 = sales_investor.groupby('h3_id').agg(
            inst_acquisitions=('buyer','count'),
            inst_dispositions=('seller','count'),
        ).reset_index()

        # merge sales_hex_summary1 and sales_hex_summary2 on 'h3_id'
        sales_hex_summary = pd.merge(sales_hex_summary1, sales_hex_summary2, on='h3_id', how='outer')

        # aggregate sales by county
        sales_joined = sales_joined.copy()
        sales_joined['sale_date'] = pd.to_datetime(sales_joined['sale_date'])
        sales_joined['year_month'] = sales_joined['sale_date'].dt.to_period('M')
        sales_joined['year_month'] = sales_joined['year_month'].astype(str)
        sales_county_summary1 = sales_joined.groupby(['county', 'year_month']).agg(
            total_sales=('parcl_property_id','count'),
            median_vintage=('year_built','median'),
            median_size=('square_feet','median'),
            median_price_sf=('price_sf','median'),
        ).reset_index()
        
        sales_county_summary2 = sales_investor.groupby(['county', 'year_month']).agg(
            inst_acquisitions=('buyer','count'),
            inst_dispositions=('seller','count'),
        ).reset_index()
        
        sales_county_summary = pd.merge(sales_county_summary1, sales_county_summary2, on=['county', 'year_month'], how='outer')

        # add a column called 'as_of_date' with the current date
        sales_hex_summary['as_of_date'] = today_formatted
        sales_county_summary['as_of_date'] = today_formatted
        sales_county_summary = sales_county_summary[[
            'county',
            'year_month',
            'as_of_date',
            'total_sales',
            'inst_acquisitions',
            'inst_dispositions',
            'median_vintage',
            'median_size',
            'median_price_sf',
        ]]

        # merge sales_hex_summary with listings_hex_summary
        final_hex_summary = pd.merge(sales_hex_summary, listings_hex_summary, on='h3_id', how='outer')

        # rearrange hex summary columns
        final_hex_summary = final_hex_summary[[
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

        # fill NaN values with 0
        final_hex_summary = final_hex_summary.fillna(0)

        # cast hex summary columns to int
        final_hex_summary['total_sales'] = final_hex_summary['total_sales'].astype(int)
        final_hex_summary['inst_acquisitions'] = final_hex_summary['inst_acquisitions'].astype(int)
        final_hex_summary['inst_dispositions'] = final_hex_summary['inst_dispositions'].astype(int)
        final_hex_summary['total_listings'] = final_hex_summary['total_listings'].astype(int)
        final_hex_summary['inst_listings'] = final_hex_summary['inst_listings'].astype(int)

        # cast county summary columns to int
        sales_county_summary['total_sales'] = sales_county_summary['total_sales'].astype(int)
        sales_county_summary['inst_acquisitions'] = sales_county_summary['inst_acquisitions'].astype(int)
        sales_county_summary['inst_dispositions'] = sales_county_summary['inst_dispositions'].astype(int)
        sales_county_summary['median_vintage'] = sales_county_summary['median_vintage'].astype(int)
        sales_county_summary['median_size'] = sales_county_summary['median_size'].astype(int)

        # # for testing purposes
        # final_hex_summary = pd.read_csv('test_hex_summary.csv')
        # sales_county_summary = pd.read_csv('test_county_summary.csv')

        # convert dataframes to dict format for supabase
        supabase_hex_summary = final_hex_summary.to_dict(orient='records')
        supabase_county_summary = sales_county_summary.to_dict(orient='records')

        # ---------- Supabase ----------

        # load environment variables
        SUPABASE_URL = os.getenv('SUPABASE_URL')
        SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')

        # establish connection to supabase
        print(f'Connecting to Supabase...')
        supabase = create_client(
            SUPABASE_URL,
            SUPABASE_KEY
        )

        # wipe existing rows in hex summary table
        supabase.table("hex_summary").delete().neq("h3_id", 0).execute()

        # insert new rows into hex_summary supabase table
        batch_size = 500
        for i in range(0, len(supabase_hex_summary), batch_size):
            batch = supabase_hex_summary[i:i+batch_size]
            supabase.table("hex_summary").insert(batch).execute()
            print(f"Inserted batch {i//batch_size + 1} of {len(supabase_hex_summary)//batch_size}")

        print('Hex summary rows added to Supabase🎉')

        # wipe existing rows in county summary table
        supabase.table("county_summary").delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()

        # insert new rows into county summary supabase table
        for i in range(0, len(supabase_county_summary), batch_size):
            batch = supabase_county_summary[i:i+batch_size]
            supabase.table("county_summary").insert(batch).execute()
            print(f"Inserted batch {i//batch_size + 1} of {len(supabase_county_summary)//batch_size}")

        print('County summary rows added to Supabase🎉')
        
    except Exception as e:
        print(f"Error: {str(e)}")
        print("Full traceback:")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()