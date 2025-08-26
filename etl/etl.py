import pandas as pd
import geopandas as gpd
from parcllabs import ParclLabsClient
from dotenv import load_dotenv
import os
from datetime import datetime
from dotenv import load_dotenv

# Get today's date in the format 'YYYY.MM.DD'
today = datetime.now().strftime('%Y.%m.%d')

# load API key
load_dotenv('config/.env')
parcl_api_key = os.getenv('PARCL_API_KEY')
client = ParclLabsClient(
    api_key=parcl_api_key
)

# set metro county dictionary
metro_counties = [
    'Cherokee', # metro 11
    # 'Clayton',
    # 'Cobb',
    # 'DeKalb',
    # 'Douglas',
    # 'Fayette',
    # 'Forsyth',
    # 'Fulton',
    # 'Gwinnett',
    # 'Henry',
    # 'Rockdale',
    # 'Barrow', # rest of the metro 29-county
    # 'Bartow',
    # 'Butts',
    # 'Carroll',
    # 'Coweta',
    # 'Dawson',
    # 'Haralson',
    # 'Heard',
    # 'Jasper',
    # 'Lumpkin',
    # 'Meriwether',
    # 'Morgan',
    # 'Newton',
    # 'Paulding',
    # 'Pickens',
    # 'Pike',
    # 'Spalding',
    # 'Walton',
]

# get FIPS codes for all Georgia counties 
df_fips = pd.read_csv(
    '../../../Geographies/FIPS_lookup.csv', dtype={'FIPS': 'str'})
df_fips['County_name'] = df_fips['County_name'].str.replace(' County', '')

df_fips_atl = df_fips[(df_fips['State'] == 'Georgia') & (df_fips['County_name'].isin(metro_counties))]
atl_fips_dict = pd.Series(
    df_fips_atl['FIPS'].values,
    index=df_fips_atl['County_name']
).to_dict()

county_id_map = {}

print('Getting Parcl IDs...')
for county_name, county_fips_code in atl_fips_dict.items():
    initial_query = client.search.markets.retrieve(
        geoid=county_fips_code
    )
    county_id = initial_query['parcl_id'].values[0]
    county_id_map[county_id] = county_name

county_id_map = {int(k): v for k, v in county_id_map.items()}


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


# create empty list to store all the cleaned listings
cleaned_listings = []

# get all the parcl ids for the counties in the metro
for parcl_id in county_id_map.keys():
    print(f'Getting listings for {county_id_map[parcl_id]}...')
    listings = client.property_v2.search.retrieve(
        parcl_ids=[parcl_id],
        event_names=["ALL_LISTINGS"],
        property_types=["SINGLE_FAMILY", "CONDO", "TOWNHOUSE", "OTHER"],
        current_on_market_flag=True,
        limit=50000,
        include_property_details=True,
        include_full_event_history=True,
    )

    listings = listings[0]

    # for any existing raw CSV files in "Uncleaned-Output" that start with "raw_listings", delete them and then export the new listings
    for file in os.listdir('Uncleaned-Output'):
        if file.startswith('raw_listings'):
            os.remove(f'Uncleaned-Output/{file}')

    # export the raw listings to a CSV file
    listings.to_csv(f'Uncleaned-Output/raw_listings_{county_id_map[parcl_id]}_{today}.csv', index=False)

    # clean the listings
    listings_cleaned = get_current_listings(listings)
    cleaned_listings.append(listings_cleaned)
    print(f'Found {len(listings_cleaned):,} listings in {county_id_map[parcl_id]}')

# create master_listing dataframe
master_listings = pd.concat(cleaned_listings, ignore_index=True)

# create a column called 'listing_delta' that is the difference between the current_list_price and the original_list_price
master_listings['listing_delta'] = master_listings['current_list_price'] - master_listings['original_list_price']
master_listings['listing_delta_pct'] = (master_listings['current_list_price'] - master_listings['original_list_price']) / master_listings['original_list_price'] * 100

# create GeoPandas geodataframe from master_listings and set crs to EPSG:4326
master_listings_gdf = gpd.GeoDataFrame(
    master_listings, 
    geometry=gpd.points_from_xy(master_listings['longitude'], master_listings['latitude']),
    crs='EPSG:4326'
    )

# read in hex values
hex_gdf = gpd.read_file('../../../Geographies/H3_geo/GA_hex_7.gpkg')
hex_gdf = hex_gdf.to_crs('EPSG:4326')

# run spatial join
listings_joined = gpd.sjoin(master_listings_gdf, hex_gdf, how='left', predicate='within')
listings_final = listings_joined.drop(columns=['index_right', 'County', 'geometry'])

# first set of aggregations: hex_id
listings_summary1 = listings_final.groupby('hex_id').agg(
    total_listings=('parcl_property_id','count'),
    median_list_price=('current_list_price','median'),
    median_list_price_sqft=('list_per_sq_ft','median'),
    median_DOM=('days_on_market','median'),
    median_listing_delta=('listing_delta','median'),
    median_listing_delta_pct=('listing_delta_pct','median'),
).reset_index()

listings_summary2 = listings_final[~listings_final['institutional_investor'].isna()].groupby('hex_id').agg(
    institutional_listings=('parcl_property_id','count'),
).reset_index()

listings_hex_summary = pd.merge(listings_summary1, listings_summary2, on='hex_id', how='left')

# second set of aggregations: county
listings_summary3 = listings_final.groupby('county_name').agg(
    total_listings=('parcl_property_id','count'),
    median_list_price=('current_list_price','median'),
    median_list_price_sqft=('list_per_sq_ft','median'),
    median_DOM=('days_on_market','median'),
    median_listing_delta=('listing_delta','median'),
    median_listing_delta_pct=('listing_delta_pct','median'),
).reset_index()

listings_summary4 = listings_final[~listings_final['institutional_investor'].isna()].groupby('county_name').agg(
    institutional_listings=('parcl_property_id','count'),
).reset_index()

listings_county_summary = pd.merge(listings_summary3, listings_summary4, on='county_name', how='left')

# for each of the three dataframes, add a column called 'date' with the current date
listings_final['as_of_date'] = today
listings_hex_summary['as_of_date'] = today
listings_county_summary['as_of_date'] = today

# look for any existing CSV files that start with 'listings_raw', 'listings_hex_summary', or 'listings_county_summary' and delete them
for file in os.listdir('.'):
    if file.startswith('listings_raw') or file.startswith('listings_hex_summary') or file.startswith('listings_county_summary'):
        os.remove(file)

# export the listings_final, listings_hex_summary, and listings_county_summary to CSV
listings_final.to_csv(f'listings_raw_{today}.csv', index=False)
listings_hex_summary.to_csv(f'listings_hex_summary_{today}.csv', index=False)
listings_county_summary.to_csv(f'listings_county_summary_{today}.csv', index=False)