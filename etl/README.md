# ETL Pipeline Refactoring

This directory contains a refactored version of the ETL pipeline that processes real estate data from Parcl Labs API and uploads it to Supabase.

## 🏗️ Architecture

The refactored pipeline follows a modular architecture with clear separation of concerns:

```
etl/
├── config.py              # Configuration management
├── api_client.py           # Parcl Labs API interactions
├── data_processors.py      # Data cleaning and transformation
├── database_operations.py  # Supabase database operations
├── utils.py               # Utility functions
├── etl.py                 # Main orchestrator
└── README.md              # This file
```

## 📋 Modules Overview

### `config.py`

- **ETLConfig class**: Centralizes all configuration settings
- Manages date calculations (lookback_lag, lookback_window)
- Contains county ID mappings and column mappings
- Handles environment variable loading

### `api_client.py`

- **ParclAPIClient class**: Wrapper for Parcl Labs API
- Standardizes API calls for listings and sales
- Handles batch processing across counties
- Provides consistent logging

### `data_processors.py`

- **ListingsProcessor class**: Handles listings data transformation
  - `get_current_listings()`: Transforms raw API data to current listings
  - `clean_and_standardize_listings()`: Standardizes column names and data types
  - `aggregate_listings_by_hex()`: Creates H3 hexagon aggregations
- **SalesProcessor class**: Handles sales data transformation
  - `apply_data_quality_filters()`: Removes duplicates and outliers
  - `create_investor_sales_data()`: Identifies institutional transactions
  - `aggregate_sales_by_hex()`: 12-month hex aggregations
  - `aggregate_sales_by_county()`: 36-month county aggregations

### `database_operations.py`

- **SupabaseManager class**: Manages all database interactions
- Handles data type conversion and preparation
- Provides batch upload functionality
- Manages table clearing and insertion

### `utils.py`

- Common utility functions
- Data validation helpers
- Hex data loading utilities

### `etl.py`

- **Main orchestrator**: Coordinates the entire pipeline
- Clear step-by-step execution with logging
- Error handling and progress tracking

## 🔧 Key Configuration Parameters

The pipeline is controlled by two main parameters in `config.py`:

- **`lookback_lag`**: 2 months - API data availability delay
- **`lookback_window`**: 36 months - Full data range to pull
- **`hex_aggregation_window`**: 12 months - Hex-level aggregation period

### Data Flow

1. **36-month data pull**: All raw data (sales & listings) goes back 36 months
2. **County aggregations**: Use full 36-month dataset for trend analysis
3. **Hex aggregations**: Use only last 12 months for choropleth mapping
4. **Unaggregated data**: Full 36-month dataset uploaded to Supabase

## 📊 Data Quality Features

- **Price filtering**: Removes sales above $2,500/sq ft
- **Duplicate removal**: Filters out multi-property sales
- **Data validation**: Built-in quality checks and logging
- **Institutional investor tracking**: Separate processing for institutional transactions

## 🚀 Usage

To run the pipeline:

```bash
cd etl
python etl.py
```

## 📈 Improvements Over Original

1. **Modular Architecture**: Separated concerns into focused classes
2. **Configuration Management**: Centralized settings and mappings
3. **Error Handling**: More granular error reporting and recovery
4. **Data Validation**: Built-in quality checks and logging
5. **Code Reusability**: Reusable components for future enhancements
6. **Maintainability**: Clear structure makes updates and debugging easier
7. **Documentation**: Comprehensive docstrings and comments

## 🔄 Architecture Notes

This modular pipeline replaces the original monolithic script while producing identical results. All existing database schemas and data formats remain unchanged. The main improvement is significantly better code organization and maintainability through separation of concerns.
