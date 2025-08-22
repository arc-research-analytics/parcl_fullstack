# Phase 1: Project Setup & ETL Infrastructure

## Step 1: Create Project Structure

- Create a new directory for your project
- Set up three subdirectories: frontend/, backend/, and etl/
- Initialize Git repository in the root directory
- Create a public GitHub repository and push initial structure

## Step 2: Database Setup (Supabase)

- Create free Supabase account at supabase.com
- Create new project with a descriptive name
- Note down your project URL and API keys from project settings
- Navigate to the Table Editor in Supabase dashboard

## Step 3: ETL Pipeline Development

- Navigate to etl/ directory
- Create virtual environment for ETL Python dependencies

## Step 4: ETL Script Development

- Install ETL dependencies: pandas, supabase-py, requests, pydantic
- Create main ETL orchestrator script that coordinates extract, transform, load operations
- Implement data extraction logic (from CSV files, APIs, or other sources)
- Build data transformation functions (cleaning, validation, geocoding if needed)
- Create data loading functions to upsert data into Supabase
- Add comprehensive error handling and logging
- Implement data validation using Pydantic models

## Step 5: GitHub Actions Workflow Setup

- Create .github/workflows/etl-pipeline.yml
- Configure workflow to run weekly (cron schedule)
- Set up Python environment and dependency installation
- Configure Supabase credentials as GitHub secrets
- Add workflow steps for running ETL script, error notification, and logging
- Test workflow with manual trigger first

## Step 6: Initial Data Import & Pipeline Testing

- Clean your initial spreadsheet data manually
- Run ETL pipeline locally to load initial dataset
- Verify data appears correctly in Supabase table
- Test GitHub Actions workflow with a subset of data
- Validate data types and coordinate accuracy
- Set up monitoring and alerting for pipeline failures

## Step 7: Database Schema Optimization

- Use Supabase's built-in SQL editor to optimize table structure
- Add appropriate indexes for geographic queries and filtering
- Set up row-level security if needed
- Create views for common queries
- Test geographic coordinate validity and performance

# Phase 2: Backend Development

## Step 8: Set Up FastAPI Project

- Navigate to your backend/ directory
- Create virtual environment for Python dependencies
- Install FastAPI, Uvicorn, Supabase Python client, and other required packages
- Create requirements.txt file with all dependencies
- Set up basic FastAPI application structure with main.py file
- Add type hints throughout for better development experience

## Step 9: Environment Configuration

- Create .env file for local development secrets
- Add Supabase URL, API key, and your custom API key for frontend authentication
- Configure environment variable loading in your FastAPI app using Pydantic Settings
- Add .env to .gitignore to prevent committing secrets
- Create .env.example file with placeholder values for documentation

## Step 10: Database Integration

- Set up Supabase client connection in FastAPI
- Create database utility functions for connecting and querying
- Implement Pydantic models for your building data structure (reuse from ETL)
- Test basic database connectivity with simple query
- Implement error handling for database connection issues

## Step 11: API Endpoint Development

- Create /api/buildings GET endpoint that returns all building data
- Implement API key authentication middleware
- Add CORS configuration to allow requests from your Next.js app
- Transform database results into GeoJSON format for Mapbox compatibility
- Add filtering capabilities (by building type, ownership, size ranges)
- Implement proper error responses and HTTP status codes
- Add OpenAPI documentation with proper response models

## Step 12: Local Backend Testing

- Start FastAPI development server with hot reload enabled
- Test API endpoints using FastAPI's automatic documentation at /docs
- Verify GeoJSON output format is correct for Mapbox consumption
- Test authentication by making requests with and without valid API keys
- Ensure CORS is working by testing from different origins

# Phase 3: Frontend Development

## Step 13: Next.js Project Setup

- Navigate to frontend/ directory
- Create new Next.js application with TypeScript using create-next-app
- Install additional dependencies: Mapbox GL JS, React-Map-GL, axios for HTTP requests
- Set up environment variables for local development (.env.local)
- Configure TypeScript strict mode and path aliases in tsconfig.json
- Install and configure ESLint and Prettier for code quality

## Step 14: Basic Next.js App Structure

- Create TypeScript interfaces for your building data structure
- Set up component structure: App, Map, BuildingsList, Filters
- Implement basic layout with header, map container, and sidebar using App Router
- Create placeholder components for each major feature with proper TypeScript props
- Set up global styles and CSS modules or Tailwind CSS

## Step 15: Mapbox Integration

- Obtain Mapbox access token from Mapbox account
- Install and configure React-Map-GL with TypeScript
- Create Map component with proper TypeScript interfaces for props
- Set initial map view to focus on your building locations
- Test that map loads correctly with your desired style
- Handle server-side rendering considerations for Mapbox

## Step 16: API Integration

- Create service functions with TypeScript for making API calls to your FastAPI backend
- Implement data fetching with proper error handling and type safety
- Add loading states for better user experience using React hooks
- Test API connectivity between Next.js app and FastAPI backend
- Set up proper TypeScript types for API responses

## Step 17: Data Visualization on Map

- Fetch building data from your API when map loads
- Add GeoJSON source to Mapbox map with your building data
- Create map layers for displaying building points or polygons
- Implement different visual styling based on building properties
- Add hover effects to show building information on map interaction

## Step 18: Interactive Features

- Create popup component to display detailed building information
- Implement click handlers for map features to show building details
- Add zoom-to-building functionality when clicking on listings
- Create filter controls for building type, ownership, and size
- Implement search functionality to find specific buildings

## Step 19: UI/UX Polish

- Design and implement sidebar with building list view
- Add responsive design for mobile and tablet devices
- Implement proper loading states and error messages
- Add map controls (zoom, fullscreen, navigation)
- Style components with chosen UI library and ensure type safety

# Phase 4: Testing & Deployment

## Step 20: Local Testing & Debugging

- Test full application flow from data loading to map interaction
- Verify all filter combinations work correctly
- Test responsive design on different screen sizes
- Check browser console for any TypeScript errors or warnings
- Test API error scenarios and ETL pipeline failure scenarios

## Step 21: Backend Deployment

- Choose deployment platform (Railway, Render, or Heroku)
- Create account and new project on chosen platform
- Configure environment variables in deployment platform
- Set up automatic deployment from your GitHub repository
- Deploy backend and verify it's accessible via public URL

## Step 22: Frontend Deployment

- Choose deployment platform (Vercel recommended for Next.js)
- Connect your GitHub repository to deployment platform
- Configure environment variables for production
- Set up automatic deployment from your GitHub repository
- Deploy frontend and verify it loads correctly

## Step 23: Production Testing & ETL Monitoring

- Test full application functionality in production environment
- Verify ETL pipeline runs successfully in GitHub Actions
- Monitor ETL pipeline logs and set up failure notifications
- Test that new data from ETL pipeline appears in the application
- Set up monitoring for both application performance and data freshness

# Phase 5: Maintenance & Iteration

## Step 24: Data Management Workflow

- Document process for updating building data sources
- Create backup procedures for your database
- Test manual data updates through Supabase interface
- Verify ETL pipeline handles data changes gracefully
- Update TypeScript types if data structure changes

## Step 25: Monitoring & Analytics

- Set up basic error monitoring for frontend, backend, and ETL pipeline
- Implement usage analytics if desired (consider Next.js Analytics)
- Monitor API usage and performance
- Set up alerts for application downtime and ETL failures
- Monitor TypeScript compilation performance

## Step 26: Future Enhancements Planning

- Document potential features for future development
- Consider user feedback mechanisms
- Plan for scaling if usage grows (consider Next.js App Router features)
- Consider additional data sources or integration points
- Plan TypeScript refactoring as application grows
