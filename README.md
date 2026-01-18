# EPA FLIGHT Mirror

A self-hosted mirror of the EPA's Facility Level Information on GreenHouse gases Tool (FLIGHT), using AWS infrastructure to serve the application entirely from AWS resources without dependencies on the original EPA endpoints.

## Overview

This project replicates the EPA FLIGHT web application using:
- **S3 Static Website Hosting** for the frontend
- **AWS Lambda with Function URLs** for the API backend
- **DuckDB** for querying Parquet data files stored in S3
- **Parquet files** converted from original EPA database tables

**Live Site**: http://epa-flight-mirror.s3-website-us-east-1.amazonaws.com/flight/

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              User Browser                                │
└─────────────────────────────────┬───────────────────────────────────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    │                           │
                    ▼                           ▼
┌───────────────────────────────┐   ┌───────────────────────────────────┐
│   S3 Static Website Hosting   │   │   AWS Lambda (Function URL)       │
│   epa-flight-mirror bucket    │   │   epa-ghg-api                     │
│                               │   │                                   │
│   /flight/                    │   │   Runtime: Python 3.11 (ARM64)    │
│   ├── index.html              │   │   Memory: 1024 MB                 │
│   ├── config.json             │   │   Uses DuckDB for SQL queries     │
│   ├── assets/                 │   │                                   │
│   │   ├── *.js                │   │   Endpoints:                      │
│   │   ├── *.css               │   │   POST /ghgp/api/facilities/...   │
│   │   └── fonts/              │   │   POST /ghgp/api/sectors/...      │
│   └── imgs/                   │   │   POST /ghgp/api/export           │
│       └── icon_marker/        │   │   etc.                            │
└───────────────────────────────┘   └─────────────────┬─────────────────┘
                                                      │
                                                      ▼
                                    ┌───────────────────────────────────┐
                                    │   S3 Data Bucket                  │
                                    │   epa-backups-eia                 │
                                    │                                   │
                                    │   /epa_ghg_tables_parquet/        │
                                    │   ├── ghg.RLPS_GHG_EMITTER_       │
                                    │   │   FACILITIES.parquet          │
                                    │   ├── ghg.RLPS_GHG_EMITTER_       │
                                    │   │   SECTOR.parquet              │
                                    │   ├── ghg.PUB_DIM_SECTOR.parquet  │
                                    │   └── ... (446 parquet files)     │
                                    └───────────────────────────────────┘
```

## Project Structure

```
epa_test_site/
├── README.md                    # This file
├── package.json                 # Node.js dependencies (for local dev server)
├── server.js                    # Local development proxy server
├── flight/                      # Frontend application (mirrors EPA FLIGHT)
│   ├── index.html               # Main HTML entry point
│   ├── config.json              # API configuration (points to Lambda URL)
│   ├── assets/                  # JavaScript, CSS, fonts
│   │   ├── Index-DmDDJjxp.js    # Main application bundle
│   │   ├── *.css                # Stylesheets
│   │   └── *.woff2              # Web fonts
│   └── imgs/                    # Images and icons
│       └── icon_marker/         # Map marker icons
└── lambda_api/                  # Lambda function code
    ├── lambda_function.py       # Main Lambda handler with all endpoints
    ├── local_server.py          # Local Flask server for testing
    ├── convert_to_parquet.py    # Script to convert CSV to Parquet
    ├── deploy.sh                # SAM deployment script
    ├── deploy-cli.sh            # AWS CLI deployment script
    ├── template.yaml            # SAM template
    ├── requirements.txt         # Python dependencies
    └── README.md                # Lambda-specific documentation
```

## Components

### 1. Frontend (S3 Static Website)

The frontend is a Vue.js single-page application originally built by EPA. Key modifications:
- **config.json**: Points `BASE_API_URL` to our Lambda Function URL instead of EPA
- **JavaScript**: Hardcoded EPA image URLs replaced with local `/flight/` paths

**S3 Bucket**: `epa-flight-mirror`
**Website URL**: http://epa-flight-mirror.s3-website-us-east-1.amazonaws.com/flight/

### 2. API Backend (AWS Lambda)

A Python Lambda function that implements the FLIGHT API endpoints using DuckDB to query Parquet files.

**Function Name**: `epa-ghg-api`
**Function URL**: https://2a56uptno4ikx5wiar6lhcgd7m0ngigk.lambda-url.us-east-1.on.aws/

#### API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/ghgp/api/version` | API version info |
| POST | `/ghgp/api/facilities/map-markers` | Facility markers for map view |
| POST | `/ghgp/api/map/overlay` | Emissions overlay data |
| POST | `/ghgp/api/sectors/total/emissions` | Total emissions by sector |
| POST | `/ghgp/api/list/sectors` | List of industry sectors |
| POST | `/ghgp/api/list/facilities` | Paginated facility list |
| POST | `/ghgp/api/bar/sector` | Bar chart data (Level 1 sectors) |
| POST | `/ghgp/api/bar/sector/level2` | Bar chart data (Level 2 subsectors) |
| POST | `/ghgp/api/pie/sectors/emissions` | Pie chart data (Level 1) |
| POST | `/ghgp/api/pie/level2/sector/emissions` | Pie chart data (Level 2) |
| POST | `/ghgp/api/pie/level3/subsector/emissions` | Pie chart data (Level 3) |
| GET | `/ghgp/api/sector/trend/{id}/{level}` | Line chart trend data |
| POST | `/ghgp/api/export` | CSV export (single year) |
| POST | `/ghgp/api/export?allReportingYears=true` | CSV export (all years, max 25K rows) |
| GET | `/ghgp/api/state/bounds/{state}` | State boundary data |
| GET | `/ghgp/api/state/counties/{state}` | State counties data |
| GET | `/ghgp/api/basin/geo` | Basin geographic data |
| POST | `/ghgp/api/facility/hover/{year}` | Facility hover tooltip data |

#### Request/Response Format

Most endpoints accept POST requests with JSON body:

```json
{
    "reportingYear": 2023,
    "state": "US",
    "dataSource": "E",
    "sector1": "",
    "pageSize": 1000,
    "pageNumber": 1
}
```

Responses follow this structure:

```json
{
    "result": { ... },
    "messages": []
}
```

### 3. Data Storage (S3 Parquet Files)

EPA GHG data converted from CSV to Parquet format for efficient querying.

**Bucket**: `epa-backups-eia`
**Prefix**: `epa_ghg_tables_parquet/`
**File Count**: 446 Parquet files

Key tables used by the API:
- `ghg.RLPS_GHG_EMITTER_FACILITIES.parquet` - Facility information (name, location, coordinates)
- `ghg.RLPS_GHG_EMITTER_SECTOR.parquet` - Emissions data by sector and facility
- `ghg.PUB_DIM_SECTOR.parquet` - Sector definitions and hierarchy

### 4. Local Development Server

A Node.js server for local development that:
- Serves the frontend from `/flight/`
- Proxies API requests to either EPA (original) or Lambda (mirror)

## Setup & Deployment

### Prerequisites

- AWS CLI configured with appropriate credentials
- Python 3.11+
- Node.js (for local development)

### Deploy Lambda Function

```bash
cd lambda_api

# Install dependencies in a package directory
pip install -r requirements.txt -t package/
cp lambda_function.py package/

# Create deployment package
cd package && zip -r ../deploy.zip . && cd ..

# Deploy to Lambda
aws lambda update-function-code \
    --function-name epa-ghg-api \
    --zip-file fileb://deploy.zip
```

### Update Frontend

```bash
# Sync frontend to S3
aws s3 sync flight/ s3://epa-flight-mirror/flight/ --delete

# Set bucket policy for public access (if needed)
aws s3api put-bucket-policy --bucket epa-flight-mirror --policy file://bucket-policy.json
```

### Local Development

```bash
# Run local server (proxies to Lambda API)
node server.js

# Open browser
open http://localhost:3000/flight/?viewType=map
```

Or run Lambda locally:

```bash
cd lambda_api
pip install flask duckdb boto3
python local_server.py

# Test endpoint
curl -X POST http://localhost:4000/ghgp/api/list/sectors -H "Content-Type: application/json" -d '{}'
```

## Configuration

### Frontend Configuration (`flight/config.json`)

```json
{
    "BASE_API_URL": "https://2a56uptno4ikx5wiar6lhcgd7m0ngigk.lambda-url.us-east-1.on.aws/ghgp/api"
}
```

### Lambda Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `S3_BUCKET` | `epa-backups-eia` | S3 bucket containing Parquet files |
| `S3_PREFIX` | `epa_ghg_tables_parquet` | Prefix/folder for Parquet files |
| `S3_REGION` | `us-east-1` | AWS region |

## Technical Details

### DuckDB Integration

The Lambda function uses DuckDB to query Parquet files. Files are downloaded from S3 to `/tmp` on first access and cached for subsequent queries within the same Lambda invocation.

```python
# Example query pattern
facilities_path = get_local_parquet("RLPS_GHG_EMITTER_FACILITIES")
query = f"""
    SELECT facility_id, latitude, longitude
    FROM read_parquet('{facilities_path}')
    WHERE year = '2023'
"""
result = db.execute(query).fetchall()
```

### Emissions Units

- Raw data is in **metric tons CO2e**
- Chart displays use **MMT (Million Metric Tons)** - divided by 1,000,000
- Export CSV provides raw metric tons

### Lambda Limits

- Response size limit: 6 MB (affects large exports)
- All-years export limited to 25,000 rows to stay within limit
- Timeout: 30 seconds (configurable)

## Cost Estimate

For moderate usage (~100k requests/month):

| Service | Estimated Cost |
|---------|---------------|
| Lambda | ~$0.50/month |
| S3 Storage | ~$5/month (for ~10GB Parquet) |
| S3 Requests | ~$0.50/month |
| Data Transfer | ~$1/month |
| **Total** | **~$7/month** |

## Troubleshooting

### Common Issues

1. **CORS Errors**: Lambda Function URLs handle CORS automatically. Don't add duplicate headers.

2. **413 Payload Too Large**: Export queries return too much data. The all-years export is limited to 25,000 rows.

3. **Cold Start Latency**: First request after idle period may take 2-3 seconds. DuckDB and Parquet files need to initialize.

4. **Missing Data**: Ensure Parquet files exist in S3 and Lambda has permission to read them.

### Viewing Lambda Logs

```bash
aws logs tail /aws/lambda/epa-ghg-api --follow
```

## Data Sources

Original data from EPA GHGRP (Greenhouse Gas Reporting Program):
- https://ghgdata.epa.gov/ghgp/
- Data years: 2010-2023

## License

This project mirrors publicly available EPA data. The original FLIGHT application and data are provided by the U.S. Environmental Protection Agency.
