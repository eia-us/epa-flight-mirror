# EPA GHG Data API (Lambda)

This Lambda function serves EPA FLIGHT API endpoints using DuckDB to query CSV data directly from S3.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Browser   │────▶│ API Gateway │────▶│   Lambda    │
│  (FLIGHT)   │     │             │     │  (DuckDB)   │
└─────────────┘     └─────────────┘     └──────┬──────┘
                                               │
                                               ▼
                                        ┌─────────────┐
                                        │     S3      │
                                        │ epa-backups │
                                        │   -eia      │
                                        └─────────────┘
```

## Data Source

The API reads from CSV files in:
- **Bucket**: `epa-backups-eia`
- **Prefix**: `epa_ghg_tables_csvs/`

Key tables used:
- `ghg.RLPS_GHG_EMITTER_FACILITIES.csv` - Facility information with lat/long
- `ghg.RLPS_GHG_EMITTER_SECTOR.csv` - Emissions by sector
- `ghg.RLPS_FAC_YEAR_AGG.csv` - Aggregated facility data by year
- `ghg.PUB_DIM_SECTOR.csv` - Sector definitions

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/version` | API version info |
| POST | `/api/facilities/map-markers` | Facility markers for map |
| POST | `/api/sectors/total/emissions` | Total emissions by sector |
| POST | `/api/list/sectors` | List of sectors |
| POST | `/api/list/facilities` | Paginated facility list |

## Local Development

### Prerequisites
- Python 3.11+
- AWS CLI configured with credentials
- DuckDB (`pip install duckdb`)

### Run locally
```bash
cd lambda_api
pip install -r requirements.txt
python local_server.py
```

The API will be available at `http://localhost:4000`

### Test endpoints
```bash
# Version
curl http://localhost:4000/ghgp/api/version

# Map markers
curl -X POST http://localhost:4000/ghgp/api/facilities/map-markers \
  -H "Content-Type: application/json" \
  -d '{"reportingYear": 2023}'

# Sectors
curl -X POST http://localhost:4000/ghgp/api/list/sectors \
  -H "Content-Type: application/json" \
  -d '{}'
```

## Deployment

### Prerequisites
- AWS SAM CLI (`brew install aws-sam-cli`)
- S3 bucket for deployment artifacts

### Deploy
```bash
# Set your deployment bucket
export S3_DEPLOY_BUCKET=your-bucket-name

# Deploy
chmod +x deploy.sh
./deploy.sh
```

### Update FLIGHT to use Lambda API

After deployment, update `flight/config.json`:
```json
{
    "BASE_API_URL": "https://xxx.execute-api.us-east-1.amazonaws.com/prod/ghgp/api"
}
```

## Performance Optimization

For better performance, consider:

1. **Convert CSVs to Parquet**: DuckDB queries Parquet files 5-10x faster
   ```python
   import duckdb
   duckdb.query(f"COPY (SELECT * FROM read_csv_auto('file.csv')) TO 'file.parquet'")
   ```

2. **Lambda Provisioned Concurrency**: Keeps Lambda warm to avoid cold starts

3. **API Gateway Caching**: Cache GET responses for frequently accessed data

4. **Pre-aggregated tables**: Create summary tables for common queries

## Adding New Endpoints

1. Add a handler function in `handler.py`:
   ```python
   def handle_my_endpoint(event):
       # Your logic here
       return cors_response(200, {'result': data, 'messages': []})
   ```

2. Add route to `ROUTES` dict:
   ```python
   ROUTES = {
       ...
       ('POST', '/api/my-endpoint'): handle_my_endpoint,
   }
   ```

## Cost Estimation

- Lambda: ~$0.20 per 1M requests (128MB, 100ms avg)
- API Gateway: ~$3.50 per 1M requests
- S3 GET: ~$0.40 per 1M requests

For moderate traffic (~100k requests/month): **< $1/month**
