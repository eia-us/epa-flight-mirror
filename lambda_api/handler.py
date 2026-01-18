"""
EPA GHG Data API - Lambda Handler
Serves FLIGHT API endpoints using DuckDB to query Parquet data from S3
"""

import json
import os
import duckdb
from datetime import datetime

# S3 bucket configuration
S3_BUCKET = os.environ.get('S3_BUCKET', 'epa-backups-eia')
S3_PREFIX = os.environ.get('S3_PREFIX', 'epa_ghg_tables_parquet')

# DuckDB connection (reused across invocations)
conn = None

def get_connection():
    """Get or create DuckDB connection with S3 access"""
    global conn
    if conn is None:
        conn = duckdb.connect(':memory:')
        # Install and load httpfs for S3 access
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")
        # Configure S3 access
        conn.execute("SET s3_region='us-east-1'")
        # Use credential_chain for local development (picks up AWS CLI credentials)
        # In Lambda, this will use the IAM role automatically
        conn.execute("SET s3_access_key_id=''")
        conn.execute("SET s3_secret_access_key=''")
        conn.execute("CREATE SECRET (TYPE S3, PROVIDER CREDENTIAL_CHAIN)")
    return conn

def s3_path(table_name):
    """Generate S3 path for a table"""
    return f"s3://{S3_BUCKET}/{S3_PREFIX}/ghg.{table_name}.parquet"

def cors_response(status_code, body):
    """Return response with CORS headers"""
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Accept'
        },
        'body': json.dumps(body)
    }

def handle_version(event):
    """GET /api/version - Return API version info"""
    return cors_response(200, {
        'result': {
            'reportedDate': datetime.now().strftime('%m/%d/%Y'),
            'releaseNumber': 'Local-1.0'
        },
        'messages': []
    })

def handle_facilities_map_markers(event):
    """POST /api/facilities/map-markers - Return facility markers for map"""
    try:
        body = json.loads(event.get('body', '{}'))
        reporting_year = body.get('reportingYear', 2023)
        low_e = float(body.get('lowE', 0))
        high_e = float(body.get('highE', 1e12))
        state = body.get('state', 'US')
        sectors = body.get('selectedSectorAndSubsectors', [])

        # Extract sector names
        sector_names = [s.get('sectorName', '') for s in sectors if s.get('sectorName')]

        db = get_connection()

        # Query facilities with emissions data
        query = f"""
            SELECT DISTINCT
                f.facility_id as id,
                CAST(f.latitude AS DOUBLE) as lt,
                CAST(f.longitude AS DOUBLE) as ln
            FROM read_parquet('{s3_path("RLPS_GHG_EMITTER_FACILITIES")}') f
            WHERE f.year = '{reporting_year}'
                AND f.latitude IS NOT NULL
                AND f.longitude IS NOT NULL
                AND f.latitude != ''
                AND f.longitude != ''
        """

        # Add state filter if not US-wide
        if state and state != 'US':
            query += f" AND f.state = '{state}'"

        result = db.execute(query).fetchall()

        markers = [{'id': int(row[0]), 'lt': row[1], 'ln': row[2]} for row in result]

        return cors_response(200, {
            'result': markers,
            'messages': []
        })

    except Exception as e:
        return cors_response(500, {
            'messages': [{'text': str(e), 'type': 500}]
        })

def handle_sectors_total_emissions(event):
    """POST /api/sectors/total/emissions - Return total emissions by sector"""
    try:
        body = json.loads(event.get('body', '{}'))
        reporting_year = body.get('reportingYear', 2023)

        db = get_connection()

        query = f"""
            SELECT
                sector_name,
                SUM(CAST(co2e_emission AS DOUBLE)) as total_emissions,
                COUNT(DISTINCT facility_id) as facility_count
            FROM read_parquet('{s3_path("RLPS_GHG_EMITTER_SECTOR")}')
            WHERE year = '{reporting_year}'
                AND sector_type = 'E'
            GROUP BY sector_name
            ORDER BY total_emissions DESC
        """

        result = db.execute(query).fetchall()

        sectors = []
        for row in result:
            sectors.append({
                'sectorName': row[0],
                'totalEmissions': float(row[1]) if row[1] else 0,
                'facilityCount': row[2]
            })

        return cors_response(200, {
            'result': sectors,
            'messages': []
        })

    except Exception as e:
        return cors_response(500, {
            'messages': [{'text': str(e), 'type': 500}]
        })

def handle_list_sectors(event):
    """POST /api/list/sectors - Return list of sectors with details"""
    try:
        body = json.loads(event.get('body', '{}'))
        reporting_year = body.get('reportingYear', 2023)

        db = get_connection()

        query = f"""
            SELECT
                sector_id,
                sector_name,
                sector_code,
                sector_color,
                sector_type
            FROM read_parquet('{s3_path("PUB_DIM_SECTOR")}')
            WHERE sector_type = 'E'
            ORDER BY sort_order
        """

        result = db.execute(query).fetchall()

        sectors = []
        for row in result:
            sectors.append({
                'sectorId': int(row[0]) if row[0] else None,
                'sectorName': row[1],
                'sectorCode': row[2],
                'sectorColor': row[3],
                'sectorType': row[4]
            })

        return cors_response(200, {
            'result': sectors,
            'messages': []
        })

    except Exception as e:
        return cors_response(500, {
            'messages': [{'text': str(e), 'type': 500}]
        })

def handle_list_facilities(event):
    """POST /api/list/facilities - Return paginated list of facilities"""
    try:
        body = json.loads(event.get('body', '{}'))
        reporting_year = body.get('reportingYear', 2023)
        page = int(body.get('pageNumber', 1)) if body.get('pageNumber') else 1
        page_size = 100
        offset = (page - 1) * page_size

        db = get_connection()

        query = f"""
            SELECT
                f.facility_id,
                f.facility_name,
                f.city,
                f.state,
                f.county,
                CAST(f.latitude AS DOUBLE) as latitude,
                CAST(f.longitude AS DOUBLE) as longitude,
                CAST(a.total_emission AS DOUBLE) as total_emission
            FROM read_parquet('{s3_path("RLPS_GHG_EMITTER_FACILITIES")}') f
            LEFT JOIN read_parquet('{s3_path("RLPS_FAC_YEAR_AGG")}') a
                ON f.facility_id = a.pgm_sys_id AND f.year = a.year
            WHERE f.year = '{reporting_year}'
            ORDER BY total_emission DESC NULLS LAST
            LIMIT {page_size} OFFSET {offset}
        """

        result = db.execute(query).fetchall()

        facilities = []
        for row in result:
            facilities.append({
                'facilityId': int(row[0]) if row[0] else None,
                'facilityName': row[1],
                'city': row[2],
                'state': row[3],
                'county': row[4],
                'latitude': row[5],
                'longitude': row[6],
                'totalEmissions': row[7] if row[7] else 0
            })

        return cors_response(200, {
            'result': facilities,
            'messages': []
        })

    except Exception as e:
        return cors_response(500, {
            'messages': [{'text': str(e), 'type': 500}]
        })

def handle_state_bounds(event, state_code):
    """GET /api/state/bounds/{state} - Return state boundary GeoJSON"""
    # For now, return empty - would need GeoJSON data
    return cors_response(200, {
        'messages': []
    })

# Route mapping
ROUTES = {
    ('GET', '/api/version'): handle_version,
    ('POST', '/api/facilities/map-markers'): handle_facilities_map_markers,
    ('POST', '/api/sectors/total/emissions'): handle_sectors_total_emissions,
    ('POST', '/api/list/sectors'): handle_list_sectors,
    ('POST', '/api/list/facilities'): handle_list_facilities,
}

def lambda_handler(event, context):
    """Main Lambda handler"""

    # Handle CORS preflight
    if event.get('httpMethod') == 'OPTIONS':
        return cors_response(204, '')

    method = event.get('httpMethod', 'GET')
    path = event.get('path', '')

    # Normalize path (remove /ghgp prefix if present)
    if path.startswith('/ghgp'):
        path = path[5:]

    # Find matching route
    handler = ROUTES.get((method, path))

    if handler:
        return handler(event)

    # Handle state bounds with path parameter
    if path.startswith('/api/state/bounds/'):
        state_code = path.split('/')[-1]
        return handle_state_bounds(event, state_code)

    # 404 for unknown routes
    return cors_response(404, {
        'messages': [{'text': f'Route not found: {method} {path}', 'type': 404}]
    })


# For local testing
if __name__ == '__main__':
    # Test version endpoint
    print(handle_version({}))
