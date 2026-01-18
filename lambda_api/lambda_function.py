"""
EPA GHG Data API - Lambda Function
Serves FLIGHT API endpoints using DuckDB to query Parquet data from S3
Uses boto3 to download files (avoids httpfs extension issues on Lambda ARM64)
"""

import json
import os
import duckdb
import boto3
import csv
import io
import base64
from datetime import datetime

# S3 bucket configuration
S3_BUCKET = os.environ.get('S3_BUCKET', 'epa-backups-eia')
S3_PREFIX = os.environ.get('S3_PREFIX', 'epa_ghg_tables_parquet')
S3_REGION = os.environ.get('S3_REGION', 'us-east-1')

# Initialize S3 client
s3_client = boto3.client('s3', region_name=S3_REGION)

# Cache for downloaded files and DuckDB connection
file_cache = {}
conn = None

def get_connection():
    """Get or create DuckDB connection"""
    global conn
    if conn is None:
        conn = duckdb.connect(':memory:')
    return conn

def get_local_parquet(table_name):
    """Download Parquet file from S3 to /tmp if not cached, return local path"""
    s3_key = f"{S3_PREFIX}/ghg.{table_name}.parquet"
    local_path = f"/tmp/ghg.{table_name}.parquet"

    # Check if already downloaded in this invocation
    if table_name in file_cache:
        return file_cache[table_name]

    # Check if file exists from a previous warm invocation
    if os.path.exists(local_path):
        file_cache[table_name] = local_path
        return local_path

    # Download from S3
    try:
        s3_client.download_file(S3_BUCKET, s3_key, local_path)
        file_cache[table_name] = local_path
        return local_path
    except Exception as e:
        raise Exception(f"Failed to download {s3_key}: {e}")

# GeoJSON cache
geo_cache = {}

def get_geo_json(filename):
    """Download GeoJSON file from S3 and cache it"""
    if filename in geo_cache:
        return geo_cache[filename]

    s3_key = f"epa_ghg_geo/{filename}"
    local_path = f"/tmp/{filename}"

    # Check if file exists from a previous warm invocation
    if os.path.exists(local_path):
        with open(local_path, 'r') as f:
            data = json.load(f)
            geo_cache[filename] = data
            return data

    # Download from S3
    try:
        s3_client.download_file(S3_BUCKET, s3_key, local_path)
        with open(local_path, 'r') as f:
            data = json.load(f)
            geo_cache[filename] = data
            return data
    except Exception as e:
        raise Exception(f"Failed to download {s3_key}: {e}")

def cors_response(status_code, body):
    """Return response - CORS headers handled by Function URL config"""
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json'
        },
        'body': json.dumps(body)
    }

def handle_version(event):
    """GET /api/version - Return API version info"""
    return cors_response(200, {
        'result': {
            'reportedDate': datetime.now().strftime('%m/%d/%Y'),
            'releaseNumber': 'EIA-Lambda-1.0'
        },
        'messages': []
    })

def handle_facilities_map_markers(event):
    """POST /api/facilities/map-markers - Return facility markers for map"""
    try:
        body = json.loads(event.get('body', '{}') or '{}')
        reporting_year = body.get('reportingYear', 2023)
        state = body.get('state', 'US')

        # Download required Parquet file
        facilities_path = get_local_parquet("RLPS_GHG_EMITTER_FACILITIES")

        db = get_connection()

        query = f"""
            SELECT DISTINCT
                facility_id as id,
                CAST(latitude AS DOUBLE) as lt,
                CAST(longitude AS DOUBLE) as ln
            FROM read_parquet('{facilities_path}')
            WHERE year = '{reporting_year}'
                AND latitude IS NOT NULL
                AND longitude IS NOT NULL
                AND latitude != ''
                AND longitude != ''
        """

        if state and state != 'US':
            query += f" AND state = '{state}'"

        result = db.execute(query).fetchall()
        markers = [{'id': int(row[0]), 'lt': row[1], 'ln': row[2]} for row in result]

        return cors_response(200, {'result': markers, 'messages': []})

    except Exception as e:
        print(f"Error in map_markers: {e}")
        return cors_response(500, {'messages': [{'text': str(e), 'type': 500}]})

def handle_map_overlay(event):
    """POST /api/map/overlay - Return facility markers with emissions for overlay view"""
    try:
        body = json.loads(event.get('body', '{}') or '{}')
        reporting_year = body.get('reportingYear', 2023)
        state = body.get('state', 'US')
        data_source = body.get('dataSource', 'E')

        # Download required Parquet files
        facilities_path = get_local_parquet("RLPS_GHG_EMITTER_FACILITIES")
        sector_path = get_local_parquet("RLPS_GHG_EMITTER_SECTOR")

        db = get_connection()

        # Get facilities with their total emissions (excluding Biogenic CO2)
        query = f"""
            SELECT
                f.facility_id as id,
                CAST(f.latitude AS DOUBLE) as lt,
                CAST(f.longitude AS DOUBLE) as ln,
                SUM(CAST(s.co2e_emission AS DOUBLE)) as emissions
            FROM read_parquet('{facilities_path}') f
            LEFT JOIN read_parquet('{sector_path}') s
                ON f.facility_id = s.facility_id AND f.year = s.year
            WHERE f.year = '{reporting_year}'
                AND f.latitude IS NOT NULL
                AND f.longitude IS NOT NULL
                AND f.latitude != ''
                AND f.longitude != ''
                AND s.sector_type = '{data_source}'
                AND s.gas_name != 'Biogenic CO2'
        """

        if state and state != 'US':
            query += f" AND f.state = '{state}'"

        query += " GROUP BY f.facility_id, f.latitude, f.longitude"

        result = db.execute(query).fetchall()

        # Convert emissions to metric tons (not MMT for overlay - used for sizing)
        markers = [{
            'id': int(row[0]) if row[0] else None,
            'lt': row[1],
            'ln': row[2],
            'emissions': round(float(row[3])) if row[3] else 0
        } for row in result if row[1] and row[2]]

        return cors_response(200, {'result': markers, 'messages': []})

    except Exception as e:
        print(f"Error in map_overlay: {e}")
        return cors_response(500, {'messages': [{'text': str(e), 'type': 500}]})

def handle_sectors_total_emissions(event):
    """POST /api/sectors/total/emissions - Return total emissions by sector"""
    try:
        body = json.loads(event.get('body', '{}') or '{}')
        reporting_year = body.get('reportingYear', 2023)
        state = body.get('state', 'US')

        # Download required Parquet files
        sector_path = get_local_parquet("RLPS_GHG_EMITTER_SECTOR")
        dim_sector_path = get_local_parquet("PUB_DIM_SECTOR")
        facilities_path = get_local_parquet("RLPS_GHG_EMITTER_FACILITIES")

        db = get_connection()

        # Build query with optional state filter
        state_join = ""
        state_filter = ""
        if state and state != 'US':
            state_join = f"INNER JOIN read_parquet('{facilities_path}') f ON e.facility_id = f.facility_id AND e.year = f.year"
            state_filter = f"AND f.state = '{state}'"

        query = f"""
            SELECT
                e.sector_name,
                SUM(CAST(e.co2e_emission AS DOUBLE)) as total_emissions,
                COUNT(DISTINCT e.facility_id) as facility_count,
                MAX(CAST(d.sector_id AS INTEGER)) as sector_id
            FROM read_parquet('{sector_path}') e
            LEFT JOIN read_parquet('{dim_sector_path}') d
                ON e.sector_name = d.sector_name
            {state_join}
            WHERE e.year = '{reporting_year}'
                AND e.sector_type = 'E'
                AND e.gas_name != 'Biogenic CO2'
                {state_filter}
            GROUP BY e.sector_name
            ORDER BY total_emissions DESC
        """

        result = db.execute(query).fetchall()

        # Convert from metric tons to MMT (Million Metric Tons) and round
        MMT_DIVISOR = 1_000_000

        total_emissions = round(sum(float(row[1]) if row[1] else 0 for row in result) / MMT_DIVISOR)
        total_facilities = sum(row[2] for row in result)

        sectors = [{
            'sectorId': row[3] if row[3] else None,
            'sectorName': row[0],
            'ghgEmission': round(float(row[1]) / MMT_DIVISOR) if row[1] else 0,
            'reportedEmission': round(float(row[1]) / MMT_DIVISOR) if row[1] else 0,
            'totalEmissions': round(float(row[1]) / MMT_DIVISOR) if row[1] else 0,
            'numberOfFacilitiesReported': row[2],
            'facilityCount': row[2],
            'numFacilities': row[2]
        } for row in result]

        return cors_response(200, {
            'result': {
                'sectorEmissionDetails': sectors,
                'totalReportedEmission': total_emissions,
                'totalNumberOfFacilities': total_facilities,
                'totalNumOfFacilitesReported': total_facilities,
                'reportingYear': reporting_year,
                'unitAbbr': 'MMT'
            },
            'messages': []
        })

    except Exception as e:
        print(f"Error in sectors_total_emissions: {e}")
        return cors_response(500, {'messages': [{'text': str(e), 'type': 500}]})

def handle_list_sectors(event):
    """POST /api/list/sectors - Return list of sectors with details"""
    try:
        body = json.loads(event.get('body', '{}') or '{}')
        reporting_year = body.get('reportingYear', 2023)

        # Download required Parquet files
        sector_path = get_local_parquet("RLPS_GHG_EMITTER_SECTOR")
        dim_sector_path = get_local_parquet("PUB_DIM_SECTOR")

        db = get_connection()

        # Get emissions by sector (excluding Biogenic CO2)
        query = f"""
            SELECT
                e.sector_name,
                SUM(CAST(e.co2e_emission AS DOUBLE)) as total_emissions,
                COUNT(DISTINCT e.facility_id) as facility_count,
                MAX(CAST(d.sector_id AS INTEGER)) as sector_id
            FROM read_parquet('{sector_path}') e
            LEFT JOIN read_parquet('{dim_sector_path}') d
                ON e.sector_name = d.sector_name
            WHERE e.year = '{reporting_year}'
                AND e.sector_type = 'E'
                AND e.gas_name != 'Biogenic CO2'
            GROUP BY e.sector_name
            ORDER BY total_emissions DESC
        """

        result = db.execute(query).fetchall()

        # Define columns for the data table
        cols = [
            {'id': 'icons', 'field': 'icons', 'name': '', 'sortable': False, 'cssClass': 'icon-col'},
            {'id': 'sector', 'field': 'sector', 'name': 'Sector', 'sortable': True, 'cssClass': '', 'type': 'string'},
            {'id': 'facilities', 'field': 'facilities', 'name': '# Facilities', 'sortable': True, 'cssClass': '', 'type': 'number'},
            {'id': 'total', 'field': 'totalReportedEmissions', 'name': 'Total Reported Emissions', 'sortable': True, 'cssClass': '', 'type': 'number'}
        ]

        # Convert to MMT and build rows
        MMT_DIVISOR = 1_000_000
        rows = [{
            'icons': [],
            'sector': row[0],
            'facilities': f"{row[2]:,}",
            'totalReportedEmissions': f"{round(row[1] / MMT_DIVISOR):,}" if row[1] else '0'
        } for row in result]

        return cors_response(200, {
            'result': {
                'data': {
                    'cols': cols,
                    'rows': rows
                },
                'year': reporting_year,
                'unit': 'MMT'
            },
            'messages': []
        })

    except Exception as e:
        print(f"Error in list_sectors: {e}")
        return cors_response(500, {'messages': [{'text': str(e), 'type': 500}]})

def handle_list_facilities(event):
    """POST /api/list/facilities - Return paginated list of facilities"""
    try:
        body = json.loads(event.get('body', '{}') or '{}')
        reporting_year = body.get('reportingYear', 2023)
        page = int(body.get('pageNumber', 1)) if body.get('pageNumber') else 1
        page_size = 100
        offset = (page - 1) * page_size

        # Download required Parquet files
        facilities_path = get_local_parquet("RLPS_GHG_EMITTER_FACILITIES")
        agg_path = get_local_parquet("RLPS_FAC_YEAR_AGG")
        sector_path = get_local_parquet("RLPS_GHG_EMITTER_SECTOR")

        db = get_connection()

        # Get facilities with emissions
        query = f"""
            SELECT
                f.facility_id,
                f.facility_name,
                f.city,
                f.state,
                f.county,
                CAST(f.latitude AS DOUBLE) as latitude,
                CAST(f.longitude AS DOUBLE) as longitude,
                COALESCE(CAST(a.total_emission AS DOUBLE), 0) as total_emission
            FROM read_parquet('{facilities_path}') f
            LEFT JOIN read_parquet('{agg_path}') a
                ON f.facility_id = a.pgm_sys_id AND f.year = a.year
            WHERE f.year = '{reporting_year}'
            ORDER BY total_emission DESC NULLS LAST
            LIMIT {page_size} OFFSET {offset}
        """

        result = db.execute(query).fetchall()

        # Define columns for the data table
        cols = [
            {'id': 'icons', 'field': 'icons', 'name': '', 'sortable': False, 'cssClass': 'icon-col'},
            {'id': 'facility', 'field': 'facility', 'name': 'Facility', 'sortable': True, 'cssClass': '', 'type': 'string'},
            {'id': 'city', 'field': 'city', 'name': 'City', 'sortable': True, 'cssClass': '', 'type': 'string'},
            {'id': 'state', 'field': 'state', 'name': 'State', 'sortable': True, 'cssClass': '', 'type': 'string'},
            {'id': 'total', 'field': 'totalReportedEmissions', 'name': 'Total Reported Emissions', 'sortable': True, 'cssClass': '', 'type': 'number'}
        ]

        # Convert to MMT and build rows
        MMT_DIVISOR = 1_000_000
        rows = [{
            'icons': [],
            'facility': f"{row[1]} [{row[0]}]",
            'city': row[2],
            'state': row[3],
            'totalReportedEmissions': f"{round(row[7] / MMT_DIVISOR):,}" if row[7] else '0'
        } for row in result]

        return cors_response(200, {
            'result': {
                'data': {
                    'cols': cols,
                    'rows': rows
                },
                'year': reporting_year,
                'unit': 'MMT'
            },
            'messages': []
        })

    except Exception as e:
        print(f"Error in list_facilities: {e}")
        return cors_response(500, {'messages': [{'text': str(e), 'type': 500}]})

def handle_sector_trend(event, sector_id, level):
    """POST /api/sector/trend/{sectorId}/{level} - Return trend data for line chart"""
    try:
        body = json.loads(event.get('body', '{}') or '{}')
        reporting_year = body.get('reportingYear', 2023)
        state = body.get('state', 'US')

        # Download required Parquet files
        sector_path = get_local_parquet("RLPS_GHG_EMITTER_SECTOR")
        dim_sector_path = get_local_parquet("PUB_DIM_SECTOR")
        facilities_path = get_local_parquet("RLPS_GHG_EMITTER_FACILITIES")

        db = get_connection()

        # Get sector name from sector_id if provided and not '0' (0 means all sectors)
        sector_name = None
        series_name = 'Total GHG Emissions'
        if sector_id and sector_id != '0':
            try:
                sector_id_int = int(sector_id)
                sector_query = f"""
                    SELECT sector_name
                    FROM read_parquet('{dim_sector_path}')
                    WHERE CAST(sector_id AS INTEGER) = {sector_id_int}
                    LIMIT 1
                """
                sector_result = db.execute(sector_query).fetchone()
                if sector_result:
                    sector_name = sector_result[0]
                    series_name = f'{sector_name} Emissions'
            except (ValueError, TypeError):
                pass  # Keep default series name if sector_id is not a valid integer

        # Build query with optional state and sector filters
        state_join = ""
        state_filter = ""
        if state and state != 'US':
            state_join = f"INNER JOIN read_parquet('{facilities_path}') f ON e.facility_id = f.facility_id AND e.year = f.year"
            state_filter = f"AND f.state = '{state}'"

        sector_filter = ""
        if sector_name:
            sector_filter = f"AND e.sector_name = '{sector_name}'"

        # Get emissions by year for trend line (excluding Biogenic CO2)
        query = f"""
            SELECT
                e.year,
                SUM(CAST(e.co2e_emission AS DOUBLE)) as total_emissions
            FROM read_parquet('{sector_path}') e
            {state_join}
            WHERE e.sector_type = 'E'
                AND e.gas_name != 'Biogenic CO2'
                {state_filter}
                {sector_filter}
            GROUP BY e.year
            ORDER BY e.year ASC
        """

        result = db.execute(query).fetchall()

        # Convert to MMT
        MMT_DIVISOR = 1_000_000

        # Build year categories and data for line chart
        years = [str(row[0]) for row in result]
        values = [round(float(row[1]) / MMT_DIVISOR) if row[1] else 0 for row in result]

        # Build response in ApexCharts-compatible format expected by frontend
        return cors_response(200, {
            'result': {
                'xAxis': {
                    'categories': years
                },
                'series': [{
                    'name': series_name,
                    'data': values,
                    'color': '#1f77b4'
                }],
                'yearRange': years,
                'credits': f'Data from EPA GHGRP {years[0]}-{years[-1]}' if years else ''
            },
            'messages': []
        })

    except Exception as e:
        print(f"Error in sector_trend: {e}")
        return cors_response(500, {'messages': [{'text': str(e), 'type': 500}]})

def handle_bar_sector(event, level=None):
    """POST /api/bar/sector - Return bar chart data by sector"""
    try:
        body = json.loads(event.get('body', '{}') or '{}')
        reporting_year = body.get('reportingYear', 2023)
        state = body.get('state', 'US')

        # Download required Parquet files
        sector_path = get_local_parquet("RLPS_GHG_EMITTER_SECTOR")
        dim_sector_path = get_local_parquet("PUB_DIM_SECTOR")
        facilities_path = get_local_parquet("RLPS_GHG_EMITTER_FACILITIES")

        db = get_connection()

        # Build query with optional state filter
        state_join = ""
        state_filter = ""
        if state and state != 'US':
            state_join = f"INNER JOIN read_parquet('{facilities_path}') f ON e.facility_id = f.facility_id AND e.year = f.year"
            state_filter = f"AND f.state = '{state}'"

        # Get emissions by sector (excluding Biogenic CO2)
        query = f"""
            SELECT
                e.sector_name,
                SUM(CAST(e.co2e_emission AS DOUBLE)) as total_emissions,
                COUNT(DISTINCT e.facility_id) as facility_count,
                MAX(CAST(d.sector_id AS INTEGER)) as sector_id
            FROM read_parquet('{sector_path}') e
            LEFT JOIN read_parquet('{dim_sector_path}') d
                ON e.sector_name = d.sector_name
            {state_join}
            WHERE e.year = '{reporting_year}'
                AND e.sector_type = 'E'
                AND e.gas_name != 'Biogenic CO2'
                {state_filter}
            GROUP BY e.sector_name
            ORDER BY total_emissions DESC
        """

        result = db.execute(query).fetchall()

        # Convert to MMT
        MMT_DIVISOR = 1_000_000

        # Sector colors
        sector_colors = {
            'Power Plants': '#1f77b4',
            'Petroleum and Natural Gas Systems': '#ff7f0e',
            'Chemicals': '#2ca02c',
            'Refineries': '#d62728',
            'Other': '#9467bd',
            'Minerals': '#8c564b',
            'Waste': '#e377c2',
            'Metals': '#7f7f7f',
            'Pulp and Paper': '#bcbd22'
        }

        # Build categories and series for bar chart
        categories = [row[0] for row in result]

        # Build series with data for each sector
        series = [{
            'name': 'GHG Emissions',
            'id': 'ghg_emissions',
            'color': '#1f77b4',
            'data': [round(float(row[1]) / MMT_DIVISOR) if row[1] else 0 for row in result]
        }]

        return cors_response(200, {
            'result': {
                'xAxis': {
                    'categories': categories
                },
                'series': series,
                'domain': 'GHG',
                'unit': 'MMT',
                'view': 'SECTOR1',
                'credits': f'Data from EPA GHGRP {reporting_year}'
            },
            'messages': []
        })

    except Exception as e:
        print(f"Error in bar_sector: {e}")
        return cors_response(500, {'messages': [{'text': str(e), 'type': 500}]})

def handle_pie_sectors_emissions(event, level=None, subsector=None):
    """POST /api/pie/sectors/emissions - Return pie chart data for sectors"""
    try:
        body = json.loads(event.get('body', '{}') or '{}')
        reporting_year = body.get('reportingYear', 2023)
        state = body.get('state', 'US')
        sector1 = body.get('sector1', '')

        # Download required Parquet files
        sector_path = get_local_parquet("RLPS_GHG_EMITTER_SECTOR")
        dim_sector_path = get_local_parquet("PUB_DIM_SECTOR")
        facilities_path = get_local_parquet("RLPS_GHG_EMITTER_FACILITIES")

        db = get_connection()

        # Build query with optional state filter
        state_join = ""
        state_filter = ""
        if state and state != 'US':
            state_join = f"INNER JOIN read_parquet('{facilities_path}') f ON e.facility_id = f.facility_id AND e.year = f.year"
            state_filter = f"AND f.state = '{state}'"

        # For level 2/3, filter by sector if provided (subsector data may not be available)
        sector_filter = ""
        if level in ['2', '3'] and sector1:
            sector_filter = f"AND e.sector_name = '{sector1}'"

        # Get emissions by sector (excluding Biogenic CO2)
        query = f"""
            SELECT
                e.sector_name,
                SUM(CAST(e.co2e_emission AS DOUBLE)) as total_emissions,
                MAX(CAST(d.sector_id AS INTEGER)) as sector_id
            FROM read_parquet('{sector_path}') e
            LEFT JOIN read_parquet('{dim_sector_path}') d
                ON e.sector_name = d.sector_name
            {state_join}
            WHERE e.year = '{reporting_year}'
                AND e.sector_type = 'E'
                AND e.gas_name != 'Biogenic CO2'
                {state_filter}
                {sector_filter}
            GROUP BY e.sector_name
            ORDER BY total_emissions DESC
        """

        result = db.execute(query).fetchall()

        # Convert to MMT
        MMT_DIVISOR = 1_000_000

        # Sector colors (matching EPA site)
        sector_colors = {
            'Power Plants': '#1f77b4',
            'Petroleum and Natural Gas Systems': '#ff7f0e',
            'Chemicals': '#2ca02c',
            'Refineries': '#d62728',
            'Other': '#9467bd',
            'Minerals': '#8c564b',
            'Waste': '#e377c2',
            'Metals': '#7f7f7f',
            'Pulp and Paper': '#bcbd22'
        }

        # Build data array for pie chart - each item has name, y (value), color, id
        pie_data = [{
            'name': row[0],
            'y': round(float(row[1]) / MMT_DIVISOR) if row[1] else 0,
            'color': sector_colors.get(row[0], '#999999'),
            'id': row[2] if row[2] else None
        } for row in result]

        return cors_response(200, {
            'result': {
                'series': [{
                    'name': 'Emissions',
                    'data': pie_data
                }],
                'domain': 'GHG',
                'unit': 'MMT',
                'view': 'SECTOR1',
                'credits': f'Data from EPA GHGRP {reporting_year}'
            },
            'messages': []
        })

    except Exception as e:
        print(f"Error in pie_sectors_emissions: {e}")
        return cors_response(500, {'messages': [{'text': str(e), 'type': 500}]})

def handle_export(event):
    """POST /api/export - Export facility data as CSV/Excel"""
    try:
        body = json.loads(event.get('body', '{}') or '{}')
        query_params = event.get('queryStringParameters', {}) or {}

        reporting_year = body.get('reportingYear', 2023)
        state = body.get('state', 'US')
        data_source = body.get('dataSource', 'E')
        all_years = query_params.get('allReportingYears', 'false').lower() == 'true'
        list_export = query_params.get('listExport', 'false').lower() == 'true'

        # Download required Parquet files
        facilities_path = get_local_parquet("RLPS_GHG_EMITTER_FACILITIES")
        sector_path = get_local_parquet("RLPS_GHG_EMITTER_SECTOR")

        db = get_connection()

        if all_years:
            # Export all years - include year column and group by year
            # Limit to top emitters per year to avoid Lambda response size limits
            query = f"""
                SELECT
                    f.year,
                    f.facility_id,
                    f.facility_name,
                    f.city,
                    f.state,
                    f.county,
                    f.zip,
                    f.address1,
                    f.latitude,
                    f.longitude,
                    f.parent_company,
                    COALESCE(SUM(CAST(s.co2e_emission AS DOUBLE)), 0) as total_emissions
                FROM read_parquet('{facilities_path}') f
                LEFT JOIN read_parquet('{sector_path}') s
                    ON f.facility_id = s.facility_id AND f.year = s.year
                    AND s.sector_type = '{data_source}'
                    AND s.gas_name != 'Biogenic CO2'
                WHERE 1=1
            """

            if state and state != 'US':
                query += f" AND f.state = '{state}'"

            query += """
                GROUP BY f.year, f.facility_id, f.facility_name, f.city, f.state,
                         f.county, f.zip, f.address1, f.latitude, f.longitude, f.parent_company
                HAVING total_emissions > 0
                ORDER BY f.year DESC, total_emissions DESC
                LIMIT 25000
            """
        else:
            # Single year export
            query = f"""
                SELECT
                    f.facility_id,
                    f.facility_name,
                    f.city,
                    f.state,
                    f.county,
                    f.zip,
                    f.address1,
                    f.latitude,
                    f.longitude,
                    f.parent_company,
                    COALESCE(SUM(CAST(s.co2e_emission AS DOUBLE)), 0) as total_emissions
                FROM read_parquet('{facilities_path}') f
                LEFT JOIN read_parquet('{sector_path}') s
                    ON f.facility_id = s.facility_id AND f.year = s.year
                    AND s.sector_type = '{data_source}'
                    AND s.gas_name != 'Biogenic CO2'
                WHERE f.year = '{reporting_year}'
            """

            if state and state != 'US':
                query += f" AND f.state = '{state}'"

            query += """
                GROUP BY f.facility_id, f.facility_name, f.city, f.state,
                         f.county, f.zip, f.address1, f.latitude, f.longitude, f.parent_company
                ORDER BY total_emissions DESC
            """

        result = db.execute(query).fetchall()

        # Create CSV in memory
        output = io.StringIO()
        writer = csv.writer(output)

        # Write header
        if all_years:
            writer.writerow([
                'Year', 'Facility ID', 'Facility Name', 'City', 'State', 'County',
                'ZIP', 'Address', 'Latitude', 'Longitude', 'Parent Company',
                'Total Emissions (Metric Tons CO2e)'
            ])
        else:
            writer.writerow([
                'Facility ID', 'Facility Name', 'City', 'State', 'County',
                'ZIP', 'Address', 'Latitude', 'Longitude', 'Parent Company',
                'Total Emissions (Metric Tons CO2e)'
            ])

        # Write data rows
        for row in result:
            if all_years:
                writer.writerow([
                    row[0],  # year
                    row[1],  # facility_id
                    row[2],  # facility_name
                    row[3],  # city
                    row[4],  # state
                    row[5],  # county
                    row[6],  # zip
                    row[7],  # address1
                    row[8],  # latitude
                    row[9],  # longitude
                    row[10],  # parent_company
                    round(float(row[11])) if row[11] else 0  # total_emissions
                ])
            else:
                writer.writerow([
                    row[0],  # facility_id
                    row[1],  # facility_name
                    row[2],  # city
                    row[3],  # state
                    row[4],  # county
                    row[5],  # zip
                    row[6],  # address1
                    row[7],  # latitude
                    row[8],  # longitude
                    row[9],  # parent_company
                    round(float(row[10])) if row[10] else 0  # total_emissions
                ])

        csv_content = output.getvalue()
        output.close()

        # Return CSV file for download (CORS handled by Function URL config)
        filename = "ghg_emissions_all_years.csv" if all_years else f"ghg_emissions_{reporting_year}.csv"
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'text/csv',
                'Content-Disposition': f'attachment; filename="{filename}"'
            },
            'body': csv_content,
            'isBase64Encoded': False
        }

    except Exception as e:
        print(f"Error in export: {e}")
        return cors_response(500, {'messages': [{'text': str(e), 'type': 500}]})

def handle_state_bounds(event, state_code):
    """GET /api/state/bounds/{state} - Return state boundary and geometry info"""
    try:
        state_code = state_code.upper()

        # Handle special cases for US-wide bounds - return empty array
        if state_code in ['US', 'LOCAL']:
            return cors_response(200, {
                'result': [],
                'messages': []
            })

        # Load state geometries
        state_data = get_geo_json('state_geometries.json')

        if state_code not in state_data:
            return cors_response(404, {
                'messages': [{'text': f'State not found: {state_code}', 'type': 404}]
            })

        state_info = state_data[state_code]

        # Return state bounds and geometry in expected format
        return cors_response(200, {
            'result': {
                'stateName': state_info['name'],
                'stateCode': state_code,
                'bounds': state_info['bounds'],
                'geometry': state_info['geometry']
            },
            'messages': []
        })
    except Exception as e:
        return cors_response(500, {
            'messages': [{'text': f'Error loading state bounds: {str(e)}', 'type': 500}]
        })

def handle_state_counties(event, state_code):
    """GET /api/state/counties/{state} - Return counties for a state"""
    try:
        state_code = state_code.upper()

        # Load counties data
        counties_data = get_geo_json('counties_by_state.json')

        if state_code not in counties_data:
            return cors_response(200, {'result': [], 'messages': []})

        counties = counties_data[state_code]

        # Return counties in expected format
        return cors_response(200, {
            'result': counties,
            'messages': []
        })
    except Exception as e:
        return cors_response(500, {
            'messages': [{'text': f'Error loading counties: {str(e)}', 'type': 500}]
        })

def handle_basin_geo(event):
    """GET /api/basin/geo - Return basin geometry (petroleum basins)"""
    # Basin data would require additional GeoJSON - return empty for now
    # Could be populated later with petroleum basin boundaries if needed
    return cors_response(200, {'result': [], 'messages': []})

def handle_data_files(event):
    """GET /api/data/files - List all CSV files available for download"""
    try:
        # List all CSV files in the bucket
        csv_prefix = 'epa_ghg_tables_csvs/'
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=csv_prefix)

        files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                filename = key.replace(csv_prefix, '')
                if filename and filename.endswith('.csv'):
                    # Format file size
                    size_bytes = obj['Size']
                    if size_bytes < 1024:
                        size_str = f"{size_bytes} B"
                    elif size_bytes < 1024 * 1024:
                        size_str = f"{size_bytes / 1024:.1f} KB"
                    else:
                        size_str = f"{size_bytes / (1024 * 1024):.1f} MB"

                    files.append({
                        'filename': filename,
                        'size': size_str,
                        'sizeBytes': size_bytes,
                        'lastModified': obj['LastModified'].isoformat()
                    })

        # Handle pagination if there are more files
        while response.get('IsTruncated'):
            response = s3_client.list_objects_v2(
                Bucket=S3_BUCKET,
                Prefix=csv_prefix,
                ContinuationToken=response['NextContinuationToken']
            )
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    filename = key.replace(csv_prefix, '')
                    if filename and filename.endswith('.csv'):
                        size_bytes = obj['Size']
                        if size_bytes < 1024:
                            size_str = f"{size_bytes} B"
                        elif size_bytes < 1024 * 1024:
                            size_str = f"{size_bytes / 1024:.1f} KB"
                        else:
                            size_str = f"{size_bytes / (1024 * 1024):.1f} MB"

                        files.append({
                            'filename': filename,
                            'size': size_str,
                            'sizeBytes': size_bytes,
                            'lastModified': obj['LastModified'].isoformat()
                        })

        # Sort by filename
        files.sort(key=lambda x: x['filename'])

        return cors_response(200, {
            'result': {
                'files': files,
                'count': len(files)
            },
            'messages': []
        })
    except Exception as e:
        print(f"Error in data_files: {e}")
        return cors_response(500, {'messages': [{'text': str(e), 'type': 500}]})

def handle_data_download(event, filename):
    """GET /api/data/download/{filename} - Generate presigned URL for file download"""
    try:
        # Security: only allow .csv files from the expected prefix
        if not filename.endswith('.csv') or '/' in filename or '..' in filename:
            return cors_response(400, {'messages': [{'text': 'Invalid filename', 'type': 400}]})

        s3_key = f'epa_ghg_tables_csvs/{filename}'

        # Check if file exists
        try:
            s3_client.head_object(Bucket=S3_BUCKET, Key=s3_key)
        except:
            return cors_response(404, {'messages': [{'text': 'File not found', 'type': 404}]})

        # Generate presigned URL (valid for 1 hour)
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': S3_BUCKET, 'Key': s3_key},
            ExpiresIn=3600
        )

        return cors_response(200, {
            'result': {
                'url': presigned_url,
                'filename': filename
            },
            'messages': []
        })
    except Exception as e:
        print(f"Error in data_download: {e}")
        return cors_response(500, {'messages': [{'text': str(e), 'type': 500}]})

def handle_facility_hover(event, year):
    """GET /api/facility/hover/{year} - Return facility details for hover popup"""
    try:
        # Get query parameters
        query_params = event.get('queryStringParameters', {}) or {}
        facility_id = query_params.get('id')

        if not facility_id:
            return cors_response(400, {'messages': [{'text': 'Missing facility id', 'type': 400}]})

        # Download required Parquet files
        facilities_path = get_local_parquet("RLPS_GHG_EMITTER_FACILITIES")
        sector_path = get_local_parquet("RLPS_GHG_EMITTER_SECTOR")

        db = get_connection()

        # Get facility info
        facility_query = f"""
            SELECT
                facility_id,
                facility_name,
                address1,
                address2,
                city,
                state,
                county,
                zip,
                latitude,
                longitude,
                parent_company
            FROM read_parquet('{facilities_path}')
            WHERE facility_id = '{facility_id}'
                AND year = '{year}'
            LIMIT 1
        """

        facility_result = db.execute(facility_query).fetchone()

        if not facility_result:
            # Try without year filter as fallback
            facility_query_fallback = f"""
                SELECT
                    facility_id,
                    facility_name,
                    address1,
                    address2,
                    city,
                    state,
                    county,
                    zip,
                    latitude,
                    longitude,
                    parent_company
                FROM read_parquet('{facilities_path}')
                WHERE facility_id = '{facility_id}'
                ORDER BY year DESC
                LIMIT 1
            """
            facility_result = db.execute(facility_query_fallback).fetchone()

        if not facility_result:
            return cors_response(404, {'messages': [{'text': 'Facility not found', 'type': 404}]})

        # Get emissions by gas type
        emissions_query = f"""
            SELECT
                gas_name,
                SUM(CAST(co2e_emission AS DOUBLE)) as total_emission
            FROM read_parquet('{sector_path}')
            WHERE facility_id = '{facility_id}'
                AND year = '{year}'
            GROUP BY gas_name
            ORDER BY total_emission DESC
        """

        emissions_result = db.execute(emissions_query).fetchall()

        facility_dto = {
            'facilityId': int(facility_result[0]) if facility_result[0] else None,
            'facilityName': facility_result[1],
            'address1': facility_result[2],
            'address2': facility_result[3],
            'city': facility_result[4],
            'state': facility_result[5],
            'county': facility_result[6],
            'zip': facility_result[7],
            'latitude': float(facility_result[8]) if facility_result[8] else None,
            'longitude': float(facility_result[9]) if facility_result[9] else None,
            'parentCompany': facility_result[10]
        }

        emissions = [{
            'type': row[0],
            'quantity': round(float(row[1])) if row[1] else 0
        } for row in emissions_result]

        return cors_response(200, {
            'result': {
                'facilityTipDto': facility_dto,
                'emissions': emissions
            },
            'messages': []
        })

    except Exception as e:
        print(f"Error in facility_hover: {e}")
        return cors_response(500, {'messages': [{'text': str(e), 'type': 500}]})

# Route mapping
ROUTES = {
    ('GET', '/api/version'): handle_version,
    ('GET', '/ghgp/api/version'): handle_version,
    ('POST', '/api/facilities/map-markers'): handle_facilities_map_markers,
    ('POST', '/ghgp/api/facilities/map-markers'): handle_facilities_map_markers,
    ('POST', '/api/sectors/total/emissions'): handle_sectors_total_emissions,
    ('POST', '/ghgp/api/sectors/total/emissions'): handle_sectors_total_emissions,
    ('POST', '/api/list/sectors'): handle_list_sectors,
    ('POST', '/ghgp/api/list/sectors'): handle_list_sectors,
    ('POST', '/api/list/facilities'): handle_list_facilities,
    ('POST', '/ghgp/api/list/facilities'): handle_list_facilities,
    ('POST', '/api/bar/sector'): handle_bar_sector,
    ('POST', '/ghgp/api/bar/sector'): handle_bar_sector,
    ('POST', '/api/pie/sectors/emissions'): handle_pie_sectors_emissions,
    ('POST', '/ghgp/api/pie/sectors/emissions'): handle_pie_sectors_emissions,
    ('POST', '/api/map/overlay'): handle_map_overlay,
    ('POST', '/ghgp/api/map/overlay'): handle_map_overlay,
}

def lambda_handler(event, context):
    """Main Lambda handler - supports both Function URL and API Gateway events"""

    request_context = event.get('requestContext', {})

    # Function URL format
    if 'http' in request_context:
        method = request_context['http'].get('method', 'GET')
        path = request_context['http'].get('path', '/')
    # API Gateway format
    else:
        method = event.get('httpMethod', 'GET')
        path = event.get('path', '/')

    # Handle CORS preflight
    if method == 'OPTIONS':
        return cors_response(204, '')

    # Find matching route
    handler = ROUTES.get((method, path))

    if handler:
        return handler(event)

    # Handle state bounds with path parameter
    if '/api/state/bounds/' in path or '/ghgp/api/state/bounds/' in path:
        state_code = path.split('/')[-1]
        return handle_state_bounds(event, state_code)

    # Handle state counties with path parameter
    if '/api/state/counties/' in path or '/ghgp/api/state/counties/' in path:
        state_code = path.split('/')[-1]
        return handle_state_counties(event, state_code)

    # Handle basin geo
    if path in ['/api/basin/geo', '/ghgp/api/basin/geo']:
        return handle_basin_geo(event)

    # Handle facility hover with path parameter
    if '/api/facility/hover/' in path or '/ghgp/api/facility/hover/' in path:
        year = path.split('/')[-1]
        return handle_facility_hover(event, year)

    # Handle sector trend with path parameters: /api/sector/trend/{sectorId}/{level}
    if '/api/sector/trend/' in path or '/ghgp/api/sector/trend/' in path:
        parts = path.rstrip('/').split('/')
        # Get last two parts as sector_id and level
        level = parts[-1] if len(parts) > 1 else '1'
        sector_id = parts[-2] if len(parts) > 2 else '0'
        return handle_sector_trend(event, sector_id, level)

    # Handle bar/sector/level2 path
    if path in ['/api/bar/sector/level2', '/ghgp/api/bar/sector/level2']:
        return handle_bar_sector(event, level='2')

    # Handle pie chart level2 and level3 paths
    if '/api/pie/level2/sector/emissions' in path or '/ghgp/api/pie/level2/sector/emissions' in path:
        return handle_pie_sectors_emissions(event, level='2')

    if '/api/pie/level3/subsector/emissions' in path or '/ghgp/api/pie/level3/subsector/emissions' in path:
        return handle_pie_sectors_emissions(event, level='3', subsector=True)

    # Handle export endpoint (path may have query params stripped)
    if path.startswith('/api/export') or path.startswith('/ghgp/api/export'):
        return handle_export(event)

    # Handle data files listing
    if path in ['/api/data/files', '/ghgp/api/data/files']:
        return handle_data_files(event)

    # Handle data file download
    if '/api/data/download/' in path or '/ghgp/api/data/download/' in path:
        filename = path.split('/')[-1]
        return handle_data_download(event, filename)

    # 404 for unknown routes
    return cors_response(404, {
        'messages': [{'text': f'Route not found: {method} {path}', 'type': 404}]
    })
