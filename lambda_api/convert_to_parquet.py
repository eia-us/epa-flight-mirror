#!/usr/bin/env python3
"""
Convert EPA GHG CSV files to Parquet format for better query performance.
Parquet files are typically 5-10x faster to query with DuckDB.
"""

import duckdb
import boto3
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

S3_BUCKET = 'epa-backups-eia'
CSV_PREFIX = 'epa_ghg_tables_csvs/'
PARQUET_PREFIX = 'epa_ghg_tables_parquet/'

# Tables most important for the FLIGHT API (convert these first)
PRIORITY_TABLES = [
    'RLPS_GHG_EMITTER_FACILITIES',
    'RLPS_GHG_EMITTER_SECTOR',
    'RLPS_FAC_YEAR_AGG',
    'PUB_DIM_SECTOR',
    'PUB_DIM_FACILITY',
    'PUB_FACILITY_LAT_LONG',
    'RLPS_GHG_EMITTER_GAS',
    'RLPS_GHG_EMITTER_SUBPART',
]

def list_csv_files(s3_client):
    """List all CSV files in the source prefix"""
    paginator = s3_client.get_paginator('list_objects_v2')
    csv_files = []

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=CSV_PREFIX):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.csv'):
                # Extract table name from key like "epa_ghg_tables_csvs/ghg.TABLE_NAME.csv"
                filename = key.split('/')[-1]
                table_name = filename.replace('ghg.', '').replace('.csv', '')
                csv_files.append({
                    'key': key,
                    'table_name': table_name,
                    'size': obj['Size']
                })

    return csv_files

def convert_single_file(table_info, s3_client):
    """Convert a single CSV file to Parquet"""
    table_name = table_info['table_name']
    csv_key = table_info['key']
    size_mb = table_info['size'] / (1024 * 1024)

    parquet_key = f"{PARQUET_PREFIX}ghg.{table_name}.parquet"

    print(f"Converting {table_name} ({size_mb:.1f} MB)...")

    try:
        # Create a fresh DuckDB connection for this conversion
        conn = duckdb.connect(':memory:')
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")
        conn.execute("SET s3_region='us-east-1'")

        # Use a temp file for the parquet output
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
            tmp_path = tmp.name

        # Convert CSV to Parquet
        csv_s3_path = f"s3://{S3_BUCKET}/{csv_key}"

        conn.execute(f"""
            COPY (SELECT * FROM read_csv_auto('{csv_s3_path}'))
            TO '{tmp_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

        conn.close()

        # Upload parquet file to S3
        s3_client.upload_file(tmp_path, S3_BUCKET, parquet_key)

        # Get the new file size
        response = s3_client.head_object(Bucket=S3_BUCKET, Key=parquet_key)
        new_size_mb = response['ContentLength'] / (1024 * 1024)

        # Clean up temp file
        os.unlink(tmp_path)

        compression_ratio = size_mb / new_size_mb if new_size_mb > 0 else 0
        print(f"  ✓ {table_name}: {size_mb:.1f} MB → {new_size_mb:.1f} MB ({compression_ratio:.1f}x compression)")

        return {
            'table': table_name,
            'success': True,
            'original_size': size_mb,
            'parquet_size': new_size_mb
        }

    except Exception as e:
        print(f"  ✗ {table_name}: {str(e)}")
        return {
            'table': table_name,
            'success': False,
            'error': str(e)
        }

def main():
    print("EPA GHG CSV to Parquet Converter")
    print("=" * 50)
    print(f"Source: s3://{S3_BUCKET}/{CSV_PREFIX}")
    print(f"Destination: s3://{S3_BUCKET}/{PARQUET_PREFIX}")
    print()

    s3_client = boto3.client('s3')

    # List all CSV files
    print("Scanning CSV files...")
    csv_files = list_csv_files(s3_client)
    print(f"Found {len(csv_files)} CSV files")

    # Sort by priority (priority tables first, then by size descending)
    def sort_key(f):
        if f['table_name'] in PRIORITY_TABLES:
            return (0, PRIORITY_TABLES.index(f['table_name']))
        return (1, -f['size'])

    csv_files.sort(key=sort_key)

    total_csv_size = sum(f['size'] for f in csv_files) / (1024 * 1024)
    print(f"Total CSV size: {total_csv_size:.1f} MB")
    print()

    # Convert files
    print("Converting to Parquet (ZSTD compression)...")
    print("-" * 50)

    results = []
    for table_info in csv_files:
        result = convert_single_file(table_info, s3_client)
        results.append(result)

    # Summary
    print()
    print("=" * 50)
    print("SUMMARY")
    print("=" * 50)

    successful = [r for r in results if r['success']]
    failed = [r for r in results if not r['success']]

    total_original = sum(r['original_size'] for r in successful)
    total_parquet = sum(r['parquet_size'] for r in successful)

    print(f"Converted: {len(successful)}/{len(results)} files")
    print(f"Original size: {total_original:.1f} MB")
    print(f"Parquet size: {total_parquet:.1f} MB")
    print(f"Space saved: {total_original - total_parquet:.1f} MB ({((total_original - total_parquet) / total_original * 100):.1f}%)")

    if failed:
        print()
        print("Failed conversions:")
        for r in failed:
            print(f"  - {r['table']}: {r['error']}")

    print()
    print("Done! Update your Lambda to use the new prefix:")
    print(f"  S3_PREFIX = '{PARQUET_PREFIX.rstrip('/')}'")

if __name__ == '__main__':
    main()
