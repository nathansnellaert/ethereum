import argparse
import os


from subsets_utils import validate_environment
from ingest import bigquery_data as ingest_data
from transforms import datasets as transform_data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest-only", action="store_true", help="Only fetch data from BigQuery")
    parser.add_argument("--transform-only", action="store_true", help="Only transform existing raw data")
    args = parser.parse_args()

    validate_environment()

    should_ingest = not args.transform_only
    should_transform = not args.ingest_only

    if should_ingest:
        print("\n=== Phase 1: Ingest ===")
        ingest_data.run()

    if should_transform:
        print("\n=== Phase 2: Transform ===")
        transform_data.run()


if __name__ == "__main__":
    main()
