
"""Ingest Ethereum data from BigQuery."""

import json
import os

from google.cloud import bigquery
from google.oauth2 import service_account
from subsets_utils import load_state, save_state, save_raw_json

DEFAULT_START_DATE = "2023-01-01"


def get_bigquery_client():
    """Create BigQuery client with credentials from environment."""
    creds_json = os.environ["GCP_SERVICE_ACCOUNT_KEY"]
    creds_dict = json.loads(creds_json)
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    return bigquery.Client(credentials=credentials, project=creds_dict["project_id"])

MAJOR_TOKENS = {
    "0xdac17f958d2ee523a2206206994597c13d831ec7": "USDT",
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": "USDC",
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": "WETH",
    "0x6b175474e89094c44da98b954eedeac495271d0f": "DAI",
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": "WBTC",
    "0x514910771af9ca656af840dff83e8264ecf986ca": "LINK",
    "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984": "UNI",
    "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9": "AAVE",
}


def run():
    """Fetch Ethereum data from BigQuery and save raw."""
    client = get_bigquery_client()
    all_data = {}

    # Blocks
    print("  Fetching blocks...")
    state = load_state("blocks")
    last_block = state.get("last_block", 0)

    blocks_query = f"""
    SELECT
        number as block_number,
        `hash` as block_hash,
        parent_hash,
        nonce,
        miner,
        difficulty,
        total_difficulty,
        size,
        gas_limit,
        gas_used,
        timestamp,
        transaction_count,
        base_fee_per_gas
    FROM `bigquery-public-data.crypto_ethereum.blocks`
    WHERE number > {last_block}
    ORDER BY number
    """

    blocks_df = client.query(blocks_query).to_dataframe()
    if len(blocks_df) > 0:
        all_data["blocks"] = {
            "data": blocks_df.to_dict(orient="records"),
            "max_block": int(blocks_df["block_number"].max())
        }
        print(f"    Fetched {len(blocks_df):,} blocks")
    else:
        print("    No new blocks")
        all_data["blocks"] = {"data": [], "max_block": last_block}

    # Transactions
    print("  Fetching transactions...")
    state = load_state("transactions")
    last_block = state.get("last_block", 0)

    transactions_query = f"""
    SELECT
        `hash` as transaction_hash,
        block_number,
        block_timestamp,
        from_address,
        to_address,
        value,
        gas,
        gas_price,
        max_fee_per_gas,
        max_priority_fee_per_gas,
        input,
        nonce,
        transaction_index,
        transaction_type,
        receipt_gas_used,
        receipt_status,
        receipt_effective_gas_price
    FROM `bigquery-public-data.crypto_ethereum.transactions`
    WHERE block_timestamp >= '{DEFAULT_START_DATE}'
      AND block_number > {last_block}
    ORDER BY block_number, transaction_index
    LIMIT 1000000
    """

    transactions_df = client.query(transactions_query).to_dataframe()
    if len(transactions_df) > 0:
        all_data["transactions"] = {
            "data": transactions_df.to_dict(orient="records"),
            "max_block": int(transactions_df["block_number"].max())
        }
        print(f"    Fetched {len(transactions_df):,} transactions")
    else:
        print("    No new transactions")
        all_data["transactions"] = {"data": [], "max_block": last_block}

    # ERC-20 Transfers
    print("  Fetching ERC-20 transfers...")
    state = load_state("erc20_transfers")
    last_block = state.get("last_block", 0)

    addresses = ", ".join(f"'{addr}'" for addr in MAJOR_TOKENS.keys())
    transfers_query = f"""
    SELECT
        token_address,
        from_address,
        to_address,
        value,
        transaction_hash,
        log_index,
        block_number,
        block_timestamp
    FROM `bigquery-public-data.crypto_ethereum.token_transfers`
    WHERE block_timestamp >= '{DEFAULT_START_DATE}'
      AND block_number > {last_block}
      AND token_address IN ({addresses})
    ORDER BY block_number, log_index
    LIMIT 1000000
    """

    transfers_df = client.query(transfers_query).to_dataframe()
    if len(transfers_df) > 0:
        all_data["erc20_transfers"] = {
            "data": transfers_df.to_dict(orient="records"),
            "max_block": int(transfers_df["block_number"].max())
        }
        print(f"    Fetched {len(transfers_df):,} ERC-20 transfers")
    else:
        print("    No new ERC-20 transfers")
        all_data["erc20_transfers"] = {"data": [], "max_block": last_block}

    save_raw_json(all_data, "ethereum_data", compress=True)
