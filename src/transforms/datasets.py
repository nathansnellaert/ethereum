
"""Transform Ethereum data."""

import pyarrow as pa
from subsets_utils import load_raw_json, upload_data, save_state


def run():
    """Transform all Ethereum datasets."""
    try:
        raw_data = load_raw_json("ethereum_data")
    except FileNotFoundError:
        print("No raw data found. Run ingest first.")
        return

    # Blocks
    print("  Transforming blocks...")
    blocks_info = raw_data.get("blocks", {})
    blocks_data = blocks_info.get("data", [])
    if blocks_data:
        table = pa.Table.from_pylist(blocks_data)
        print(f"    {len(table):,} rows")
        upload_data(table, "ethereum_blocks")
        save_state("blocks", {"last_block": blocks_info.get("max_block", 0)})
    else:
        print("    No new blocks to transform")

    # Transactions
    print("  Transforming transactions...")
    transactions_info = raw_data.get("transactions", {})
    transactions_data = transactions_info.get("data", [])
    if transactions_data:
        table = pa.Table.from_pylist(transactions_data)
        print(f"    {len(table):,} rows")
        upload_data(table, "ethereum_transactions")
        save_state("transactions", {"last_block": transactions_info.get("max_block", 0)})
    else:
        print("    No new transactions to transform")

    # ERC-20 Transfers
    print("  Transforming ERC-20 transfers...")
    transfers_info = raw_data.get("erc20_transfers", {})
    transfers_data = transfers_info.get("data", [])
    if transfers_data:
        table = pa.Table.from_pylist(transfers_data)
        print(f"    {len(table):,} rows")
        upload_data(table, "ethereum_erc20_transfers")
        save_state("erc20_transfers", {"last_block": transfers_info.get("max_block", 0)})
    else:
        print("    No new ERC-20 transfers to transform")
