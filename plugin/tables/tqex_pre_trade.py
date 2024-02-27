from datetime import datetime
from typing import Any, Generator, Dict

import pyarrow as pa
from cloudquery.sdk.schema import Column, Table
from cloudquery.sdk.scheduler import TableResolver
from cloudquery.sdk.schema.resource import Resource

from plugin.client import Client

class TQEXPreTrade(Table):
    def __init__(self) -> None:
        super().__init__(
            name="tqex_pre_trade",
            title="TQEX Pre-Trade Data",
            is_incremental=True,
            columns=[
                Column("distribution_time", pa.timestamp("us")),
                Column("instrument_id", pa.uint64(), primary_key=True, unique=True),
                Column("source_venue", pa.uint8()),
                Column("bid_market_size", pa.float64()),
                Column("bid_limit_price", pa.float64()),
                Column("bid_yield", pa.float64()),
                Column("bid_limit_size", pa.float64()),
                Column("offer_market_size", pa.float64()),
                Column("offer_limit_price", pa.float64()),
                Column("offer_yield", pa.float64()),
                Column("offer_limit_size", pa.float64()),
                Column("order_book_type", pa.uint8()),
                Column("instrument_identification_code", pa.string()),
            ],
        )

    @property
    def resolver(self):
        return TQEXPreTradeResolver(table=self)

class TQEXPreTradeResolver(TableResolver):
    def __init__(self, table: Table) -> None:
        super().__init__(table=table)

    def resolve(self, client: Client, parent_resource: Resource = None) -> Generator[Dict[str, Any], None, None]:
        for item_response in client.client.item_iterator("Turqouise-europe-Pre-Trade"):
            cleaned_row = {
                "distribution_time": datetime.fromisoformat(item_response.get("distributionTime")),
                "instrument_id": int(item_response.get("instrumentId")),
                "source_venue": int(item_response.get("sourceVenue")),
                "bid_market_size": float(item_response.get("bidMarketSize")),
                "bid_limit_price": float(item_response.get("bidLimitPrice")),
                "bid_yield": float(item_response.get("bidYield")),
                "bid_limit_size": float(item_response.get("bidLimitSize")),
                "offer_market_size": float(item_response.get("offerMarketSize")),
                "offer_limit_price": float(item_response.get("offerLimitPrice")),
                "offer_yield": float(item_response.get("offerYield")),
                "offer_limit_size": float(item_response.get("offerLimitSize")),
                "order_book_type": int(item_response.get("orderBookType")),
                "instrument_identification_code": item_response.get("instrumentIdentificationCode"),
            }
            yield cleaned_row
