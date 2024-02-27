from datetime import datetime
from typing import Any, Generator, Dict 
from zoneinfo import ZoneInfo

import pyarrow as pa
from cloudquery.sdk.schema import Column, Table
from cloudquery.sdk.scheduler import TableResolver
from cloudquery.sdk.schema.resource import Resource

from plugin.client import Client

class TRQXPreTrade(Table):
    def __init__(self) -> None:
        super().__init__(
            name="trqx_pre_trade",
            title="TRQX Trade Data",
            is_incremental=True,
            columns=[
                Column("message_timestamp", pa.timestamp("us", ZoneInfo("UTC"))),
                Column("rec_no", pa.uint64()),
                Column("market_data_group", pa.uint8()),
                Column("dss_id", pa.uint64()),
                Column("message_type", pa.string()),
                Column("order_id", pa.uint64()),
                Column("instrument_id", pa.uint64(),primary_key=True, unique=True),
                Column("instrument_identification_code", pa.string()),
                Column("currency", pa.string()),
                Column("source_venue", pa.uint8()),
                Column("order_book_type", pa.uint8()),
                Column("side", pa.string()),
                Column("size", pa.float64()),
                Column("price", pa.float64()),
                Column("old_price", pa.float64()),
                Column("old_size", pa.float64()),
            ],
        )

    @property
    def resolver(self):
        return TRQXPreTradeResolver(table=self)

class TRQXPreTradeResolver(TableResolver):
    def __init__(self, table: Table) -> None:
        super().__init__(table=table)

    def resolve(self, client: Client, parent_resource: Resource = None) -> Generator[Dict[str, Any], None, None]:
        for item_response in client.client.item_iterator("Turquoise-UK-Pre-Trade"):
            # Assuming item_response is a dict-like object with keys matching the CSV column names
            cleaned_row = {
                "message_timestamp": datetime.strptime(item_response.get("Message_Timestamp"), "%Y-%m-%d %H:%M:%S.%f"),
                "rec_no": int(item_response.get("RecNo")),
                "market_data_group": int(item_response.get("Market_Data_Group")),
                "dss_id": int(item_response.get("DSS_ID")),
                "message_type": item_response.get("Message_Type"),
                "order_id": int(item_response.get("Order_ID")),
                "instrument_id": int(item_response.get("Instrument_ID")),
                "instrument_identification_code": item_response.get("Instrument_Identification_Code"),
                "currency": item_response.get("Currency"),
                "source_venue": int(item_response.get("Source_Venue")),
                "order_book_type": int(item_response.get("Order_Book_Type")),
                "side": item_response.get("Side"),
                "size": float(item_response.get("Size")),
                "price": float(item_response.get("Price")),
                "old_price": float(item_response.get("Old_Price")) if item_response.get("Old_Price") else None,
                "old_size": float(item_response.get("Old_Size")) if item_response.get("Old_Size") else None,
            }
            yield cleaned_row
