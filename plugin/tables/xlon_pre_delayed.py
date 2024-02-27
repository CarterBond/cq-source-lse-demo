from datetime import datetime
from typing import Any, Generator
from zoneinfo import ZoneInfo

from cloudquery.sdk.scheduler import TableResolver
import pyarrow as pa
from cloudquery.sdk.schema import Column, Table
from cloudquery.sdk.schema.resource import Resource

from plugin.client import Client

class XLONPreTrade(Table):
    def __init__(self) -> None:
        super().__init__(
            name="xlon_pre_trade",
            title="LSE Pre-Trade Data",
            is_incremental=True,
            columns=[
                Column("message_timestamp", pa.timestamp("us", ZoneInfo("UTC"))),
                Column("rec_no", pa.int64()),
                Column("market_data_group", pa.int32()),
                Column("dss_id", pa.int64()),
                Column("message_type", pa.string()),
                Column("order_id", pa.int64()),
                Column("instrument_id", pa.int64(), primary_key=True, unique=True),
                Column("instrument_identification_code", pa.string()),
                Column("currency", pa.string()),
                Column("source_venue", pa.int32()),
                Column("order_book_type", pa.int32()),
                Column("side", pa.string()),
                Column("size", pa.float64()),
                Column("price", pa.float64()),
                Column("old_price", pa.float64()),
                Column("old_size", pa.float64()),
            ],
        )

    @property
    def resolver(self):
        return XLONPreTradeResolver(table=self)

class XLONPreTradeResolver(TableResolver):
    def __init__(self, table) -> None:
        super().__init__(table=table)

    def resolve(
            self, client: Client, parent_resource: Resource
        ) -> Generator[Any, None, None]:
        for item_response in client.client.item_iterator("LSE-Pre-Trade"):
            print(item_response)
            cleaned_row = {
                "message_timestamp": str(item_response.get("Message_Timestamp")),
                "rec_no": item_response.get("RecNo"),
                "market_data_group": item_response.get("Market_Data_Group"),
                "dss_id": item_response.get("DSS_ID"),
                "message_type": item_response.get("Message_Type"),
                "order_id": item_response.get("Order_ID"),
                "instrument_id": item_response.get("Instrument_ID"),
                "instrument_identification_code": item_response.get("Instrument_Identification_Code"),
                "currency": item_response.get("Currency"),
                "source_venue": item_response.get("Source_Venue"),
                "order_book_type": item_response.get("Order_Book_Type"),
                "side": item_response.get("Side"),
                "size": float(item_response.get("Size") or 0),  # Default to 0 if empty
                "price": float(item_response.get("Price") or 0),  
                "old_price": float(item_response.get("Old_Price") or 0),  
                "old_size": float(item_response.get("Old_Size") or 0),  
            }
            yield cleaned_row
