from datetime import datetime
from typing import Any, Generator
from zoneinfo import ZoneInfo

import pyarrow as pa
from cloudquery.sdk.scheduler import TableResolver
from cloudquery.sdk.schema import Column
from cloudquery.sdk.schema import Table
from cloudquery.sdk.schema.resource import Resource

from plugin.client import Client


class XLONPostDelayed(Table):
    def __init__(self) -> None:
        super().__init__(
            name="xlon_post_delayed",
            title="LSE Post-trade Delayed",
            is_incremental=True,
            columns=[
                Column("distribution_timestamp", pa.timestamp("us", ZoneInfo("UTC"))),
                Column("trading_timestamp", pa.timestamp("us", ZoneInfo("UTC"))),
                Column("transaction_id", pa.uint64(), primary_key=True, unique=True),
                Column("instrument_id", pa.uint64()),
                Column("isin_instrument_code", pa.string()),
                Column("currency", pa.string()),
                Column("price", pa.float64()),
                Column("quantity", pa.uint64()),
            ],
        )

    @property
    def resolver(self):
        return XLONPostDelayedResolver(table=self)


class XLONPostDelayedResolver(TableResolver):
    def __init__(self, table) -> None:
        super().__init__(table=table)

    def resolve(
        self, client: Client, parent_resource: Resource
    ) -> Generator[Any, None, None]:
        for item_response in client.client.xlon_iterator():
            cleaned_row = {
                "distribution_timestamp": datetime.fromisoformat(item_response.get("distributionTime")),
                "trading_timestamp": datetime.fromisoformat(item_response.get("tradingDateAndTime")),
                "transaction_id": int(item_response.get("transactionIdentificationCode")),
                "instrument_id": int(item_response.get("instrumentId")),
                "isin_instrument_code": item_response.get("instrumentIdentificationCode"),
                "currency": item_response.get("priceCurrency"),
                "price": float(item_response.get("mifidPrice")),
                "quantity": int(item_response.get("mifidQuantity")),
            }
            if cleaned_row["trading_timestamp"] > datetime.now(tz=ZoneInfo("UTC")):
                continue  # trade has impossible datetime (it happened in the future)
            if cleaned_row["isin_instrument_code"] is None:
                continue  # trade must have an instrument code
            if cleaned_row["currency"] is None:
                continue  # trade must have a currency
            if cleaned_row["quantity"] < 0:
                continue  # trade must have a positive quantity
            yield cleaned_row
