from datetime import datetime
from typing import Any, Generator, Dict

import pyarrow as pa
from cloudquery.sdk.schema import Column, Table
from cloudquery.sdk.scheduler import TableResolver
from cloudquery.sdk.schema.resource import Resource

from plugin.client import Client

class TQEXPostTrade(Table):
    def __init__(self) -> None:
        super().__init__(
            name="tqex_post_trade",
            title="TQEX Post-Trade Data",
            is_incremental=True,
            columns=[
                Column("distribution_time", pa.timestamp("us")),
                Column("source_venue", pa.uint8()),
                Column("instrument_id", pa.uint64()),
                Column("transaction_identification_code", pa.uint64(), primary_key=True, unique=True),  
                Column("mifid_price", pa.float64()),
                Column("mifid_quantity", pa.uint64()),
                Column("trading_date_and_time", pa.timestamp("us")),
                Column("instrument_identification_code_type", pa.string()),
                Column("instrument_identification_code", pa.string()),
                Column("price_notation", pa.string()),
                Column("price_currency", pa.string()),
                Column("notional_amount", pa.float64()),
                Column("notional_currency", pa.string()),
                Column("venue_of_execution", pa.string()),
                Column("publication_date_and_time", pa.timestamp("us")),
                Column("transaction_to_be_cleared", pa.bool_()),
                Column("measurement_unit", pa.string()),
                Column("quantity_in_measurement_unit", pa.float64()),
                Column("type", pa.string()),
                Column("mifid_flags", pa.string()),
            ],
        )

    @property
    def resolver(self):
        return TQEXPostTradeResolver(table=self)

class TQEXPostTradeResolver(TableResolver):
    def __init__(self, table: Table) -> None:
        super().__init__(table=table)

    def resolve(self, client: Client, parent_resource: Resource = None) -> Generator[Dict[str, Any], None, None]:
        for item_response in client.client.item_iterator("Turqouise-europe-Post-Trade"):
            cleaned_row = {
                "distribution_time": datetime.fromisoformat(item_response.get("distributionTime")),
                "source_venue": int(item_response.get("sourceVenue")),
                "instrument_id": int(item_response.get("instrumentId")),
                "transaction_identification_code": int(item_response.get("transactionIdentificationCode")),
                "mifid_price": float(item_response.get("mifidPrice")),
                "mifid_quantity": int(item_response.get("mifidQuantity")),
                "trading_date_and_time": datetime.fromisoformat(item_response.get("tradingDateAndTime")),
                "instrument_identification_code_type": item_response.get("instrumentIdentificationCodeType"),
                "instrument_identification_code": item_response.get("instrumentIdentificationCode"),
                "price_notation": item_response.get("priceNotation"),
                "price_currency": item_response.get("priceCurrency"),
                "notional_amount": float(item_response.get("notionalAmount")) if item_response.get("notionalAmount") else None,
                "notional_currency": item_response.get("notionalCurrency"),
                "venue_of_execution": item_response.get("venueOfExecution"),
                "publication_date_and_time": datetime.fromisoformat(item_response.get("publicationDateAndTime")),
                "transaction_to_be_cleared": bool(int(item_response.get("transactionToBeCleared"))),
                "measurement_unit": item_response.get("measurementUnit"),
                "quantity_in_measurement_unit": float(item_response.get("quantityInMeasurementUnit")) if item_response.get("quantityInMeasurementUnit") else None,
                "type": item_response.get("type"),
                "mifid_flags": item_response.get("mifidFlags"),
            }
            yield cleaned_row
