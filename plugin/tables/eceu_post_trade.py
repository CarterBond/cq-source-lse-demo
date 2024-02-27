from datetime import datetime
from typing import Any, Generator, Dict
from zoneinfo import ZoneInfo

import pyarrow as pa
from cloudquery.sdk.schema import Column, Table
from cloudquery.sdk.scheduler import TableResolver
from cloudquery.sdk.schema.resource import Resource
from plugin.client import Client

class ECEUPostTrade(Table):
    def __init__(self) -> None:
        super().__init__(
            name="eceu_post_trade",
            title="ECEU Post-Trade Data",
            is_incremental=True,
            columns=[
                Column("distribution_time", pa.timestamp("us"), ZoneInfo("UTC")),
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
                Column("venue_of_publication", pa.string()),
                Column("mifid_flags", pa.string()),
                Column("total_number_of_transactions", pa.uint64()),
                Column("third_country_trading_venue_of_execution", pa.string()),
                Column("missing_price", pa.float64()),
            ],
        )

    @property
    def resolver(self):
        return ECEUPostTradeResolver(table=self)

class ECEUPostTradeResolver(TableResolver):
    def __init__(self, table: Table) -> None:
        super().__init__(table=table)

    def resolve(self, client: Client, parent_resource: Resource = None) -> Generator[Dict[str, Any], None, None]:
        for item_response in client.client.item_iterator("TRADEcho-NL-Post-Trade"):
            cleaned_row = {
                "distribution_time": datetime.fromisoformat(item_response.get("distributionTime")),
                "source_venue": int(item_response.get("sourceVenue")),
                "instrument_id": int(item_response.get("instrumentId")),
                "transaction_identification_code": int(item_response.get("transactionIdentificationCode")),
                "mifid_price": str(item_response.get("mifidPrice")),
                "mifid_quantity": str(item_response.get("mifidQuantity")),
                "trading_date_and_time": datetime.fromisoformat(item_response.get("tradingDateAndTime")),
                "instrument_identification_code_type": item_response.get("instrumentIdentificationCodeType"),
                "instrument_identification_code": item_response.get("instrumentIdentificationCode"),
                "price_notation": item_response.get("priceNotation"),
                "price_currency": item_response.get("priceCurrency"),
                "notional_amount": str(item_response.get("notionalAmount")),
                "notional_currency": item_response.get("notionalCurrency"),
                "venue_of_execution": item_response.get("venueOfExecution"),
                "publication_date_and_time": datetime.fromisoformat(item_response.get("publicationDateAndTime")),
                "transaction_to_be_cleared": bool(int(item_response.get("transactionToBeCleared"))),
                "measurement_unit": item_response.get("measurementUnit"),
                "quantity_in_measurement_unit": float(item_response.get("quantityInMeasurementUnit")) if item_response.get("quantityInMeasurementUnit") else None,
                "type": item_response.get("type"),
                "venue_of_publication": item_response.get("venueOfPublication"),
                "mifid_flags": item_response.get("mifidFlags"),
                "total_number_of_transactions": int(item_response.get("totalNumberOfTransactions")),
                "third_country_trading_venue_of_execution": item_response.get("thirdCountryTradingVenueOfExecution"),
                "missing_price": str(item_response.get("missingPrice")) if item_response.get("missingPrice") else None,
            }
            yield cleaned_row
