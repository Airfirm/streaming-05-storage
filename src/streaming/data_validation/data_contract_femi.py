"""src/streaming/data_validation/data_contract_femi.py.

Defines what a valid sales message looks like for this project.

Technical Modification:
- Added stronger money validation for unit_price.
- Made optional field validation safer.
- Added output fields for product category, high-value orders,
  order size bands, and region running totals.

New Problem:
- Real-time sales quality and revenue monitoring.
"""

from typing import Any, Final

from datafun_streaming.core.types import DataRecordDict
from datafun_streaming.data_validation.types import ValidationResult
from datafun_streaming.data_validation.validation_utils import (
    validate_boolean_text,
    validate_datetime,
    validate_positive_integer,
    validate_required_fields,
)

from streaming.data_validation.data_validation_femi import validate_money_amount

# === EVENT TABLE FIELDS ===

SALES_REQUIRED_FIELDS: Final[list[str]] = [
    "order_id",
    "datetime",
    "region_id",
    "currency_code",
    "product_id",
    "unit_price",
    "quantity",
    "is_online",
    "customer_id",
    "payment_method",
]

SALES_OPTIONAL_FIELDS: Final[list[str]] = [
    "is_new_customer",
    "device_type",
    "referral_source",
    "discount_code",
    "customer_note",
]

VALID_SALES_FIELDNAMES: Final[list[str]] = [
    *SALES_REQUIRED_FIELDS,
    *SALES_OPTIONAL_FIELDS,
]

# === REFERENCE TABLE FIELDS ===

REGIONS_REQUIRED_FIELDS: Final[list[str]] = [
    "region_id",
    "region_name",
    "country_code",
    "country_name",
    "currency_code",
    "tax_rate_pct",
    "timezone",
]

PRODUCTS_REQUIRED_FIELDS: Final[list[str]] = [
    "product_id",
    "product_name",
    "category",
    "level",
    "price_usd",
    "instructor",
]

CURRENCIES_REQUIRED_FIELDS: Final[list[str]] = [
    "currency_code",
    "currency_name",
    "symbol",
    "exchange_rate_to_usd",
    "rate_date",
]

DISCOUNT_CODES_REQUIRED_FIELDS: Final[list[str]] = [
    "discount_code",
    "discount_pct",
    "valid_from",
    "valid_to",
    "description",
]

# === ALLOWED VALUES ===

ALLOWED_DEVICE_TYPES: Final[set[str]] = {"mobile", "desktop", "tablet"}

ALLOWED_PAYMENT_METHODS: Final[set[str]] = {
    "credit_card",
    "paypal",
    "apple_pay",
    "gift_card",
}

ALLOWED_REFERRAL_SOURCES: Final[set[str]] = {
    "organic",
    "paid_search",
    "email",
    "social",
}

ALLOWED_CURRENCY_CODES: Final[set[str]] = {"USD", "CAD", "MXN"}

# === OUTPUT FIELD ORDER ===

CONSUMED_FIELDNAMES: Final[list[str]] = [
    *VALID_SALES_FIELDNAMES,
    "subtotal",
    "tax_amount",
    "total",
    "product_category",
    "order_size_band",
    "high_value_order",
    "region_running_total",
    "_kafka_key",
    "_kafka_partition",
    "_kafka_offset",
]

REJECTED_SALES_FIELDNAMES: Final[list[str]] = [
    *VALID_SALES_FIELDNAMES,
    "validation_errors",
]


def validate_sale_record(
    *,
    record: DataRecordDict,
    valid_region_ids: set[str],
    valid_product_ids: set[str],
) -> ValidationResult:
    """Validate one sale record against this project's data contract.

    Args:
        record: The message to validate.
        valid_region_ids: Valid region IDs from regions.csv.
        valid_product_ids: Valid product IDs from products.csv.

    Returns:
        ValidationResult with is_valid and errors.
    """
    errors: list[str] = []

    errors.extend(
        validate_required_fields(record=record, required_fields=SALES_REQUIRED_FIELDS)
    )

    if errors:
        return ValidationResult(is_valid=False, errors=errors)

    if record["region_id"] not in valid_region_ids:
        errors.append(f"Unknown region_id: {record['region_id']!r}")

    if record["product_id"] not in valid_product_ids:
        errors.append(f"Unknown product_id: {record['product_id']!r}")

    if record["currency_code"] not in ALLOWED_CURRENCY_CODES:
        errors.append(f"Invalid currency_code: {record['currency_code']!r}")

    if record["payment_method"] not in ALLOWED_PAYMENT_METHODS:
        errors.append(f"Invalid payment_method: {record['payment_method']!r}")

    errors.extend(validate_datetime(record["datetime"]))
    errors.extend(validate_positive_integer(record["quantity"]))
    errors.extend(validate_money_amount(record["unit_price"], field_name="unit_price"))
    errors.extend(validate_boolean_text(record["is_online"], field_name="is_online"))

    device_type = record.get("device_type", "")
    if device_type and device_type not in ALLOWED_DEVICE_TYPES:
        errors.append(f"Invalid device_type: {device_type!r}")

    referral_source = record.get("referral_source", "")
    if referral_source and referral_source not in ALLOWED_REFERRAL_SOURCES:
        errors.append(f"Invalid referral_source: {referral_source!r}")

    is_new_customer = record.get("is_new_customer", "")
    if is_new_customer:
        errors.extend(
            validate_boolean_text(is_new_customer, field_name="is_new_customer")
        )

    has_errors = bool(errors)
    is_result_valid = not has_errors

    return ValidationResult(is_valid=is_result_valid, errors=errors)


def keep_sales_fields(row: dict[str, Any]) -> dict[str, Any]:
    """Return only required sales fields in standard order."""
    return {field: row.get(field, "") for field in SALES_REQUIRED_FIELDS}
