"""src/streaming/data_validation/data_validation_femi.py.

Project-specific validation extensions.

Generic validation helpers live in datafun-streaming.
Add domain-specific validators here as requirements evolve.
"""

from datafun_streaming.data_validation.reference import (
    make_lookup_set,
    validate_reference_records,
)
from datafun_streaming.data_validation.validation_utils import add_validation_errors

__all__ = [
    "add_validation_errors",
    "make_lookup_set",
    "validate_money_amount",
    "validate_quantity",
    "validate_reference_records",
]


def validate_quantity(value: str) -> list[str]:
    """Return errors for an invalid quantity value.

    All quantity values must be integers greater than or equal to 1.
    """
    try:
        quantity = int(value)
    except ValueError:
        return [f"Quantity must be an integer: {value}"]

    if quantity < 1:
        return [f"Quantity must be at least 1: {value}"]

    return []


def validate_money_amount(value: str, *, field_name: str) -> list[str]:
    """Return errors for an invalid money amount.

    Money values must be numeric and greater than or equal to 0.

    Args:
        value: Text value to validate.
        field_name: Name of the field being validated.

    Returns:
        A list of error messages, or an empty list if valid.
    """
    try:
        amount = float(value)
    except ValueError:
        return [f"{field_name} must be numeric: {value}"]

    if amount < 0:
        return [f"{field_name} must be greater than or equal to 0: {value}"]

    return []
