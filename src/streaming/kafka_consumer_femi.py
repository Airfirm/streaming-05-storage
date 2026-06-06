"""src/streaming/kafka_consumer_femi.py.

Kafka consumer: full pipeline example.

Reads sales messages from a Kafka topic and runs the full pipeline:
  - Validates each message against the data contract
  - Computes derived fields such as subtotal, tax amount, and total
  - Adds custom analytics fields for a new business problem
  - Stores accepted and rejected messages in DuckDB
  - Writes accepted messages to a CSV artifact

Technical Modification:
- Adds product category enrichment from products.csv
- Adds high-value order flag
- Adds order size band
- Tracks running sales total by region
- Stores rejected consumed messages in DuckDB

New Problem:
- Real-time sales quality and revenue monitoring

Author: O S
Date: 2026-06-06

Terminal command to run this file from the root project folder:

    uv run python -m streaming.kafka_consumer_femi
"""

# === DECLARE IMPORTS ===

import os
from pathlib import Path
from typing import Any, Final

from confluent_kafka.cimpl import OFFSET_BEGINNING, TopicPartition
from datafun_streaming.io.io_utils import append_csv_row, read_csv_as_lookup
from datafun_streaming.kafka.kafka_admin_utils import (
    create_admin_client,
    get_topic_message_count,
    topic_exists,
)
from datafun_streaming.kafka.kafka_connection_utils import verify_kafka_connection
from datafun_streaming.kafka.kafka_consumer_utils import (
    consume_kafka_message,
    create_consumer,
)
from datafun_streaming.kafka.kafka_settings import KafkaSettings
from datafun_streaming.stats.stats_utils import RunningStats
from datafun_toolkit.logger import get_logger, log_header, log_path
from dotenv import load_dotenv

from streaming.core.utils import log_env_vars
from streaming.data_engineering.derived_fields import enrich_message
from streaming.data_validation.data_contract_femi import (
    CONSUMED_FIELDNAMES,
    SALES_REQUIRED_FIELDS,
    validate_required_fields,
)
from streaming.storage.storage_femi import (
    init_db,
    log_storage_summary,
    write_rejected_record,
    write_valid_record,
)

# === CONFIGURE LOGGER ===

LOG = get_logger("C05", level="DEBUG")

# === LOAD ENVIRONMENT VARIABLES ===

load_dotenv(override=True)
log_env_vars(LOG)

# === DECLARE GLOBAL CONSTANTS ===

COURSE_NAME: Final[str] = "Streaming Data"
TIMEOUT_SECONDS: Final[float] = float(os.getenv("CONSUMER_TIMEOUT_SECONDS", "10.0"))
MAX_MESSAGES: Final[int] = int(os.getenv("CONSUMER_MAX_MESSAGES", "1000"))
HIGH_VALUE_TOTAL: Final[float] = float(os.getenv("HIGH_VALUE_TOTAL", "250.0"))

# === DECLARE CONSTANT PATHS ===

ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
OUTPUT_DIR: Final[Path] = DATA_DIR / "output"

OUTPUT_CSV: Final[Path] = OUTPUT_DIR / "consumed_sales_femi.csv"
OUTPUT_DB: Final[Path] = OUTPUT_DIR / "sales_femi.duckdb"

REGIONS_CSV: Final[Path] = DATA_DIR / "regions.csv"
PRODUCTS_CSV: Final[Path] = DATA_DIR / "products.csv"
CURRENCIES_CSV: Final[Path] = DATA_DIR / "currencies.csv"
DISCOUNT_CODES_CSV: Final[Path] = DATA_DIR / "discount_codes.csv"


# ==========================================================
# DEFINE SECTION A. ACQUIRE RESOURCES AND GET READY HELPERS
# ==========================================================


def log_paths() -> None:
    """Log run header and all paths."""
    log_header(LOG, "C05")
    LOG.info("========================")
    LOG.info("START consumer main()")
    LOG.info("========================")
    LOG.info("Project: %s", COURSE_NAME)
    log_path(LOG, "ROOT_DIR", ROOT_DIR)
    log_path(LOG, "DATA_DIR", DATA_DIR)
    log_path(LOG, "OUTPUT_CSV", OUTPUT_CSV)
    log_path(LOG, "OUTPUT_DB", OUTPUT_DB)
    log_path(LOG, "REGIONS_CSV", REGIONS_CSV)
    log_path(LOG, "PRODUCTS_CSV", PRODUCTS_CSV)
    log_path(LOG, "CURRENCIES_CSV", CURRENCIES_CSV)
    log_path(LOG, "DISCOUNT_CODES_CSV", DISCOUNT_CODES_CSV)


def load_settings() -> KafkaSettings:
    """Load settings from .env and log them.

    Returns:
        A KafkaSettings instance populated from environment variables.
    """
    LOG.info("Loading settings from .env...")
    settings = KafkaSettings.from_env()
    LOG.info(f"KAFKA_BOOTSTRAP_SERVERS  = {settings.bootstrap_servers}")
    LOG.info(f"KAFKA_TOPIC              = {settings.topic}")
    LOG.info(f"KAFKA_GROUP_ID           = {settings.group_id}")
    LOG.info(f"CONSUMER_TIMEOUT_SECONDS = {TIMEOUT_SECONDS}")
    LOG.info(f"CONSUMER_MAX_MESSAGES    = {MAX_MESSAGES}")
    LOG.info(f"HIGH_VALUE_TOTAL         = {HIGH_VALUE_TOTAL}")
    return settings


def verify_connection(settings: KafkaSettings) -> None:
    """Verify Kafka is reachable before doing anything else.

    Raises:
        SystemExit: If Kafka is not reachable.
    """
    LOG.info("Verifying Kafka connection...")

    try:
        verify_kafka_connection(settings)
        LOG.info("Kafka port is reachable.")
    except ConnectionError as error:
        LOG.error(str(error))
        raise SystemExit(1) from error


def verify_topic(settings: KafkaSettings) -> None:
    """Verify the Kafka topic exists and has messages.

    Raises:
        SystemExit: If the topic does not exist or is empty.
    """
    LOG.info("Verifying Kafka topic...")
    admin = create_admin_client(settings)

    if not topic_exists(admin, settings.topic):
        LOG.error(f"Topic {settings.topic!r} does not exist.")
        LOG.error("Run the producer first.")
        raise SystemExit(1)

    message_count = get_topic_message_count(admin, settings.topic, settings)
    LOG.info(f"Topic {settings.topic!r} exists.")
    LOG.info(f"Found {message_count} message(s) available.")

    if message_count == 0:
        LOG.error("Topic is empty. Run the producer first.")
        raise SystemExit(1)


def get_kafka_consumer(settings: KafkaSettings) -> Any:
    """Create a Kafka consumer subscribed to the topic.

    Resets offsets to the beginning so this example reads all available messages.

    Returns:
        A confluent_kafka.Consumer instance subscribed to the topic.
    """
    LOG.info("Creating Kafka consumer...")
    consumer = create_consumer(settings)

    consumer.subscribe(
        [settings.topic],
        on_assign=lambda c, partitions: c.assign(
            [
                TopicPartition(
                    partition.topic,
                    partition.partition,
                    OFFSET_BEGINNING,
                )
                for partition in partitions
            ]
        ),
    )

    LOG.info(f"Subscribed to topic: {settings.topic!r} (reading from beginning)")
    return consumer


# ===========================================================================
# DEFINE SECTION C. CONSUME AND PROCESS MESSAGES HELPERS
# ===========================================================================


def initialize_output() -> RunningStats:
    """Initialize output resources.

    Returns:
        A RunningStats instance.
    """
    LOG.info("Initializing output...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if OUTPUT_CSV.exists():
        OUTPUT_CSV.unlink()

    LOG.info(f"Output CSV cleared: {OUTPUT_CSV.name}")

    init_db(OUTPUT_DB)
    LOG.info(f"Database initialized: {OUTPUT_DB.name}")

    return RunningStats()


def load_reference_data() -> tuple[dict[str, float], dict[str, str]]:
    """Load reference data used for message enrichment.

    Returns:
        A tuple containing:
        - region tax rate lookup
        - product category lookup
    """
    LOG.info("Loading enrichment reference data...")

    region_rows = read_csv_as_lookup(
        REGIONS_CSV,
        key_field="region_id",
        value_field="tax_rate_pct",
    )

    region_lookup: dict[str, float] = {}

    for region_id, tax_rate_pct in region_rows.items():
        region_lookup[str(region_id)] = float(tax_rate_pct)

    product_rows = read_csv_as_lookup(
        PRODUCTS_CSV,
        key_field="product_id",
        value_field="category",
    )

    product_category_lookup: dict[str, str] = {}

    for product_id, category in product_rows.items():
        product_category_lookup[str(product_id)] = str(category)

    LOG.info(f"Found {len(region_lookup)} region tax rates.")
    LOG.info(f"Found {len(product_category_lookup)} product categories.")

    return region_lookup, product_category_lookup


def process_message(
    row: dict[str, Any],
    *,
    region_lookup: dict[str, float],
    product_category_lookup: dict[str, str],
    stats: RunningStats,
    region_sales_totals: dict[str, float],
) -> tuple[dict[str, Any] | None, list[str]]:
    """Process one consumed message.

    Steps:
      - Validate required fields
      - Enrich with subtotal, tax amount, and total
      - Add product category
      - Add high-value order flag
      - Add order size band
      - Track running sales total by region

    Args:
        row: A raw consumed Kafka message row.
        region_lookup: Tax rates by region_id.
        product_category_lookup: Product category by product_id.
        stats: Running statistics accumulator.
        region_sales_totals: Running sales totals by region_id.

    Returns:
        A tuple of enriched row and validation errors.
        If the message is invalid, enriched row is None.
    """
    errors = validate_required_fields(record=row, required_fields=SALES_REQUIRED_FIELDS)

    if errors:
        LOG.warning(f"Validation failed for order {row.get('order_id', '?')}")
        LOG.warning(f"errors={errors}")
        return None, errors

    try:
        enriched = enrich_message(row, region_lookup)

        total = float(enriched["total"])
        region_id = str(enriched["region_id"])
        product_id = str(enriched["product_id"])

        product_category = product_category_lookup.get(product_id, "unknown")
        enriched["product_category"] = product_category

        if total >= HIGH_VALUE_TOTAL:
            enriched["high_value_order"] = "yes"
        else:
            enriched["high_value_order"] = "no"

        if total >= 250:
            enriched["order_size_band"] = "high"
        elif total >= 100:
            enriched["order_size_band"] = "medium"
        else:
            enriched["order_size_band"] = "low"

        previous_region_total = region_sales_totals.get(region_id, 0.0)
        new_region_total = previous_region_total + total
        region_sales_totals[region_id] = new_region_total
        enriched["region_running_total"] = round(new_region_total, 2)

    except (KeyError, TypeError, ValueError) as error:
        error_message = f"Processing failed: {error}"
        LOG.warning(error_message)
        return None, [error_message]

    LOG.info(f"subtotal={enriched['subtotal']}")
    LOG.info(f"tax={enriched['tax_amount']}")
    LOG.info(f"total={enriched['total']}")
    LOG.info(f"product_category={enriched['product_category']}")
    LOG.info(f"order_size_band={enriched['order_size_band']}")
    LOG.info(f"high_value_order={enriched['high_value_order']}")
    LOG.info(f"region_running_total={enriched['region_running_total']}")
    LOG.info(f"running_total={stats.total + total:.2f}")

    stats.update(total)

    return enriched, []


def consume_messages(
    consumer: Any,
    *,
    region_lookup: dict[str, float],
    product_category_lookup: dict[str, str],
    stats: RunningStats,
) -> tuple[int, int]:
    """Consume and process messages from the Kafka topic.

    Runs until MAX_MESSAGES is reached or TIMEOUT_SECONDS elapses
    with no new message.

    Args:
        consumer: An open Kafka consumer subscribed to the topic.
        region_lookup: Tax rates by region_id.
        product_category_lookup: Product category by product_id.
        stats: Running statistics accumulator.

    Returns:
        A tuple of consumed_count and skipped_count.
    """
    LOG.info("Consuming messages...")
    LOG.info(f"Waiting for up to {MAX_MESSAGES} message(s).")
    LOG.info("Press CTRL+C to stop early.\n")

    consumed_count = 0
    skipped_count = 0
    region_sales_totals: dict[str, float] = {}

    while consumed_count + skipped_count < MAX_MESSAGES:
        row = consume_kafka_message(
            consumer=consumer,
            timeout_seconds=TIMEOUT_SECONDS,
        )

        if row is None:
            LOG.info(f"No message received within {TIMEOUT_SECONDS}s timeout.")
            LOG.info("Producer finished or paused. Stopping consumer.")
            break

        LOG.info(row)

        enriched, errors = process_message(
            row,
            region_lookup=region_lookup,
            product_category_lookup=product_category_lookup,
            stats=stats,
            region_sales_totals=region_sales_totals,
        )

        if enriched is None:
            skipped_count += 1

            write_rejected_record(
                db_path=OUTPUT_DB,
                record=row,
                errors=errors,
            )

            LOG.warning("MESSAGE REJECTED")
            LOG.warning(f"order={row.get('order_id', '?')}")
            LOG.warning(f"skipped={skipped_count}")
            continue

        write_valid_record(OUTPUT_DB, enriched)

        LOG.info("Wrote valid record to DuckDB:")
        LOG.info(f"  order={enriched['order_id']}")

        append_csv_row(
            path=OUTPUT_CSV,
            row={field: enriched.get(field, "") for field in CONSUMED_FIELDNAMES},
            fieldnames=CONSUMED_FIELDNAMES,
        )

        consumed_count += 1

        LOG.info("MESSAGE ACCEPTED")
        LOG.info(f"order={enriched['order_id']}")
        LOG.info(f"total=${enriched['total']:.2f}")
        LOG.info(f"category={enriched['product_category']}")
        LOG.info(f"band={enriched['order_size_band']}")
        LOG.info(f"high_value={enriched['high_value_order']}")
        LOG.info(f"region_running_total={enriched['region_running_total']}")
        LOG.info(f"consumed={consumed_count}")

        LOG.info("RUNNING STATS")
        LOG.info(f"total_sales=${stats.total:,.2f}")
        LOG.info(f"average=${stats.mean:,.2f}")
        LOG.info(f"min=${stats.minimum:,.2f}")
        LOG.info(f"max=${stats.maximum:,.2f}")

    return consumed_count, skipped_count


def save_artifacts() -> None:
    """Save output artifacts and log DuckDB summary results."""
    LOG.info("Saving artifacts...")
    log_path(LOG, "WROTE OUTPUT_CSV", OUTPUT_CSV)
    log_path(LOG, "WROTE OUTPUT_DB", OUTPUT_DB)
    log_storage_summary(OUTPUT_DB)


# ===========================================================================
# DEFINE SECTION E. EXIT AND CLEANUP HELPERS
# ===========================================================================


def log_summary(
    consumed_count: int,
    skipped_count: int,
    stats: RunningStats,
    settings: KafkaSettings,
) -> None:
    """Log final summary statistics.

    Args:
        consumed_count: Number of accepted messages.
        skipped_count: Number of rejected messages.
        stats: Running statistics accumulator.
        settings: Kafka settings.
    """
    LOG.info("Summary:")
    LOG.info(f"Consumed {consumed_count} message(s) from topic {settings.topic!r}.")
    LOG.info(f"Skipped  {skipped_count} message(s).")
    log_path(LOG, "OUTPUT_CSV", OUTPUT_CSV)
    log_path(LOG, "OUTPUT_DB", OUTPUT_DB)

    if stats.count > 0:
        LOG.info(f"  Total sales:  ${stats.total:,.2f}")
        LOG.info(f"  Average sale: ${stats.mean:,.2f}")
        LOG.info(f"  Minimum sale: ${stats.minimum:,.2f}")
        LOG.info(f"  Maximum sale: ${stats.maximum:,.2f}")

    LOG.info("========================")
    LOG.info("Consumer executed successfully!")
    LOG.info("========================")


# ===========================================================================
# MAIN FUNCTION
# ===========================================================================


def main() -> None:
    """Main entry point for the Kafka consumer."""
    log_paths()

    LOG.info("========================")
    LOG.info("SECTION A. Acquire")
    LOG.info("========================")

    settings = load_settings()
    verify_connection(settings)
    verify_topic(settings)
    consumer = get_kafka_consumer(settings)

    LOG.info("========================")
    LOG.info("SECTION C. Consume and Process Messages")
    LOG.info("========================")

    stats = initialize_output()
    region_lookup, product_category_lookup = load_reference_data()

    consumed_count = 0
    skipped_count = 0

    try:
        consumed_count, skipped_count = consume_messages(
            consumer,
            region_lookup=region_lookup,
            product_category_lookup=product_category_lookup,
            stats=stats,
        )
    finally:
        consumer.close()
        LOG.info("Kafka consumer closed.")

    save_artifacts()

    LOG.info("========================")
    LOG.info("SECTION E. Exit")
    LOG.info("========================")

    log_summary(consumed_count, skipped_count, stats, settings)


# === CONDITIONAL EXECUTION GUARD ===

if __name__ == "__main__":
    main()
