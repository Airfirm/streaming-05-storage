# Database Integration with Streaming Pipelines

## Project Overview

This project demonstrates how to integrate a Kafka streaming pipeline with local file and database storage.
The pipeline reads sales data, validates records, sends valid messages to a
Kafka topic, consumes messages from Kafka, enriches the data, and stores the results as output artifacts.

The project uses Python, Kafka, CSV files, and DuckDB to show how streaming
data can be processed and saved for later analysis.

## Project Files

The main custom project files are:

- `kafka_admin_femi.py`
- `kafka_producer_femi.py`
- `kafka_consumer_femi.py`
- `data_contract_femi.py`
- `data_validation_femi.py`
- `storage_femi.py`

## Dataset

The project uses sales data stored in CSV files in the `data/` folder.

The main input file is:

- `data/sales.csv`

The project also uses reference data files:

- `data/regions.csv`
- `data/products.csv`
- `data/currencies.csv`
- `data/discount_codes.csv`

The sales data includes fields such as order ID, datetime, region ID, currency
code, product ID, unit price, quantity, online status, customer ID, and payment method.

The reference files help validate and enrich the streaming sales messages. For
example, `regions.csv` provides tax rates by region, and `products.csv` provides product category information.

## Streaming Pipeline

The pipeline runs in three main steps.

First, `kafka_admin_femi.py` prepares or verifies the Kafka topic.

Next, `kafka_producer_femi.py` reads sales records from `sales.csv`, validates
each record against the data contract, writes rejected producer records to a
CSV file, and sends valid records to the Kafka topic.

Finally, `kafka_consumer_femi.py` reads messages from the Kafka topic,
validates them again, enriches accepted records with derived fields, writes
accepted records to a CSV file, and stores accepted and rejected records in a DuckDB database.

## Signals

The original signals in the sales data include:

- order ID
- datetime
- region ID
- currency code
- product ID
- unit price
- quantity
- online status
- customer ID
- payment method
- device type
- referral source
- discount code

The project also creates new analytical signals, including:

- subtotal
- tax amount
- total
- product category
- order size band
- high-value order flag
- running sales total by region

These signals help turn raw sales messages into useful business intelligence.

## Technical Modification

For my technical modification, I expanded the consumer and storage pipeline.

I modified the consumer so each accepted sales message is enriched with
additional analytics fields. The consumer now adds product category, order
size band, high-value order status, and running sales total by region.

I also modified the storage logic so accepted and rejected consumed messages
can be stored in DuckDB. This makes the pipeline more useful because the
results are saved and can be queried after the stream finishes.

The output file names were also customized with `_femi` so they are easier to
identify as my project artifacts.

## New Problem

I applied the streaming pipeline to a real-time sales quality and revenue monitoring problem.

Instead of only sending and receiving sales messages, the system now helps
monitor sales performance and data quality. It can identify accepted and
rejected records, calculate sales totals, flag high-value orders, group orders
by product category, and track running revenue by region.

This type of system could help a business monitor incoming sales activity and
quickly understand which regions or product categories are producing the most revenue.

## Output Artifacts

The project creates output files in the `data/output/` folder.

Expected output files include:

```text
data/output/producer_rejected_sales_femi.csv
data/output/consumed_sales_femi.csv
data/output/sales_femi.duckdb
