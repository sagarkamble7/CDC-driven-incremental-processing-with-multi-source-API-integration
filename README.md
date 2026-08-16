````markdown
# CDC-Driven Incremental Processing with Multi-Source API Integration

## Project Overview

An end-to-end **API-driven Retail Data Lakehouse** built on **Databricks Free Edition** using the Medallion Architecture.

The project integrates data from multiple API sources, performs schema conformance and data cleansing, implements **Delta Change Data Feed (CDF)** for incremental processing, and builds a **business-ready Gold Star Schema** with SCD Type 2 dimensions.

---

## Business Purpose

- Integrate retail data from multiple heterogeneous API sources.
- Create a centralized and reliable retail data platform for analytics.
- Standardize inconsistent schemas and data formats across sources.
- Maintain historical changes to customers and products using **SCD Type 2**.
- Process only changed data using **Delta Change Data Feed + watermark tracking**.
- Provide business-ready fact and dimension tables for reporting and analytics.
- Preserve historical accuracy when product/customer attributes change over time.

---

## Architecture

```text
                     Mockaroo API                 DummyJSON API
                   Synthetic Retail          Products / Users / Carts
                         |                             |
                         +-------------+---------------+
                                       |
                                       v
                          API Ingestion Layer
                        PySpark + Structured
                             Streaming
                                       |
                                       v
                              +---------------+
                              |    BRONZE     |
                              | Raw / Append- |
                              |     Only      |
                              |               |
                              | Raw API data  |
                              | JSON payload  |
                              | preservation  |
                              | Metadata &    |
                              | batch details |
                              +-------+-------+
                                      |
                                      v
                              +---------------+
                              |    SILVER     |
                              | Cleaned &     |
                              | Conformed Data|
                              |               |
                              | Schema        |
                              | standardization|
                              | JSON parsing  |
                              | Data cleansing|
                              | MD5 hashing   |
                              | MERGE-based   |
                              | processing    |
                              | CDF           |
                              +-------+-------+
                                      |
                                      v
                              +---------------+
                              |     GOLD      |
                              | Business Layer|
                              |               |
                              |  fact_orders  |
                              |       |       |
                              |       +-------+-------+
                              |       |       |       |
                              | dim_product dim_customer dim_date
                              |    SCD2       SCD2       |
                              +---------------+
````

---

## End-to-End Data Flow

### 1. API Sources

Data is ingested from:

* **Mockaroo** – synthetic, schema-controlled retail data.
* **DummyJSON** – public e-commerce-style data containing products, users and carts.

The two sources intentionally have different structures to demonstrate real-world multi-source integration challenges.

---

### 2. Bronze Layer

The Bronze layer stores the incoming API data in its raw form.

* Uses **Spark Structured Streaming** with `readStream` / `writeStream`.
* API responses are first landed as files in a **Unity Catalog Volume**.
* `trigger(availableNow=True)` provides bounded streaming execution.
* DummyJSON nested payloads are preserved as raw JSON instead of flattening them immediately.
* Bronze acts as the raw ingestion layer.

---

### 3. Silver Layer

The Silver layer converts heterogeneous API data into clean, conformed datasets.

#### Products

* Standardizes Mockaroo and DummyJSON product structures.
* Uses `sku` as the common business key.
* Parses nested DummyJSON JSON using `from_json()`.
* Handles different date formats from the two sources.
* Retains useful source-specific attributes as nullable columns.
* Uses MD5 `record_hash` for change detection.
* Uses Delta `MERGE` for idempotent processing.
* Delta CDF is enabled for downstream incremental processing.

#### Customers

* Parses and flattens DummyJSON user data.
* Uses `customer_key` as the business key.
* Excludes unnecessary sensitive fields such as bank, SSN, password and crypto information.
* Uses hash-based change detection and MERGE.

#### Order Lines

* Parses nested cart data.
* Uses `explode()` on the cart `products[]` array.
* Creates one row per product line item.
* Uses a composite grain of cart + product.
* Produces the `silver_order_lines` dataset.

---

## CDC & Incremental Processing

Delta **Change Data Feed (CDF)** is used to identify changes occurring in Silver tables.

The pipeline uses a separate:

```text
mia_catalog.gold._pipeline_state
```

control table to store:

```text
source_table
target_table
last_processed_version
updated_at
```

The watermark tells each Gold consumer which Delta version it has already processed.

### Incremental Flow

```text
Silver Delta Table
       │
       ▼
Delta Change Data Feed
       │
       ▼
Read last processed version
       │
       ▼
Read only new changes
       │
       ▼
Apply Gold transformation
       │
       ▼
Successful processing
       │
       ▼
Update pipeline watermark
```

CDF records **what changed**, while the watermark records **what the downstream process has already consumed**.

---

# Gold Layer

The Gold layer provides business-ready data using a **Star Schema**.

```text
                    dim_date
                       |
                       v
dim_product --------> fact_orders <-------- dim_customer
   (SCD2)                |                     (SCD2)
                         |
                      Measures
```

### Dimension Tables

#### `dim_date`

* Standard date dimension.
* Contains calendar attributes.
* Static/deterministic and can be regenerated.

#### `dim_product`

* Implements **SCD Type 2**.
* Maintains historical product versions.
* Uses surrogate keys.
* Tracks:

  * `effective_start_date`
  * `effective_end_date`
  * `is_current`
  * `dw_created_at`
  * `dw_updated_at`

#### `dim_customer`

* Implements **SCD Type 2**.
* Maintains historical customer versions.
* Uses surrogate keys and validity dates.

### Fact Table

#### `fact_orders`

* Grain: **one row per product line item per cart**.
* Contains:

  * Product surrogate key
  * Customer surrogate key
  * Date surrogate key
  * Quantity
  * Unit price
  * Line total
  * Discount
  * Discounted line total

The fact table uses **as-of joins** against SCD2 dimensions so historical orders resolve to the dimension version that was valid when the order occurred.

---

## Key Design Decisions

* **Medallion Architecture:** Bronze → Silver → Gold.
* **Multi-source integration:** Mockaroo + DummyJSON.
* **Structured Streaming:** Used for incremental file ingestion.
* **Unity Catalog Volumes:** Used instead of the disabled DBFS root.
* **Explicit schemas:** Used for controlled streaming ingestion.
* **Raw JSON preservation:** Nested API payloads remain intact in Bronze.
* **Schema conformance:** Different API structures are standardized in Silver.
* **Record hashing:** MD5 hash used to detect actual data changes.
* **Delta MERGE:** Used for idempotent Silver processing.
* **Change Data Feed:** Used for downstream CDC processing.
* **Watermark table:** Tracks the last processed Delta version per source-target pair.
* **SCD Type 2:** Maintains historical product and customer versions.
* **Surrogate keys:** Used for Gold dimension relationships.
* **As-of joins:** Ensure facts connect to historically correct dimension versions.
* **Star schema:** Separates business facts from descriptive dimensions.

---

## Technology Stack

| Technology                     | Purpose                                        |
| ------------------------------ | ---------------------------------------------- |
| **Databricks Free Edition**    | Data engineering platform / compute            |
| **PySpark**                    | Data transformation and processing             |
| **Spark Structured Streaming** | Incremental file ingestion                     |
| **Delta Lake**                 | ACID tables, MERGE, versioning and time travel |
| **Delta Change Data Feed**     | Row-level change detection                     |
| **Unity Catalog**              | Data governance, catalogs, schemas and Volumes |
| **Python**                     | Pipeline development                           |
| **Spark SQL**                  | SQL transformations and MERGE operations       |
| **Mockaroo API**               | Synthetic retail source                        |
| **DummyJSON API**              | Public e-commerce source                       |
| **Databricks Secrets**         | API credential management                      |

---

## Current Data Model

### Silver

```text
silver_products
silver_customers
silver_order_lines
```

### Gold

```text
dim_date
dim_product       -- SCD Type 2
dim_customer      -- SCD Type 2
fact_orders       -- SCD Type 1
_pipeline_state   -- CDC watermark/control table
```

---

## Project Highlights

* Multi-source API integration.
* Medallion Lakehouse architecture.
* Structured Streaming ingestion.
* Nested JSON processing with `from_json()`.
* Array processing with `explode()`.
* Schema conformance across heterogeneous sources.
* Hash-based change detection.
* Delta MERGE and idempotent processing.
* Delta Change Data Feed.
* Watermark-driven incremental processing.
* Production-style SCD Type 2 implementation.
* Surrogate key management.
* Historical as-of joins.
* Star schema dimensional modeling.
* Data governance and sensitive-field exclusion.

---

## Key Engineering Challenges

* Handling inconsistent field names and data formats across APIs.
* Cleaning malformed source field names such as trailing spaces.
* Conforming flat Mockaroo data with nested DummyJSON data.
* Mapping different product identifiers between source systems.
* Handling the absence of a true order timestamp in DummyJSON.
* Designing CDC processing using CDF and a separate watermark.
* Preventing surrogate-key collisions during incremental SCD2 loads.
* Maintaining historical correctness through as-of joins.
* Working within Databricks Free Edition storage and compute constraints.

---

## Project Outcome

This project demonstrates an end-to-end **modern data engineering pipeline** that takes heterogeneous API data and transforms it into a governed, incremental, historically accurate analytics model.

The final architecture combines:

**API ingestion → Structured Streaming → Bronze → Silver → CDF → Watermark-driven CDC → SCD2 Dimensions → Star Schema → Business-ready Gold data.**

---

## Author

**Sagar kamble** — Data Engineer  
