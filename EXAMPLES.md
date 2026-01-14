# SchemaForge Use Case Registry

This collection demonstrates how SchemaForge solves complex database lifecycle challenges across different industries. Each scenario includes `v1.sql` (Initial State) and `v2.sql` (Target State) to demonstrate the migration capabilities.

---

## 1. Finance: Compliance & Audit (DB2 z/OS)

**Directory:** `examples/finance_compliance/`

Financial institutions require absolute precision and unalterable audit trails. This scenario demonstrates migrating a standard General Ledger to a **System-Period Temporal Table**.

### Key Features Demonstrated:
*   **Temporal Versioning:** Adding `PERIOD FOR SYSTEM_TIME` and a History Table (`general_ledger_hist`) to automatically track every row change.
*   **Legacy Storage Parameters:** Handling `USING STOGROUP`, `PRIQTY`, and `SECQTY` to ensure mainframe storage compliance.
*   **Zero-Loss Precision:** Managing `DECIMAL(19, 4)` types without rounding errors.

**Try it:**
```bash
sf compare \
  --source examples/finance_compliance/v1.sql \
  --target examples/finance_compliance/v2.sql \
  --dialect db2 \
  --plan
```

---

## 2. Healthcare: HIPAA & Privacy (PostgreSQL)

**Directory:** `examples/healthcare_hipaa/`

Healthcare providers must protect Patient Health Information (PHI) and enforce strict access controls. This scenario upgrades a patient database to use **Row Level Security (RLS)**.

### Key Features Demonstrated:
*   **Row Level Security:** Implementing `CREATE POLICY` to restrict data access based on the user's role and organization ID.
*   **PII Encryption:** Transitioning cleartext `ssn` to `bytea` for encrypted storage.
*   **PHI Tagging:** Using `COMMENT ON` to tag sensitive columns for downstream audit scanners.

**Try it:**
```bash
sf compare \
  --source examples/healthcare_hipaa/v1.sql \
  --target examples/healthcare_hipaa/v2.sql \
  --dialect postgres \
  --plan
```

---

## 3. SaaS: Scale & Multi-Tenancy (MySQL)

**Directory:** `examples/saas_multitenant/`

High-growth SaaS platforms eventually hit the limits of single-table performance. This scenario demonstrates implementing **Table Partitioning** and **Full-Text Search** for scale.

### Key Features Demonstrated:
*   **Range Partitioning:** Splitting the `orders` table by year (`PARTITION BY RANGE`) to improve query performance and data archival.
*   **Sharding Prep:** Adding a `region_id` column to prepare for future horizontal sharding.
*   **Search Optimization:** Adding a `FULLTEXT` index for fast user lookup.

**Try it:**
```bash
sf compare \
  --source examples/saas_multitenant/v1.sql \
  --target examples/saas_multitenant/v2.sql \
  --dialect mysql \
  --plan
```

---

## 4. Retail: Strict Schema Evolution (SQLite)

**Directory:** `examples/ecommerce/` (Existing)
*Note: This is a basic example included in the root examples folder.*

Demonstrates strict typing in SQLite (`STRICT` tables) and modifying column constraints in a database that typically doesn't support complex ALTERS.

### Key Features Demonstrated:
*   **SQLite Rigor:** Using `STRICT` mode to enforce types.
*   **Migration Logic:** Handling SQLite's "Create-Copy-Drop" migration pattern automatically.

---

## 5. Analytics: Modern Pipelines (Snowflake)

**Directory:** `examples/analytics_snowflake/`

Demonstrates using Snowflake's native Data Engineering constructs to build a self-refreshing analytics pipeline.

### Key Features Demonstrated:
*   **Dynamic Tables:** Replacing complex ETL with declarative `CREATE DYNAMIC TABLE`.
*   **Clustering:** Adding `CLUSTER BY` for query performance optimization.
*   **Governance:** Applying Object Tags (`SET TAG`) for cost tracking.

**Try it:**
```bash
sf compare \
  --source examples/analytics_snowflake/v1.sql \
  --target examples/analytics_snowflake/v2.sql \
  --dialect snowflake \
  --plan
```

---

## 6. Logistics: High Throughput (Oracle)

**Directory:** `examples/logistics_oracle/`

A shipment tracking system optimized for massive write volume and fast primary key lookups.

### Key Features Demonstrated:
*   **Index Organized Tables (IOT):** Storing data within the B-Tree index for O(1) access.
*   **Hash Partitioning:** Distributing data across 16 partitions to prevent hot-spots.

**Try it:**
```bash
sf compare \
  --source examples/logistics_oracle/v1.sql \
  --target examples/logistics_oracle/v2.sql \
  --dialect oracle \
  --plan
```

---

## 7. Corporate: Hierarchy & Config (MSSQL)

**Directory:** `examples/corporate_mssql/`

Managing complex corporate data structures using T-SQL specific extensions.

### Key Features Demonstrated:
*   **HierarchyID:** Replaces inefficient self-joins with an optimized hierarchical data type.
*   **XML Type:** Storing configuration blobs in validated `XML` columns.
*   **Clustered Indexes:** Explicit control over physical data ordering.

**Try it:**
```bash
sf compare \
  --source examples/corporate_mssql/v1.sql \
  --target examples/corporate_mssql/v2.sql \
  --dialect mssql \
  --plan
```
