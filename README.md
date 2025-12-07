# SchemaForge - Database as Code

![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)
![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)
![Version](https://img.shields.io/badge/version-1.2.0-orange.svg)
![Tests](https://img.shields.io/badge/tests-20k%2B-green.svg)

**SchemaForge** is the definitive enterprise-grade engine for Schema Management. It treats your database schema as a first-class software artifact, enabling "Infrastructure as Code" for your most critical data assets.

Designed for high-compliance environments (Banking, Healthcare, Government), SchemaForge ensures that what you declare in code is **exactly** what exists in your database — down to the byte-level storage parameter.

---

## 🚀 Why SchemaForge?

### 1. Mainframe-Grade Precision (DB2 z/OS)
We don't just speak "SQL"; we speak **Storage**.
*   **Legacy Preservation**: We parse and respect 20-year old storage definitions (`USING STOGROUP`, `PRIQTY`, `SECQTY`, `CCSID`).
*   **Compliance Ready**: Native support for **System-Period Temporal Tables** (`PERIOD FOR SYSTEM_TIME`) for audit trails.
*   **Financial Accuracy**: Rigorously tested against `DECIMAL(31, 10)` and `DECFLOAT` for zero-rounding-error environments.

### 2. Battle-Hardened Reliability
Most tools fail at the edges. We thrive there.
*   **20,000+ Tests**: Our test suite covers "God Mode" scenarios including invisible characters, SQL injection-like patterns, and mixed-case quoting hell.
*   **Idempotency Guarantee**: Running a generated migration against the target state results in **Zero Drift**.
*   **Zero Regressions**: Every release passes a massive adversarial regression suite.

### 3. Universal "God Mode" Dialect Support
We support the deepest quirks of every major dialect.

| Dialect | Status | Key "God Mode" Features |
| :--- | :--- | :--- |
| **DB2 (z/OS)** | **Platinum** | `IN DATABASE.TS`, `AUX TABLE`, `PARTITION BY RANGE`, `STOGROUP` |
| **Snowflake** | **Gold** | `DYNAMIC TABLE`, `ICEBERG`, `STREAM`, `PIPE`, `MASKING POLICY` |
| **PostgreSQL** | **Gold** | `EXCLUSION`, `RLS`, `PARTITION BY`, `JSONB`, `GIN/GIST` |
| **Oracle** | **Silver** | `TABLESPACE`, `SYNONYM`, `SEQUENCE` |
| **MySQL** | **Silver** | `VIEW`, `PROCEDURE`, `FULLTEXT` |
| **SQLite** | **Silver** | `STRICT`, `WITHOUT ROWID`, `VIRTUAL TABLE` |

---

## 📦 Installation

### Pre-built Binaries (Recommended)
Download the standalone binary for your OS (Zero Dependencies).
*   [Latest Release](https://github.com/structlabsinc/schemaforge/releases) (`sf-linux`, `sf-windows.exe`, `sf-macos`)

### Python Package
```bash
pip install schemaforge
sf --version
```

---

## 🛠 Usage

### 1. The "What If?" Plan
Preview changes without touching the database.
```bash
sf compare \
    --source ./schema/v2.sql \
    --target "postgres://user:pass@prod-db/app" \
    --dialect postgres \
    --plan
```

### 2. Live Database Introspection
Generate a faithful representation of your production database.
```bash
sf compare-livedb \
    --source "db2://user:pass@mainframe:50000/BLUDB" \
    --target ./local_schema/ \
    --dialect db2 \
    --sql-out migration.sql
```

### 3. CI/CD Integration
Generate machine-readable artifacts for your pipeline to enforce policies (e.g., "No destructive drops").
```bash
sf compare \
    --source v1.sql --target v2.sql \
    --dialect snowflake \
    --plan --json-out plan.json --sql-out deploy.sql --silent
```

---

## 🔎 Deep Dive: DB2 z/OS Support

SchemaForge v1.2.0 introduces groundbreaking support for IBM DB2 on z/OS.

**Example: Temporal Table & Storage Groups**
We parse this faithfully:
```sql
CREATE TABLE "compliance_logs" (
  "log_id" BIGINT GENERATED ALWAYS AS IDENTITY,
  "sys_start" TIMESTAMP(12) NOT NULL GENERATED ALWAYS AS ROW BEGIN,
  "sys_end" TIMESTAMP(12) NOT NULL GENERATED ALWAYS AS ROW END,
  "trans_id" TIMESTAMP(12) GENERATED ALWAYS AS TRANSACTION START ID,
  PERIOD FOR SYSTEM_TIME (sys_start, sys_end)
) IN DATABASE "db_audit"."ts_logs"
  USING STOGROUP "sg_fast" PRIQTY 500 CCSID EBCDIC;
```

---

## 🤝 Governance & License
SchemaForge is open-source under the **Apache 2.0** license.
Enterprise support and SLAs available for Fortune 500 deployments.

See [CHANGELOG.md](CHANGELOG.md) for version history.
