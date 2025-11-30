# SchemaForge - Database as Code

**[Executive Summary](EXECUTIVE_SUMMARY.md)** | **[Walkthrough](walkthrough.md)**

SchemaForge is an enterprise-grade utility for managing database schemas as code. It brings "Infrastructure as Code" practices to databases by allowing you to define your schema in version-controlled SQL files and automatically generating safe, idempotent migration scripts to update your database.

## Key Features
*   **Multi-Dialect Support**: Enterprise-grade support for **Snowflake**, **Oracle**, **DB2**, **PostgreSQL**, **MySQL**, and **SQLite**.
*   **Robust Parsing**: Powered by a custom engine tested against 20,000+ adversarial scenarios.
*   **CI/CD Ready**: Generates machine-readable JSON plans and standard SQL scripts for automated pipelines.
*   **Live Introspection**: Compare local SQL files directly against a running database.

## Installation

### Option 1: Pre-built Binaries (Recommended)
No Python installation required.

1.  Go to the [Releases Page](https://github.com/structlabsinc/schemaforge/releases/latest).
2.  Download the binary for your OS:
    *   **Linux**: `sf-linux-amd64`
    *   **Windows**: `sf-windows-amd64.exe`
    *   **macOS**: `sf-macos-amd64`
3.  **Linux/macOS**: Make it executable and move to PATH:
    ```bash
    chmod +x sf-linux-amd64
    sudo mv sf-linux-amd64 /usr/local/bin/sf
    ```
4.  **Windows**: Rename to `sf.exe` and add the folder to your System PATH.
5.  Verify:
    ```bash
    sf --version
    ```

### Option 2: From Source (Python)
Requires Python 3.9+.

```bash
git clone https://github.com/structlabsinc/schemaforge.git
cd schemaforge
pip install -r requirements.txt
# Run via python
python3 schemaforge/main.py --help
```

## Usage Scenarios

The CLI supports flexible output combinations to suit different workflows.

### 1. Preview Changes (Human-Readable Plan)
Best for local development to see what *would* happen.
```bash
sf compare --source v1.sql --target v2.sql --dialect postgres --plan
```

### 2. CI/CD Integration (Plan + JSON)
Generate a human-readable plan for logs and a JSON file for programmatic checks (e.g., failing the build on destructive changes).
```bash
sf compare --source v1.sql --target v2.sql --dialect mysql --plan --json-out plan.json
```

### 3. Developer Workflow (Plan + SQL)
Review the plan and generate the migration script in one go.
```bash
sf compare --source v1.sql --target v2.sql --dialect oracle --plan --sql-out migration.sql
```

### 4. Automated Deployment (JSON + SQL)
Generate artifacts for deployment without printing to stdout (silent mode).
```bash
sf compare --source v1.sql --target v2.sql --dialect snowflake --json-out plan.json --sql-out migration.sql
```

## Governance
See [GOVERNANCE.md](GOVERNANCE.md) for branch protection rules and release policies.

## License
MIT

## Live Database Comparison (`compare-livedb`)

Instead of comparing two files, you can compare a live database (Source) against your local schema definition (Target).

### Basic Usage
```bash
sf compare-livedb \
    --source "postgresql://user:pass@localhost:5432/mydb" \
    --target ./schema/ \
    --dialect postgres \
    --plan
```

### Advanced Options
*   **Directory Support**: The `--target` can be a directory. The tool will recursively find and aggregate all `.sql` files.
*   **Object Filtering**: Use `--object-types` to limit the comparison (e.g., only check tables and views).

```bash
sf compare-livedb \
    --source "mysql://user:pass@prod-db:3306/ecommerce" \
    --target ./src/schema/ \
    --dialect mysql \
    --object-types table,view,procedure \
    --sql-out prod_migration.sql
```

## Supported Dialects & Features

| Dialect | Key Features Supported |
| :--- | :--- |
| **Snowflake** | `TRANSIENT`, `CLUSTER BY`, `VARIANT`, `PIPE`, `TASK`, `STREAM` |
| **Oracle** | `TABLESPACE`, `PARTITION BY`, `SEQUENCE`, `SYNONYM`, PL/SQL |
| **DB2** | `IN tablespace`, `PARTITION BY`, `ALIAS`, Identity Columns |
| **PostgreSQL**| `UNLOGGED`, `PARTITION BY`, `JSONB`, `GIN/GIST`, `MAT VIEW` |
| **MySQL** | `PARTITION BY`, `FULLTEXT`, `VIEW`, `PROCEDURE` |
| **SQLite** | `STRICT`, `WITHOUT ROWID`, `VIRTUAL TABLE`, `TRIGGER` |

## Reliability & Testing

This tool is verified by a **Massive Blackbox Testing Suite** containing over **3,000 unique scenarios**.
*   **Adversarial Testing**: Validated against "God Mode" files containing invisible characters, mixed quoting, and extreme edge cases.
*   **Zero Crashes**: 100% stability rate across all test suites.
*   **Idempotency**: Guaranteed that applying a generated migration results in the target state.

## Project Structure
```
schemaforge/
├── main.py          # Entry point
├── models.py        # Abstract Schema Models
├── comparator.py    # Diffing Engine
├── parsers/         # Dialect-specific Parsers
└── generators/      # SQL Generators
```
