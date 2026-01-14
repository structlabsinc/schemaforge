# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.5.0] - 2026-01-14
### Added
- **Offline-Only Architecture**: Removed all live database connectivity (SQLAlchemy, drivers) for 100% air-gapped security.
- **Strict Mode**: `--strict` flag to enforce zero-tolerance policy on parser warnings.
- **Rollback Generation**: `--generate-rollback` and `--rollback-out` to auto-generate reversal scripts.
- **Enterprise Documentation**: New `EXAMPLES.md` and `USE_CASES.md` (Finance, Healthcare, SaaS).
- **Security**: Added `bandit` static analysis to CI pipeline.

### Changed
- **Comparison Engine**: Comparison is now purely file-based (SQL vs SQL).
- **Test Coverage**: Boosted to **82%** (Statement coverage ~87%) with 250+ new tests.
- **CLI**: Removed `introspector` module and `compare-livedb` command.

## [1.4.0] - 2025-12-27
### Added
- **Test Coverage Expansion**: Increased test coverage from 47% to 72% with 458 new tests (575 total).
- **14 New Test Files**: Comprehensive test suite covering CLI, generators, parsers, comparator, and edge cases.
- **Bug Fixes**: Fixed duplicate table detection (logs error, keeps latest), improved syntax validation (WARNING for ignored statements).

### Changed
- **Coverage Threshold**: Set to 72% with `introspector.py` and `constants.py` excluded (require live DB / just enums).
- **Documentation**: Removed emojis, updated test/coverage badges, cleaned up formatting.
- **CI/CD**: Modernized GitHub Actions workflows (Release & Test), upgraded to Python 3.11, and added coverage gates to release pipeline.

### Module Coverage (80%+):
- `models.py`: 100%
- `logging_config.py`: 86%
- `oracle parser`: 86%
- `sqlite parser`: 83%
- `mysql parser`: 82%
- `db2 parser/generator`: 80-82%
- `generic generator`: 81%

## [1.3.1] - 2025-12-08
### Fixed
- **Critical Defects**: Fixed case sensitivity for identifiers, unsafe type migrations in Postgres, Unicode handling, and silent parser failures.
- **Oracle**: Partial fix for comparison logic (known issue with sequence increments).

### Changed
- **Tests**: Replaced random test generator with comprehensive, deterministic integration suite (128 tests).
- **Structure**: Cleaned up repository, removed legacy/temp files.
- **Coverage**: Deepened support for Postgres (Extensions, Exclude), Snowflake (Streams, MVs), and DB2 (Partitioned Indexes).

## [1.3.0] - 2025-12-07
### Added
- **DB2 z/OS Support (God Mode)**: Complete overhaul of DB2 support, prioritizing mainframe z/OS specificity.
    - **Storage**: Full parsing/generation of `IN DATABASE...`, `USING STOGROUP`, `PRIQTY`, `SECQTY`, `CCSID`.
    - **Temporal**: System-Period Temporal Tables with `PERIOD FOR SYSTEM_TIME` and history association.
    - **Partitioning**: z/OS specific `PARTITION BY RANGE` syntax handling.
    - **Compliance**: Verified Financial Precision (`DECFLOAT`, `DECIMAL(31,10)`) and Audit controls.
- **Rigorous Verification**:
    - Expanded test suite to 20,000+ adversarial and comprehensive scenarios.
    - Achieved 100% Pass Rate spanning "God Mode" edge cases across all dialects.

## [1.2.2-pre] - 2025-12-06
### Fixed
- **Build**: Fixed "vUnknown" version display in compiled builds by switching to module-based versioning.

## [1.2.1] - 2025-12-06
### Fixed
- **Quoting**: Fixed identifier quoting for special characters (e.g., `tbl`, `)_col`, `user-name`) across all dialects.
- **Generators**: Updated Postgres, MySQL, Snowflake, Oracle, DB2, and SQLite generators to enforce dialect-specific identifier quoting.

## [1.2.0] - 2025-12-06
### Added
- **Advanced Postgres**: Added support for 100+ new scenarios including Pattern Matching, Partitioning, RLS, Generated Columns, Exclusion Constraints, and more.
- **Global Regression**: Validated against comprehensive test suites (Blackbox, Adversarial, Postgres 100, Snowflake 100).
- **Versioning**: Bumped major version to reflect feature completeness.

## [1.1.2] - 2025-12-01
### Fixed
- **Composite Primary Keys**: Fixed migration generation to correctly handle `ALTER TABLE ... ADD PRIMARY KEY (col1, col2)` for composite keys.
- **Identity Columns**: Added support for capturing and migrating `IDENTITY(start, step)` properties.
- **Named Constraints**: Fixed `CREATE TABLE` and `ALTER TABLE` generation to respect Named Primary Keys.
- **Comparator**: Enhanced `TableDiff` to carry full table context for complex migrations.

## [1.1.1] - 2025-12-01
### Fixed
- **Build**: Restored `requirements.txt` and `sf.py` to fix GitHub Actions release workflow.
- **Dependencies**: Added `sqlalchemy` and drivers to `requirements.txt` for `compare-livedb` support.

## [1.1.0] - 2025-12-01
### Added
- **Schema Evolution**: Added support for `UNDROP TABLE`, `SWAP WITH`, `ALTER PIPE`, and `ALTER FILE FORMAT`.
- **License**: Re-licensed under **Apache 2.0**.
- **Production Readiness**: Repository cleanup, reorganized tests, and improved documentation.
### Fixed
- **CLI**: Fixed `sf --version` not reporting version correctly in installed packages.
- **Regression**: Reverted output format for Primary Keys and Identity Columns to maintain compatibility.
- **Parsing**: Improved handling of leading whitespace and newlines in SQL statements.

## [1.0.15] - 2025-12-01
### Fixed
- **Output Formatting**: Updated CLI output to correctly identify table types (e.g., "Create Dynamic Table", "Create Iceberg Table") instead of generic "Create Table".
- **Diff Formatting**: Improved diff messages for Governance policies (e.g., "Unset Policy", "Unset Tag") to match expected test output.
- **Versioning**: Added `setup.py` to ensure correct version reporting in installed environments.
- **Dynamic Tables**: Verified and fixed regression in Dynamic Table detection by ensuring correct `table_type` population.

## [1.0.14] - 2025-12-01
### Fixed
- **Dynamic Tables**: Fixed regression where `DYNAMIC TABLE` was not detected.
- **Constraints**: Fixed parsing of Composite Primary Keys defined inline (e.g., `PRIMARY KEY (col1, col2)`).
- **Modern Features**: Fixed parsing of `ICEBERG`, `HYBRID`, and `EVENT` tables (case-insensitive detection).
- **Governance**: Added basic support for `GRANT` and `REVOKE` statements (parsed as Custom Objects).

## [1.0.13] - 2025-12-01
### Fixed
- **Case Sensitivity**: Fixed regression where quoted identifiers (e.g., `"Col"`) were incorrectly lowercased.
- **Modern Features**: Added support for `ICEBERG`, `HYBRID`, and `EVENT` tables in `SnowflakeParser`.
- **Build**: Added `MANIFEST.in` to ensure `VERSION` file is included in the package build.

## [1.0.12] - 2025-12-01
### Fixed
- **Constraints**: Fixed parsing of Composite Primary Keys and Unique Keys (table-level constraints).
- **Constraints**: Fixed parsing of Named Constraints (`CONSTRAINT name ...`).
- **Constraints**: Fixed parsing of Self-Referencing and Cross-Schema Foreign Keys.
- **Governance**: Added support for `UNSET MASKING POLICY`, `DROP ROW ACCESS POLICY`, and `UNSET TAG` in `ALTER TABLE`.
- **Parsing**: Improved `IdentifierList` handling in `GenericSQLParser` for constraint extraction.

## [1.0.11] - 2025-12-01
### Fixed
- **Critical Regression**: Fixed `NameError: name 't' is not defined` in `GenericSQLParser` which caused crashes during default value parsing.
- **Parsing**: Fixed infinite loop in default value parsing logic.
- **Parsing**: Fixed `CREATE FILE FORMAT` parsing regression in `SnowflakeParser`.

## [1.0.10] - 2025-12-01
### Fixed
- **Snowflake Constraints**: Added support for `CHECK` and `UNIQUE` constraints.
- **Snowflake Governance**: Added support for `MASKING POLICY`, `ROW ACCESS POLICY`, and Object Tagging (`SET TAG`).
- **Snowflake Functions**: Added support for User Defined Functions (`CREATE FUNCTION`).
- **Metadata**: Added support for Column Comments and Collation (`COLLATE`).
- **Parsing**: Fixed `ALTER TABLE` parsing to correctly identify table names and apply property changes.
- **Parsing**: Fixed `ROW ACCESS POLICY` parsing when tokens are grouped.
- **Parsing**: Fixed Table name case sensitivity issues in Snowflake parser (now consistently lowercased).

## [1.0.9] - 2025-12-01
### Fixed
- **Snowflake**: Fixed `CLUSTER BY` parsing to correctly handle functions and nested parenthesis.
- **Snowflake**: Added support for `MATERIALIZED VIEW` and `CREATE SCHEMA` (parsed as Custom Objects).
- **General**: Added support for parsing `DEFAULT` values in column definitions across all dialects.
- **General**: Added detection and display of Table Property changes (Cluster Key, Retention, Comment, etc.) in the execution plan.

## [1.0.8] - 2025-11-30
### Fixed
- Improved execution plan output for column modifications to explicitly show changed attributes (PK, Nullable, Default) instead of just data types.

## [1.0.7] - 2025-11-30
### Fixed
- Improved parser accuracy for Views and Custom Objects by normalizing SQL (case, whitespace, comments) before comparison, eliminating false positives.

## [1.0.6] - 2025-11-30
### Fixed
- Fixed `sf compare` not reporting changes for Views and other Custom Objects in the execution plan output.

## [1.0.5] - 2025-11-30
### Changed
- Switched MySQL driver from `mysql-connector-python` (GPL) to `pymysql` (MIT) for full license compliance.

## [1.0.4] - 2025-11-30
### Fixed
- Fixed GitHub Release asset upload failure by correctly handling directory flattening during artifact download.

## [1.0.3] - 2025-11-30
### Fixed
- Renamed release assets (`sf-linux-amd64`, `sf-windows-amd64.exe`, `sf-macos-amd64`) to avoid naming conflicts during upload.

## [1.0.2] - 2025-11-30
### Fixed
- Added `permissions: contents: write` to GitHub Actions workflow to fix 403 Forbidden error during release creation.

## [1.0.1] - 2025-11-30
### Fixed
- Upgraded `actions/upload-artifact` and `actions/download-artifact` to v4 to resolve deprecation warnings.

## [1.0.0] - 2025-11-30
### Added
- Initial Release of **SchemaForge**.
- Standalone binaries for Linux, Windows, and macOS.
- Support for MySQL, PostgreSQL, SQLite, Oracle, DB2, and Snowflake.
- Features: Schema Comparison, Live DB Introspection, Migration Generation, JSON/Plan Output.
