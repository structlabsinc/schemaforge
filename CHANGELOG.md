# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
