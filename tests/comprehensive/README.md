# Comprehensive Test Suite Documentation

## Overview
This directory contains a procedurally generated test suite of **20,000+ SQL test cases** targeting 6 major dialects (Snowflake, PostgreSQL, MySQL, SQLite, Oracle, DB2). The suite covers a wide range of object types and complexity levels, from simple tables to "God Mode" adversarial scenarios.

## Test Suite Artifacts
*   **Generator Script**: `test_generator.py` - The Python script used to generate the suite.
*   **Full Suite**: `full_suite.json` - The generated JSON file containing all 20,000+ test cases.

## Test Execution Plan

### Recommended Execution Strategy
Given the size of the suite (20k tests), we recommend a parallelized execution strategy:

1.  **Batching**: Split the `full_suite.json` into batches of 500 tests.
2.  **Parallelism**: Run 4-8 concurrent workers, each processing a batch.
3.  **Timeout**: Set a hard timeout of 5 seconds per test case to catch infinite loops in parsers.
4.  **Environment**: Use a machine with at least 16GB RAM and 8 vCPUs.

### Estimated Runtime
*   **Single Thread**: ~2.5 hours (assuming 0.5s per test).
*   **8 Threads**: ~20 minutes.

### Execution Command (Example)
```bash
python3 run_suite.py --input tests/comprehensive/full_suite.json --workers 8 --output results.json
```

## Risk Matrix

| Risk Category | Description | Severity | Detected By |
| :--- | :--- | :--- | :--- |
| **Parser Crashes** | Unhandled exceptions on valid but complex SQL (e.g., nested comments). | Critical | CHAOS / GODMODE tests |
| **Semantic Blindness** | Failure to detect subtle changes (e.g., ZWSP, type aliases). | High | ADVERSARIAL tests |
| **Dialect Confusion** | Applying MySQL logic to Snowflake DDL (or vice versa). | Medium | NEGATIVE / FUZZ tests |
| **Performance Degradation** | Exponential slowdown on deep nesting or large files. | Medium | LOAD / STRESS tests |
| **Tokenization Errors** | Incorrect splitting of multi-word types or identifiers. | High | HARD / GODMODE tests |

## Summary Report Template

Use the following template to report test results:

```markdown
# Test Execution Report

**Date**: YYYY-MM-DD
**Suite Version**: 1.0
**Total Tests**: 20,000

### Summary Counts
*   **PARSE_SUCCESS**: [Count]
*   **PARSE_WARN**: [Count]
*   **PARSE_ERROR**: [Count] (Expected vs Unexpected)
*   **DIFF_DETECTED**: [Count]
*   **MIGRATION_APPLY_OK**: [Count]
*   **MIGRATION_APPLY_FAIL**: [Count]

### Top 20 Failure Modes
1.  [Failure Type] - [Count]
2.  ...

### Sample Failures (Top 10)
1.  **ID**: SNOW-TABLE-GODMODE-1234
    *   **Input**: `CREATE TABLE "t\u200bname" ...`
    *   **Error**: `Unexpected token '\u200b'`
    *   **Expected**: PARSE_SUCCESS

### Remediation Plan
*   [ ] Fix ZWSP handling in tokenizer.
*   [ ] Increase recursion limit for nested CTEs.
*   [ ] Update alias map for DB2 types.
```
