# God Mode Adversarial Report

## Overview
This report documents the semantic traps and complexity drivers introduced in the "God Mode" adversarial schema (`god_mode.sql`) and the mitigations implemented in the DaC tool to handle them.

## Traps & Mitigations

*   **Trap**: **Invisible Characters (ZWSP)**
    *   *Description*: Zero Width Space (U+200B) inserted inside quoted identifiers (`"col​1"`) and string literals. Visually identical to normal text but causes comparison failures.
    *   *Mitigation*: `_clean_name` and `_clean_type` explicitly strip `\u200b` (and other invisible characters) before comparison.

*   **Trap**: **Nested Block Comments**
    *   *Description*: `/* Outer /* Inner */ Outer */`. Standard regex or simple state machines fail to track nesting depth, often closing the comment too early at the first `*/`.
    *   *Mitigation*: Implemented a custom `_strip_comments` preprocessor that tracks nesting level and correctly handles nested `/* ... */` blocks.

*   **Trap**: **Snowflake Dollar Quoting (`$$`)**
    *   *Description*: `$$` used as string delimiters for comments and procedure bodies. Can contain unescaped quotes (`'`) and newlines.
    *   *Mitigation*: `_strip_comments` and `_clean_type` recognize `$$` delimiters and treat the content as a single string block, preventing tokenizer confusion.

*   **Trap**: **Grouped Identifiers / Token Flattening**
    *   *Description*: `sqlparse` sometimes groups identifiers and types (e.g., `c3 BYTEINT`) into a single `Identifier` or `IdentifierList` token, especially for unknown types or complex nesting.
    *   *Mitigation*: Implemented recursive token flattening (`_flatten_tokens`) to unpack deeply nested token structures into a linear stream for processing.

*   **Trap**: **Ambiguous Type Aliases**
    *   *Description*: Dialects use different names for the same type (e.g., `FLOAT8` vs `DOUBLE PRECISION`, `BOOL` vs `TINYINT(1)`).
    *   *Mitigation*: Expanded `_clean_type` with a comprehensive alias map, including custom/vendor-specific aliases like `BYTEINT` and `BOOLISH`.

*   **Trap**: **Multi-Word Types**
    *   *Description*: Types like `DOUBLE PRECISION` and `CHARACTER VARYING` are split into multiple tokens.
    *   *Mitigation*: `_process_column` lookahead logic consumes multiple tokens when specific keywords (`PRECISION`, `VARYING`) are detected.

*   **Trap**: **Parenthesis Handling**
    *   *Description*: `sqlparse` wraps column definitions in a `Parenthesis` object, which needs careful unwrapping.
    *   *Mitigation*: `_parse_columns_and_constraints` detects and strips outer parentheses while preserving inner structure.

*   **Trap**: **Case-Insensitive Name Collision**
    *   *Description*: `Orders` and `orders` in the same schema.
    *   *Mitigation*: The parser normalizes all identifiers to lowercase (unless strictly quoted and case-sensitive dialect options are enabled). In this test, they collide, which correctly reflects the ambiguity in case-insensitive systems.

## Migration Hint
To migrate from OLD to NEW schema safely:
1.  **Create New Table**: `t_new_feature`.
2.  **Alter Table**: `u_table_Ａ` add `new_col_zwsp`.
3.  **Alter Table**: `t_ambiguous` modify `c2` length and `c4` type.
4.  **Update View**: `v6` (semantic update).
5.  **Update Procedure**: `proc_sql` and `proc_js_dynamic` (body updates).
6.  **Update Task**: `task_1` (schedule change).
