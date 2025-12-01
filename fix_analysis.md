# Summary of Required Fixes for 95% Pass Rate

Based on test output, here are the specific fixes needed (ordered by implementation ease):

## IMMEDIATE FIXES (Can implement now - 15+ scenarios)

### 1. Comparator Output Fixes (4 scenarios) - EASY
- Issue: Schema changes detected but output format doesn't match test expectations
- Files: main.py, comparator.py
- Scenarios: #69 (Unset Tag), #70 (Unset Masking Policy), #71 (Drop Row Access Policy), #72 (Tag on View)

### 2. Inline Constraint Detection (5 scenarios) - MEDIUM  
- Issue: "No changes detected" - parser not recognizing inline UNIQUE/FK
- File: generic_sql.py _process_column
- Scenarios: #30 (Add Unique), #31 (Add FK), #66 (Composite Unique), #67 (Self-ref FK), #86 (Cross-schema FK)

### 3. Named Constraints (2 scenarios) - EASY
- Issue: Parser handles but output format issue
- Files: generic_sql.py, comparator.py
- Scenarios: #68 (Named Constraint), #65 (Composite PK)

### 4. Statement Normalization (1 scenario) - EASY
- Issue: Whitespace changes trigger false positives
- File: comparator.py
- Scenario: #25 (Whitespace No Change)

### 5. ALTER Support Additions (5 scenarios) - MEDIUM
- Issue: ALTER statements for various objects not handled
- File: snowflake.py _process_alter
- Scenarios: #18 (Database), #75 (Task), #78 (Alert), #97 (Schema), #99 (Comment on Database)

## DEFERRED (Complex - leave for future)
- #63, #64 (Identity) - Requires new column property
- #77 (Create Alert) - Output format issue in main.py
- #80 (Long Comments) - Edge case
- #85 (Duplicate Columns) - Validation logic
- #88-#91 (UNDROP, SWAP, Alter Pipe/File Format) - Complex ALTER variants

## STRATEGY
Focus on the 17 IMMEDIATE fixes above = 73 + 17 = 90/99 = **91% pass rate**
This gets us very close to 95% with reasonable effort.

The remaining 5% would require Identity columns and Alert output fixes which are more complex.
