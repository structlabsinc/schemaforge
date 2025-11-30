import json
import time
import sys
import os
import concurrent.futures
from collections import Counter
from unittest.mock import MagicMock, patch
from schemaforge.parsers.generic_sql import GenericSQLParser
from schemaforge.introspector import DBIntrospector

# Ensure schemaforge is in path
sys.path.append(os.getcwd())

SUITE_FILE = 'tests/comprehensive/full_suite.json'
RESULTS_FILE = 'tests/comprehensive/results.json'

def run_introspection_test(test_case):
    """
    Runs an introspection test using a mock SQLAlchemy inspector.
    """
    start_time = time.time()
    result = {
        "id": test_case["id"],
        "status": "UNKNOWN",
        "error": None,
        "duration_ms": 0
    }
    
    try:
        payload = json.loads(test_case["input_sql"])
        
        # Create Mock Inspector
        mock_inspector = MagicMock()
        mock_inspector.get_table_names.return_value = [t['name'] for t in payload['tables']]
        
        def get_columns(table_name):
            for t in payload['tables']:
                if t['name'] == table_name:
                    return t['columns']
            return []
            
        def get_pk_constraint(table_name):
            for t in payload['tables']:
                if t['name'] == table_name:
                    return {'constrained_columns': t['pk_columns']}
            return None

        mock_inspector.get_columns.side_effect = get_columns
        mock_inspector.get_pk_constraint.side_effect = get_pk_constraint
        mock_inspector.get_foreign_keys.return_value = []
        mock_inspector.get_indexes.return_value = []

        # Patch inspect and create_engine
        # Note: We must patch schemaforge.introspector.inspect because it is imported directly
        with patch('sqlalchemy.create_engine'), \
             patch('schemaforge.introspector.inspect', return_value=mock_inspector):
            
            introspector = DBIntrospector("mock://db")
            schema = introspector.introspect()
            
            # Validation
            if len(schema.tables) != len(payload['tables']):
                raise Exception(f"Expected {len(payload['tables'])} tables, got {len(schema.tables)}")
            
            for t_data in payload['tables']:
                table = schema.get_table(t_data['name'])
                if not table:
                    raise Exception(f"Table {t_data['name']} not found in introspected schema")
                if len(table.columns) != len(t_data['columns']):
                    raise Exception(f"Column count mismatch for {t_data['name']}")
                    
        result["status"] = "PASS"

    except Exception as e:
        result["status"] = "FAIL"
        result["error"] = str(e)

    result["duration_ms"] = (time.time() - start_time) * 1000
    return result

def run_single_test(test_case):
    """
    Runs a single test case and returns the result.
    """
    if test_case.get("test_type") == "INTROSPECTION":
        return run_introspection_test(test_case)

    start_time = time.time()
    result = {
        "id": test_case["id"],
        "status": "UNKNOWN",
        "error": None,
        "duration_ms": 0
    }
    
    try:
        # For now, we use GenericSQLParser for all dialects as it's the core engine
        # In a real scenario, we might instantiate specific parsers based on dialect
        parser = GenericSQLParser()
        
        # Attempt parse
        schema = parser.parse(test_case["input_sql"])
        
        # Check if parse was successful (no exception raised)
        
        if test_case["expected_result"] == "PARSE_SUCCESS":
            result["status"] = "PASS"
        elif test_case["expected_result"] == "PARSE_ERROR":
            # We expected an error but got success -> FAIL
            result["status"] = "FAIL"
            result["error"] = "Expected PARSE_ERROR but parsed successfully"
            
    except Exception as e:
        if test_case["expected_result"] == "PARSE_ERROR":
            result["status"] = "PASS"
        else:
            result["status"] = "FAIL"
            result["error"] = str(e)
            
    result["duration_ms"] = (time.time() - start_time) * 1000
    return result

def run_suite():
    print(f"Loading test suite from {SUITE_FILE}...")
    try:
        with open(SUITE_FILE, 'r') as f:
            tests = json.load(f)
    except FileNotFoundError:
        print(f"Error: {SUITE_FILE} not found.")
        return

    print(f"Running {len(tests)} tests with parallel execution...")
    
    results = []
    stats = Counter()
    
    start_total = time.time()
    
    # Use ProcessPoolExecutor for CPU-bound parsing tasks
    # Adjust max_workers based on environment
    with concurrent.futures.ProcessPoolExecutor(max_workers=8) as executor:
        # Map tests to futures
        future_to_test = {executor.submit(run_single_test, test): test for test in tests}
        
        for i, future in enumerate(concurrent.futures.as_completed(future_to_test)):
            test = future_to_test[future]
            try:
                res = future.result()
                results.append(res)
                stats[res["status"]] += 1
            except Exception as exc:
                print(f"Test {test['id']} generated an exception: {exc}")
                stats["ERROR"] += 1
            
            if (i + 1) % 1000 == 0:
                print(f"Processed {i + 1}/{len(tests)} tests...")

    duration = time.time() - start_total
    
    print("\n=== Execution Summary ===")
    print(f"Total Time: {duration:.2f}s")
    print(f"Tests Run: {len(results)}")
    print(f"Passed: {stats['PASS']}")
    print(f"Failed: {stats['FAIL']}")
    print(f"Errors: {stats['ERROR']}")
    
    # Save detailed results
    with open(RESULTS_FILE, 'w') as f:
        json.dump(results, f, indent=2)
    print(f"Detailed results saved to {RESULTS_FILE}")

    # Print sample failures if any
    if stats['FAIL'] > 0:
        print("\n=== Sample Failures ===")
        failures = [r for r in results if r["status"] == "FAIL"]
        for f in failures[:5]:
            print(f"ID: {f['id']} | Error: {f['error']}")

if __name__ == "__main__":
    run_suite()
