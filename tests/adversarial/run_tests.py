print("Starting test runner...")
import re
import os
import sys
import traceback
from schemaforge.parsers.generic_sql import GenericSQLParser
from schemaforge.comparator import Comparator
from schemaforge.models import Schema

# Path to the scenarios file
SCENARIOS_FILE = '/home/shivam/.gemini/antigravity/brain/51c5cef9-a5f6-4735-8dee-055f16dc67db/scenarios_batch_1.md'

def parse_scenarios(filepath):
    with open(filepath, 'r') as f:
        content = f.read()

    scenarios = []
    # Regex to find scenarios
    # Assumes format:
    # ## Scenario N
    # • Dialect: <dialect>
    # • Old DDL:
    # ```sql
    # <content>
    # ```
    # • New DDL:
    # ```sql
    # <content>
    # ```
    
    # Split by "## Scenario"
    parts = content.split('## Scenario')
    for part in parts[1:]: # Skip preamble
        try:
            lines = part.strip().split('\n')
            scenario_num = lines[0].strip()
            
            dialect = re.search(r'• Dialect: (.*)', part).group(1).strip()
            
            # Extract code blocks
            code_blocks = re.findall(r'```sql\n(.*?)```', part, re.DOTALL)
            
            if len(code_blocks) >= 2:
                old_ddl = code_blocks[0].strip()
                new_ddl = code_blocks[1].strip()
                
                scenarios.append({
                    'id': scenario_num,
                    'dialect': dialect,
                    'old_ddl': old_ddl,
                    'new_ddl': new_ddl
                })
        except Exception as e:
            print(f"Error parsing scenario block: {e}")
            continue
            
    return scenarios

def run_test(scenario):
    print(f"Running Scenario {scenario['id']} ({scenario['dialect']})...", end='', flush=True)
    
    try:
        parser = GenericSQLParser() # Dialect specific parsing logic is inside GenericSQLParser for now or we can instantiate specific ones if needed
        # Note: In main.py we select parser based on dialect. 
        # For now, GenericSQLParser is used for all, but let's check if we need specific subclasses.
        # The tool uses GenericSQLParser for all dialects currently in the provided code context?
        # Let's check main.py logic if possible, but GenericSQLParser seems to be the main one.
        
        # Parse Old
        schema_old = parser.parse(scenario['old_ddl'])
        
        # Parse New
        schema_new = parser.parse(scenario['new_ddl'])
        
        # Compare
        comparator = Comparator()
        plan = comparator.compare(schema_old, schema_new)
        
        # Check if there are changes
        changes_found = False
        if plan.new_tables or plan.dropped_tables or plan.modified_tables:
            changes_found = True
            
        if changes_found:
            print(f" DIFF FOUND")
            # print(f"  New Tables: {[t.name for t in plan.new_tables]}")
            # print(f"  Dropped Tables: {[t.name for t in plan.dropped_tables]}")
            # for mod in plan.modified_tables:
            #     print(f"  Modified Table: {mod.table_name}")
        else:
            print(f" NO DIFF")
            
        return {
            'id': scenario['id'],
            'status': 'SUCCESS',
            'diff_found': changes_found
        }
        
    except Exception as e:
        print(f" CRASHED: {e}")
        # traceback.print_exc()
        return {
            'id': scenario['id'],
            'status': 'CRASH',
            'error': str(e)
        }

def main():
    if len(sys.argv) > 1:
        scenarios_file = sys.argv[1]
    else:
        scenarios_file = SCENARIOS_FILE
        
    print(f"Loading scenarios from: {scenarios_file}")
    scenarios = parse_scenarios(scenarios_file)
    print(f"Found {len(scenarios)} scenarios.")
    
    results = []
    for scenario in scenarios:
        result = run_test(scenario)
        results.append(result)
        
    print("\n" + "="*30)
    print("SUMMARY")
    print("="*30)
    crashes = [r for r in results if r['status'] == 'CRASH']
    diffs = [r for r in results if r['status'] == 'SUCCESS' and r['diff_found']]
    no_diffs = [r for r in results if r['status'] == 'SUCCESS' and not r['diff_found']]
    
    print(f"Total Scenarios: {len(results)}")
    print(f"Crashes: {len(crashes)}")
    print(f"Diffs Found: {len(diffs)}")
    print(f"No Diffs: {len(no_diffs)}")
    
    if crashes:
        print("\nCrashed Scenarios:")
        for r in crashes:
            print(f"  Scenario {r['id']}: {r['error']}")

if __name__ == "__main__":
    main()
