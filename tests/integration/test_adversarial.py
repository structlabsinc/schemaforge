import pytest
import re
import os
from schemaforge.parsers.generic_sql import GenericSQLParser
from schemaforge.comparator import Comparator

SCENARIOS_FILE = 'tests/fixtures/scenarios.md'

def parse_scenarios(filepath):
    if not os.path.exists(filepath):
        return []
        
    with open(filepath, 'r') as f:
        content = f.read()

    scenarios = []
    # Split by "## Scenario"
    parts = content.split('## Scenario')
    for part in parts[1:]: # Skip preamble
        try:
            lines = part.strip().split('\n')
            scenario_num = lines[0].strip()
            
            dialect_match = re.search(r'• Dialect: (.*)', part)
            if not dialect_match: continue
            dialect = dialect_match.group(1).strip()
            
            # Extract code blocks
            code_blocks = re.findall(r'```sql\n(.*?)```', part, re.DOTALL)
            
            if len(code_blocks) >= 2:
                scenarios.append({
                    'id': scenario_num,
                    'dialect': dialect,
                    'old_ddl': code_blocks[0].strip(),
                    'new_ddl': code_blocks[1].strip()
                })
        except Exception as e:
            print(f"Error parsing scenario {scenario_num if 'scenario_num' in locals() else 'Unknown'}: {e}")
            continue
            
    return scenarios

# Dynamically load scenarios
scenarios = parse_scenarios(SCENARIOS_FILE)

@pytest.mark.parametrize("scenario", scenarios, ids=[f"{s['id']}-{s['dialect']}" for s in scenarios])
def test_adversarial_scenario(scenario):
    """Run adversarial scenarios from markdown file"""
    parser = GenericSQLParser()
    
    # Parse Old
    schema_old = parser.parse(scenario['old_ddl'])
    
    # Parse New
    schema_new = parser.parse(scenario['new_ddl'])
    
    # Compare
    comparator = Comparator()
    plan = comparator.compare(schema_old, schema_new)
    
    # Determine if change occurred
    changes_found = bool(plan.new_tables or plan.dropped_tables or plan.modified_tables or plan.new_custom_objects)
    
    print(f"Scenario {scenario['id']}: Changes Found = {changes_found}")
    
    # We don't have explicit "Expected Result" in the markdown, assuming "Crash or No Crash" 
    # and maybe some basic sanity checks. The original runner just printed Diff/No Diff.
    # We assert no crash (implicit in pytest).
    # We can also assert that parsing produced *something* if DDL wasn't empty.
    
    if scenario['old_ddl'].strip():
        assert len(schema_old.tables) + len(schema_old.custom_objects) > 0 or "EMPTY" in scenario['old_ddl']
