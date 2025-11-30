import os
import sys
import json
import subprocess
import concurrent.futures
import time
from generator import ScenarioGenerator

DAC_CLI = "python3 schemaforge/main.py"
# DAC_CLI = "./dist/sf"

def run_single_scenario(scenario_dir, dialect):
    source = os.path.join(scenario_dir, "source.sql")
    target = os.path.join(scenario_dir, "target.sql")
    expected_file = os.path.join(scenario_dir, "expected.json")
    
    with open(expected_file, 'r') as f:
        expected_changes = json.load(f)
        
    # Run Compare
    cmd = f"export PYTHONPATH=$PYTHONPATH:. && {DAC_CLI} compare --source {source} --target {target} --dialect {dialect} --plan --json-out {os.path.join(scenario_dir, 'plan.json')} --sql-out {os.path.join(scenario_dir, 'migration.sql')}"
    
    start = time.time()
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    duration = time.time() - start
    
    if result.returncode != 0:
        return {
            "status": "CRASH",
            "error": result.stderr,
            "duration": duration
        }
        
    # Validate Output
    # 1. Check if plan matches expectations (loose check)
    # We check if the number of operations roughly matches or if keywords exist
    # For now, let's check if "No changes detected" aligns with expectations
    
    output_lower = result.stdout.lower()
    no_changes_expected = "no changes" in [e.lower() for e in expected_changes]
    no_changes_detected = "no changes detected" in output_lower
    
    if no_changes_expected != no_changes_detected:
        return {
            "status": "FAIL_MISMATCH",
            "error": f"Expected no changes: {no_changes_expected}, Got no changes: {no_changes_detected}",
            "duration": duration
        }

    # 1.5 Validate Artifacts Existence
    plan_json_path = os.path.join(scenario_dir, 'plan.json')
    if not os.path.exists(plan_json_path):
        return {
            "status": "FAIL_NO_JSON",
            "error": "plan.json was not generated",
            "duration": duration
        }
    
    try:
        with open(plan_json_path, 'r') as f:
            json.load(f)
    except json.JSONDecodeError:
        return {
            "status": "FAIL_INVALID_JSON",
            "error": "plan.json is not valid JSON",
            "duration": duration
        }

    migration_sql_path = os.path.join(scenario_dir, 'migration.sql')
    if not os.path.exists(migration_sql_path):
        return {
            "status": "FAIL_NO_SQL",
            "error": "migration.sql was not generated",
            "duration": duration
        }

    # 2. Idempotency Check
    # Apply migration to source (conceptually) -> Parse generated SQL -> Compare with Target
    # Since we don't have a live DB, we can verify that the GENERATED SQL is parsable
    # and that (Source + Migration) == Target.
    # This is hard without a DB.
    # Alternative Idempotency: Parse(Target) -> Generate -> Parse(Generated) == Parse(Target)
    # This verifies the Generator's correctness for the Target schema.
    
    # Let's run a separate check: Parse Target -> Generate -> Parse -> Compare
    # We can use the tool's compare command on (target.sql) and (generated_from_target)
    # But we need to generate SQL for the target first.
    # The tool doesn't have a "generate DDL from file" command directly exposed easily via CLI 
    # except via compare(empty, target).
    
    # Let's try: compare empty.sql target.sql -> save to target_gen.sql -> compare target.sql target_gen.sql
    empty_file = os.path.join(scenario_dir, "empty.sql")
    open(empty_file, 'w').close()
    target_gen_file = os.path.join(scenario_dir, "target_gen.sql")
    
    cmd_gen = f"export PYTHONPATH=$PYTHONPATH:. && {DAC_CLI} compare --source {empty_file} --target {target} --dialect {dialect} --sql-out {target_gen_file}"
    res_gen = subprocess.run(cmd_gen, shell=True, capture_output=True, text=True)
    
    if res_gen.returncode != 0:
         return {
            "status": "FAIL_GEN_CRASH",
            "error": res_gen.stderr,
            "duration": duration
        }
        
    # Now compare target vs target_gen
    cmd_verify = f"export PYTHONPATH=$PYTHONPATH:. && {DAC_CLI} compare --source {target} --target {target_gen_file} --dialect {dialect} --plan"
    res_verify = subprocess.run(cmd_verify, shell=True, capture_output=True, text=True)
    
    if res_verify.returncode != 0:
         return {
            "status": "FAIL_VERIFY_CRASH",
            "error": res_verify.stderr,
            "duration": duration
        }
        
    if "no changes detected" not in res_verify.stdout.lower():
         return {
            "status": "FAIL_IDEMPOTENCY",
            "error": "Generated SQL does not match original Target schema",
            "duration": duration
        }

    return {
        "status": "PASS",
        "duration": duration
    }

def run_massive(dialect, count=50):
    output_dir = f"tests/blackbox/scenarios/{dialect}"
    gen = ScenarioGenerator(dialect, output_dir)
    
    print(f"Generating {count} scenarios for {dialect}...")
    scenarios = []
    for i in range(count):
        sid = f"run_{i:04d}"
        s = gen.generate_scenario(sid)
        gen.save_scenario(s)
        scenarios.append(os.path.join(output_dir, sid))
        
    print(f"Running {count} tests...")
    results = {"PASS": 0, "CRASH": 0, "FAIL": 0}
    failures = []
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=8) as executor:
        future_to_sid = {executor.submit(run_single_scenario, s_dir, dialect): s_dir for s_dir in scenarios}
        
        for i, future in enumerate(concurrent.futures.as_completed(future_to_sid)):
            s_dir = future_to_sid[future]
            try:
                res = future.result()
                status = res['status']
                if status == 'PASS':
                    results["PASS"] += 1
                elif 'CRASH' in status:
                    results["CRASH"] += 1
                    failures.append((s_dir, res))
                else:
                    results["FAIL"] += 1
                    failures.append((s_dir, res))
            except Exception as e:
                print(f"Exception in runner: {e}")
                results["CRASH"] += 1
            
            if (i+1) % 10 == 0:
                print(f"Processed {i+1}/{count}...")

    print("\n=== Massive Test Results ===")
    print(f"Dialect: {dialect}")
    print(f"Total: {count}")
    print(f"PASS: {results['PASS']}")
    print(f"FAIL: {results['FAIL']}")
    print(f"CRASH: {results['CRASH']}")
    
    if failures:
        print("\nTop 5 Failures:")
        for f in failures[:5]:
            print(f"{f[0]}: {f[1]['status']} - {f[1].get('error', '')[:200]}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dialect", required=True)
    parser.add_argument("--count", type=int, default=50)
    args = parser.parse_args()
    
    run_massive(args.dialect, args.count)
