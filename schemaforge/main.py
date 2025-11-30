import argparse
import sys
from schemaforge.parsers.mysql import MySQLParser
from schemaforge.parsers.postgres import PostgresParser
from schemaforge.parsers.sqlite import SQLiteParser
from schemaforge.parsers.oracle import OracleParser
from schemaforge.parsers.db2 import DB2Parser
from schemaforge.parsers.snowflake import SnowflakeParser

from schemaforge.generators.mysql import MySQLGenerator
from schemaforge.generators.postgres import PostgresGenerator
from schemaforge.generators.sqlite import SQLiteGenerator
from schemaforge.generators.oracle import OracleGenerator
from schemaforge.generators.db2 import DB2Generator
from schemaforge.generators.snowflake import SnowflakeGenerator

from schemaforge.comparator import Comparator

def get_parser(dialect):
    if dialect == 'mysql': return MySQLParser()
    if dialect == 'postgres': return PostgresParser()
    if dialect == 'sqlite': return SQLiteParser()
    if dialect == 'oracle': return OracleParser()
    if dialect == 'db2': return DB2Parser()
    if dialect == 'snowflake': return SnowflakeParser()
    raise ValueError(f"Unknown dialect: {dialect}")

def get_generator(dialect):
    if dialect == 'mysql': return MySQLGenerator()
    if dialect == 'postgres': return PostgresGenerator()
    if dialect == 'sqlite': return SQLiteGenerator()
    if dialect == 'oracle': return OracleGenerator()
    if dialect == 'db2': return DB2Generator()
    if dialect == 'snowflake': return SnowflakeGenerator()
    raise ValueError(f"Unknown dialect: {dialect}")
    raise ValueError(f"Unknown dialect: {dialect}")

def read_sql_source(path: str) -> str:
    """
    Reads SQL content from a file or recursively from a directory.
    """
    import os
    import glob
    
    if os.path.isfile(path):
        with open(path, 'r') as f:
            return f.read()
            
    elif os.path.isdir(path):
        content = []
        # Recursive glob for .sql files
        sql_files = glob.glob(os.path.join(path, '**/*.sql'), recursive=True)
        # Sort to ensure deterministic order
        sql_files.sort()
        
        if not sql_files:
            raise ValueError(f"No .sql files found in directory: {path}")
            
        for sql_file in sql_files:
            with open(sql_file, 'r') as f:
                content.append(f.read())
        
        return "\n".join(content)
        
    else:
        raise ValueError(f"Path not found: {path}")

def main():
    parser = argparse.ArgumentParser(description='SchemaForge - Database as Code')
    parser.add_argument('command', choices=['compare', 'compare-livedb'], help='Command to execute')
    
    # Source Arguments
    parser.add_argument('--source', required=True, help='Path to source schema file (or DB URL for compare-livedb)')
    
    # Target Arguments
    parser.add_argument('--target', required=True, help='Path to target schema file')
    
    parser.add_argument('--dialect', required=True, choices=['mysql', 'postgres', 'sqlite', 'oracle', 'db2', 'snowflake'], help='SQL Dialect')
    parser.add_argument('--object-types', help='Comma-separated list of object types to introspect (e.g. table,view). Default: table')
    
    # Independent flags
    parser.add_argument('--plan', action='store_true', help='Print detailed human-readable plan to stdout')
    parser.add_argument('--json-out', help='Path to save detailed JSON plan')
    parser.add_argument('--sql-out', help='Path to save migration SQL script')
    
    # Quality of Life flags
    parser.add_argument('--no-color', action='store_true', help='Disable colored output')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose output')
    # Version handling
    version = 'Unknown'
    try:
        import os
        # Determine path to VERSION file
        if getattr(sys, 'frozen', False):
            # Running as compiled executable
            base_path = sys._MEIPASS
        else:
            # Running as script
            base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            
        version_path = os.path.join(base_path, 'VERSION')
        if os.path.exists(version_path):
            with open(version_path, 'r') as f:
                version = f.read().strip()
    except Exception:
        pass

    parser.add_argument('--version', action='version', version=f'SchemaForge v{version}')
    
    args = parser.parse_args()
    
    if args.verbose:
        print(f"Debug: Command={args.command}, Dialect={args.dialect}, Source={args.source}, Target={args.target}", file=sys.stderr)
    
    if args.command == 'compare':
        try:
            source_sql = read_sql_source(args.source)
            target_sql = read_sql_source(args.target)
                
            parser_instance = get_parser(args.dialect)
            source_schema = parser_instance.parse(source_sql)
            target_schema = parser_instance.parse(target_sql)
            
            comparator = Comparator()
            migration_plan = comparator.compare(source_schema, target_schema)
            
            # ... (Output logic) ...
            _handle_output(args, migration_plan)

        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)

    elif args.command == 'compare-livedb':
        try:
            # Source is DB URL, Target is File
            from schemaforge.introspector import DBIntrospector
            
            print(f"Introspecting database at {args.source}...")
            introspector = DBIntrospector(args.source)
            
            obj_types = args.object_types.split(',') if args.object_types else None
            source_schema = introspector.introspect(object_types=obj_types)
            
            target_sql = read_sql_source(args.target)
            
            parser_instance = get_parser(args.dialect)
            target_schema = parser_instance.parse(target_sql)
            
            comparator = Comparator()
            migration_plan = comparator.compare(source_schema, target_schema)
            
            _handle_output(args, migration_plan)
            
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)

def _handle_output(args, migration_plan):
    # 1. Human Readable Plan
    if args.plan:
        # ANSI Color Codes
        if args.no_color:
            GREEN = ''
            RED = ''
            YELLOW = ''
            RESET = ''
        else:
            GREEN = '\033[92m'
            RED = '\033[91m'
            YELLOW = '\033[93m'
            RESET = '\033[0m'
        
        output_content = "Execution Plan:\n"
        for table in migration_plan.new_tables:
            output_content += f"{GREEN}  + Create Table: {table.name}{RESET}\n"
            for col in table.columns:
                output_content += f"{GREEN}    + Column: {col.name} ({col.data_type}){RESET}\n"
                
        for table in migration_plan.dropped_tables:
            output_content += f"{RED}  - Drop Table: {table.name}{RESET}\n"
            
        for diff in migration_plan.modified_tables:
            output_content += f"{YELLOW}  ~ Modify Table: {diff.table_name}{RESET}\n"
            for col in diff.added_columns:
                output_content += f"{GREEN}    + Add Column: {col.name} ({col.data_type}){RESET}\n"
            for col in diff.dropped_columns:
                output_content += f"{RED}    - Drop Column: {col.name}{RESET}\n"
            for old_col, new_col in diff.modified_columns:
                output_content += f"{YELLOW}    ~ Modify Column: {new_col.name} ({old_col.data_type} -> {new_col.data_type}){RESET}\n"
        
        if not (migration_plan.new_tables or migration_plan.dropped_tables or migration_plan.modified_tables):
             output_content += "No changes detected.\n"
             
        print(output_content)

    # 2. JSON Output
    if args.json_out:
        import json
        with open(args.json_out, 'w') as f:
            json.dump(migration_plan.to_dict(), f, indent=2)
        print(f"JSON plan saved to {args.json_out}")

    # 3. SQL Output
    if args.sql_out:
        get_generator = globals()['get_generator'] # Access global get_generator
        generator_instance = get_generator(args.dialect)
        migration_sql = generator_instance.generate_migration(migration_plan)
        full_sql = f"-- Migration Script for {args.dialect}\n{migration_sql}"
        with open(args.sql_out, 'w') as f:
            f.write(full_sql)
        print(f"Migration SQL saved to {args.sql_out}")
        
    if not (args.plan or args.json_out or args.sql_out):
        print("No output action specified. Use --plan, --json-out, or --sql-out.")

if __name__ == '__main__':
    main()
