import random
import string
import os
import json
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any

# --- Models for Generation ---
@dataclass
class GenColumn:
    name: str
    data_type: str
    is_nullable: bool = True
    is_pk: bool = False
    default: Optional[str] = None
    properties: Dict[str, Any] = field(default_factory=dict)

@dataclass
class GenTable:
    name: str
    columns: List[GenColumn] = field(default_factory=list)
    # Dialect specific properties
    properties: Dict[str, str] = field(default_factory=dict)

@dataclass
class Scenario:
    id: str
    dialect: str
    source_sql: str
    target_sql: str
    expected_changes: List[str]  # Description of expected changes

# --- Dialect Adapters ---
class DialectAdapter:
    def get_data_types(self) -> List[str]:
        return ['INT', 'VARCHAR(100)', 'BOOLEAN', 'DATE']

    def format_column(self, col: GenColumn) -> str:
        parts = [col.name, col.data_type]
        if not col.is_nullable:
            parts.append("NOT NULL")
        if col.default:
            parts.append(f"DEFAULT {col.default}")
        if col.is_pk:
            parts.append("PRIMARY KEY")
        return " ".join(parts)

    def format_table(self, table: GenTable) -> str:
        cols = ",\n  ".join([self.format_column(c) for c in table.columns])
        return f"CREATE TABLE {table.name} (\n  {cols}\n);"

class SnowflakeAdapter(DialectAdapter):
    def get_data_types(self) -> List[str]:
        return ['INT', 'VARCHAR(100)', 'BOOLEAN', 'DATE', 'VARIANT', 'TIMESTAMP', 'FLOAT']

    def format_table(self, table: GenTable) -> str:
        prefix = "CREATE "
        if table.properties.get('transient'):
            prefix += "TRANSIENT "
        
        cols = ",\n  ".join([self.format_column(c) for c in table.columns])
        stmt = f"{prefix}TABLE {table.name} (\n  {cols}\n)"
        
        if table.properties.get('cluster_by'):
            stmt += f"\nCLUSTER BY ({table.properties['cluster_by']})"
            
        if table.properties.get('retention'):
            stmt += f"\nDATA_RETENTION_TIME_IN_DAYS = {table.properties['retention']}"
            
        if table.properties.get('comment'):
            stmt += f"\nCOMMENT = '{table.properties['comment']}'"
            
        return stmt + ";"

class PostgresAdapter(DialectAdapter):
    def get_data_types(self) -> List[str]:
        return ['INT', 'VARCHAR(100)', 'BOOLEAN', 'DATE', 'TIMESTAMP', 'TEXT', 'JSONB', 'UUID', 'SERIAL']

    def format_table(self, table: GenTable) -> str:
        cols = ",\n  ".join([self.format_column(c) for c in table.columns])
        stmt = "CREATE "
        if table.properties.get('unlogged'):
            stmt += "UNLOGGED "
        stmt += f"TABLE {table.name} (\n  {cols}\n)"
        
        if table.properties.get('partition_by'):
            stmt += f" PARTITION BY {table.properties['partition_by']}"
            
        return stmt + ";"

class MySQLAdapter(DialectAdapter):
    def get_data_types(self) -> List[str]:
        return ['INT', 'VARCHAR(100)', 'BOOLEAN', 'DATE', 'DATETIME', 'TEXT', 'ENUM(\'a\',\'b\')', 'DECIMAL(10,2)']

    def format_table(self, table: GenTable) -> str:
        cols = ",\n  ".join([self.format_column(c) for c in table.columns])
        stmt = f"CREATE TABLE {table.name} (\n  {cols}\n)"
        
        if table.properties.get('partition_by'):
            stmt += f" PARTITION BY {table.properties['partition_by']}"
            
        return stmt + ";"

class SQLiteAdapter(DialectAdapter):
    def get_data_types(self) -> List[str]:
        return ['INTEGER', 'TEXT', 'BLOB', 'REAL', 'NUMERIC']

    def format_table(self, table: GenTable) -> str:
        cols = ",\n  ".join([self.format_column(c) for c in table.columns])
        stmt = f"CREATE TABLE {table.name} (\n  {cols}\n)"
        
        if table.properties.get('without_rowid'):
            stmt += " WITHOUT ROWID"
        if table.properties.get('strict'):
            stmt += " STRICT"
            
        return stmt + ";"

class OracleAdapter(DialectAdapter):
    def get_data_types(self) -> List[str]:
        return ['NUMBER', 'VARCHAR2(100)', 'DATE', 'TIMESTAMP', 'CLOB', 'BLOB']

    def format_table(self, table: GenTable) -> str:
        cols = ",\n  ".join([self.format_column(c) for c in table.columns])
        stmt = f"CREATE TABLE {table.name} (\n  {cols}\n)"
        
        if table.properties.get('tablespace'):
            stmt += f" TABLESPACE {table.properties['tablespace']}"
            
        if table.properties.get('partition_by'):
            stmt += f" PARTITION BY {table.properties['partition_by']}"
            
        return stmt + ";"

class DB2Adapter(DialectAdapter):
    def get_data_types(self) -> List[str]:
        return ['INTEGER', 'VARCHAR(100)', 'DATE', 'TIMESTAMP', 'CLOB', 'BLOB', 'DECIMAL(10,2)', 'XML']

    def format_column(self, col: GenColumn) -> str:
        parts = [col.name, col.data_type]
        if not col.is_nullable:
            parts.append("NOT NULL")
        if col.default:
            parts.append(f"DEFAULT {col.default}")
        # Identity
        if col.properties.get('is_identity'):
            parts.append("GENERATED ALWAYS AS IDENTITY")
        if col.is_pk:
            parts.append("PRIMARY KEY")
        return " ".join(parts)

    def format_table(self, table: GenTable) -> str:
        cols = ",\n  ".join([self.format_column(c) for c in table.columns])
        stmt = f"CREATE TABLE {table.name} (\n  {cols}\n)"
        
        if table.properties.get('tablespace'):
            stmt += f" IN {table.properties['tablespace']}"
            
        if table.properties.get('partition_by'):
            stmt += f" PARTITION BY {table.properties['partition_by']}"
            
        return stmt + ";"

# --- Generator Engine ---
class ScenarioGenerator:
    def __init__(self, dialect: str, output_dir: str):
        self.dialect = dialect
        self.output_dir = output_dir
        if dialect == 'snowflake':
            self.adapter = SnowflakeAdapter()
        elif dialect == 'postgres':
            self.adapter = PostgresAdapter()
        elif dialect == 'mysql':
            self.adapter = MySQLAdapter()
        elif dialect == 'sqlite':
            self.adapter = SQLiteAdapter()
        elif dialect == 'oracle':
            self.adapter = OracleAdapter()
        elif dialect == 'db2':
            self.adapter = DB2Adapter()
        else:
            self.adapter = DialectAdapter()
        self.tables = []

    def _random_name(self, prefix=''):
        return f"{prefix}{''.join(random.choices(string.ascii_lowercase, k=6))}"

    def _random_type(self):
        return random.choice(self.adapter.get_data_types())

    def generate_base_schema(self, num_tables=5):
        self.tables = []
        for _ in range(num_tables):
            t_name = self._random_name('tbl_')
            cols = []
            # Ensure at least one PK
            cols.append(GenColumn('id', 'INT', is_nullable=False, is_pk=True))
            
            for _ in range(random.randint(2, 8)):
                cols.append(GenColumn(
                    self._random_name('col_'),
                    self._random_type(),
                    is_nullable=random.choice([True, False])
                ))
            
            props = {}
            if self.dialect == 'snowflake':
                if random.random() < 0.3: props['transient'] = True
                if random.random() < 0.3: props['retention'] = random.randint(0, 90)
                if random.random() < 0.3: props['comment'] = "Generated table"
            elif self.dialect == 'db2':
                if random.random() < 0.3: props['tablespace'] = "USERSPACE1"
                if random.random() < 0.2: props['partition_by'] = "RANGE (id) (STARTING 1 ENDING 1000)"
            elif self.dialect == 'oracle':
                if random.random() < 0.3: props['tablespace'] = "USERS"
                if random.random() < 0.2: props['partition_by'] = "RANGE (id) (PARTITION p1 VALUES LESS THAN (100))"
            elif self.dialect == 'postgres':
                if random.random() < 0.2: props['unlogged'] = True
                if random.random() < 0.2: props['partition_by'] = "RANGE (id)"
            elif self.dialect == 'mysql':
                if random.random() < 0.2: props['partition_by'] = "RANGE (id) (PARTITION p0 VALUES LESS THAN (10))"
            elif self.dialect == 'sqlite':
                if random.random() < 0.2: props['strict'] = True
                if random.random() < 0.2: props['without_rowid'] = True
                
            self.tables.append(GenTable(t_name, cols, props))
            
            # DB2 Identity
            if self.dialect == 'db2':
                for col in cols:
                    if col.data_type == 'INTEGER' and random.random() < 0.2:
                        col.properties['is_identity'] = True

    def mutate(self) -> (List[GenTable], List[str]):
        """
        Applies mutations to the current schema to create a target schema.
        Returns (New Schema Tables, List of Expected Changes)
        """
        import copy
        target_tables = copy.deepcopy(self.tables)
        expectations = []
        
        # 1. Add Table
        if random.random() < 0.3:
            new_table = GenTable(self._random_name('new_tbl_'), [
                GenColumn('id', 'INT', is_pk=True),
                GenColumn('data', 'VARCHAR(50)')
            ])
            target_tables.append(new_table)
            expectations.append(f"Create Table: {new_table.name}")

        # 2. Drop Table
        if len(target_tables) > 1 and random.random() < 0.2:
            idx = random.randint(0, len(target_tables)-1)
            dropped = target_tables.pop(idx)
            expectations.append(f"Drop Table: {dropped.name}")

        # 3. Modify Tables
        for table in target_tables:
            # Add Column
            if random.random() < 0.2:
                new_col = GenColumn(self._random_name('new_col_'), self._random_type())
                table.columns.append(new_col)
                expectations.append(f"Table {table.name}: Add Column {new_col.name}")
            
            # Drop Column (ensure not dropping PK for simplicity)
            non_pk_cols = [c for c in table.columns if not c.is_pk]
            if non_pk_cols and random.random() < 0.2:
                col_to_drop = random.choice(non_pk_cols)
                table.columns.remove(col_to_drop)
                expectations.append(f"Table {table.name}: Drop Column {col_to_drop.name}")
                
            # Modify Column Type
            non_pk_cols = [c for c in table.columns if not c.is_pk]
            if non_pk_cols and random.random() < 0.2:
                col = random.choice(non_pk_cols)
                old_type = col.data_type
                new_type = self._random_type()
                if old_type != new_type:
                    col.data_type = new_type
                    expectations.append(f"Table {table.name}: Modify Column {col.name} ({old_type} -> {new_type})")
                    # DEBUG
                    # print(f"DEBUG: Mutated {table.name}.{col.name} from {old_type} to {new_type}")

        return target_tables, expectations

    def generate_scenario(self, run_id: str) -> Scenario:
        self.generate_base_schema(random.randint(3, 10))
        
        # Source SQL
        source_sql = "\n".join([self.adapter.format_table(t) for t in self.tables])
        
        # Mutate
        target_tables, expectations = self.mutate()
        
        # Target SQL
        target_sql = "\n".join([self.adapter.format_table(t) for t in target_tables])
        
        if not expectations:
            expectations.append("No changes")

        return Scenario(run_id, self.dialect, source_sql, target_sql, expectations)

    def save_scenario(self, scenario: Scenario):
        base_path = os.path.join(self.output_dir, scenario.id)
        os.makedirs(base_path, exist_ok=True)
        
        with open(os.path.join(base_path, "source.sql"), 'w') as f:
            f.write(scenario.source_sql)
            
        with open(os.path.join(base_path, "target.sql"), 'w') as f:
            f.write(scenario.target_sql)
            
        with open(os.path.join(base_path, "expected.json"), 'w') as f:
            json.dump(scenario.expected_changes, f, indent=2)

if __name__ == "__main__":
    # Test run
    gen = ScenarioGenerator('snowflake', 'tests/blackbox/scenarios/snowflake')
    s = gen.generate_scenario("test_001")
    gen.save_scenario(s)
    print(f"Generated scenario {s.id}")
