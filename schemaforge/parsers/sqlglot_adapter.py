from typing import Optional, List, Dict
import sqlglot
from sqlglot import exp
from schemaforge.models import Table, Column, Index, ForeignKey, CheckConstraint, CustomObject
from schemaforge.parsers.base import BaseParser
from schemaforge.logging_config import get_logger
from schemaforge.models import Schema

class SqlglotParser(BaseParser):
    """
    Parser using `sqlglot` library.
    v2.0 Parser implementation replacing GenericSQLParser.
    """
    
    def __init__(self, dialect: str = None, strict: bool = False):
        super().__init__(strict)
        self.dialect = dialect
        self.logger = get_logger("parser")
    
    def _preprocess(self, content: str) -> str:
        # sqlglot fails on Postgres EXCLUDE USING syntax, falling back to Command.
        # We strip it to allow parsing of the table.
        # Regex to match EXCLUDE USING ... (...)
        import re
        if "EXCLUDE USING" in content.upper():
             content = re.sub(r',\s*EXCLUDE\s+USING\s+\w+\s*\([^)]+\)', '', content, flags=re.IGNORECASE)
             content = re.sub(r'EXCLUDE\s+USING\s+\w+\s*\([^)]+\)', '', content, flags=re.IGNORECASE)
        
        if "WITHOUT ROWID" in content.upper():
             content = re.sub(r'\s+WITHOUT\s+ROWID', '', content, flags=re.IGNORECASE)

        # MSSQL: CLUSTERED / NONCLUSTERED causes issues in inline PK/Index
        if self.dialect == 'tsql':
             content = re.sub(r'\b(?:NON)?CLUSTERED\b', '', content, flags=re.IGNORECASE)

             
        # SQLite STRICT handling - sqlglot might fail if not in read='sqlite' 
        # but even then some versions might bug. We'll capture it in model anyway.
        if "STRICT" in content.upper() and self.dialect == 'sqlite':
             # Only strip if it's at the very end of CREATE TABLE (most common case)
             content = re.sub(r'\)\s*STRICT\s*;', ');', content, flags=re.IGNORECASE)
             
        # Normalize Postgres COMMENT ON CONSTRAINT x ON y IS z -> COMMENT ON CONSTRAINT x IS z
        if "COMMENT ON CONSTRAINT" in content.upper():
             content = re.sub(r'(COMMENT\s+ON\s+CONSTRAINT\s+[^\s]+)\s+ON\s+[^\s]+(\s+IS)', r'\1\2', content, flags=re.IGNORECASE)

        return content

    def _clean_type(self, data_type: str) -> str:
        """
        Clean and normalize data type strings.
        Can be overridden by dialect subclasses.
        """
        return data_type

    def parse(self, content: str) -> Schema:
        from schemaforge.exceptions import StrictModeError
        import re
        
        self.raw_content = content
        schema = Schema()

        
        # Pre-parse detection for certain keywords that might cause fallbacks or are easier to catch here
        without_rowid_tables = []
        strict_tables = []
        
        # Simple regex to find tables with WITHOUT ROWID or STRICT
        # These are usually at the end of the CREATE TABLE statement: ) WITHOUT ROWID;
        wr_matches = re.findall(r'CREATE\s+TABLE\s+([^\s(]+).*?\)\s*WITHOUT\s+ROWID', content, re.IGNORECASE | re.DOTALL)
        for m in wr_matches:
             without_rowid_tables.append(m.replace('"', '').replace('`', '').strip())
             
        strict_matches = re.findall(r'CREATE\s+TABLE\s+([^\s(]+).*?\)\s*STRICT', content, re.IGNORECASE | re.DOTALL)
        for m in strict_matches:
             strict_tables.append(m.replace('"', '').replace('`', '').strip())
        
        content = self._preprocess(content)
        
        # Parse all content
        try:
            if self.strict:
                 error_lvl = None 
            else:
                 error_lvl = sqlglot.ErrorLevel.IGNORE 
            
            expressions = sqlglot.parse(content, read=self.dialect, error_level=error_lvl)
        except Exception as e:
            if self.strict:
                raise StrictModeError(content, str(e))
            self.logger.error(f"Failed to parse SQL content: {e}")
            return schema
            
        if not expressions and content.strip():
             if self.strict:
                  raise StrictModeError(content, "Failed to parse content (empty result)")
        
        for expression in expressions:
            if expression is None: continue 
            
            # Strict mode: Reject fallback Commands AND unmatched expressions
            if self.strict:
                 valid_types = (exp.Create, exp.Alter, exp.Comment, exp.Drop, exp.Command)
                 if not isinstance(expression, valid_types):
                      raise StrictModeError(content, f"Unsupported statement type in strict mode: {type(expression)} - {expression.sql()}")
                 if isinstance(expression, exp.Command):
                      # Check if allowed command
                      sql_upper = expression.sql().upper()
                      if not ("ALTER SCHEMA" in sql_upper or "ALTER TYPE" in sql_upper or "ENABLE ROW LEVEL SECURITY" in sql_upper):
                           raise StrictModeError(content, f"Statement parsed as Command (unsupported syntax): {expression.sql()}")

            if isinstance(expression, exp.Create):
                if expression.kind == "TABLE":
                    table = self._extract_create_table(expression)
                    if table:
                        clean_name = table.name.replace('"', '').replace('`', '').strip()
                        if clean_name in without_rowid_tables:
                             table.without_rowid = True
                        if clean_name in strict_tables:
                             table.is_strict = True
                        schema.add_table(table)
                elif expression.kind == "INDEX" or expression.kind == "UNIQUE_INDEX" or isinstance(expression.this, exp.Index):
                    is_unique = expression.args.get("unique") or expression.kind == "UNIQUE_INDEX"
                    self._extract_create_index(expression, schema, is_unique=is_unique)
                elif expression.kind in ("VIEW", "FUNCTION", "PROCEDURE", "SEQUENCE", "ALIAS", "TYPE", "DOMAIN", "PACKAGE"):
                     # Support these as CustomObjects
                     name = "unknown"
                     node = expression.this
                     
                     if isinstance(node, exp.UserDefinedFunction):
                          # node.this is the function identifier/table
                          name = node.this.name if hasattr(node.this, 'name') else str(node.this)
                     elif hasattr(node, 'name') and node.name:
                          name = node.name
                     elif hasattr(node, 'this') and hasattr(node.this, 'name'):
                          name = node.this.name
                     elif isinstance(node, str):
                          name = node
                     
                     # Normalize name to lower
                     name = name.lower()
                     
                     obj = CustomObject(obj_type=expression.kind, name=name, properties={'raw_sql': expression.sql(comments=False)})
                     
                     if expression.kind == "TYPE":
                          schema.types.append(obj)
                     elif expression.kind == "DOMAIN":
                          schema.domains.append(obj)
                     else:
                          schema.custom_objects.append(obj)

            elif isinstance(expression, exp.Alter):
                self._process_alter_table(expression, schema)
            elif isinstance(expression, exp.Command):
                 # Handle generic commands or fallbacks
                 sql_upper = expression.sql().upper()
                 raw_sql = expression.sql(comments=False)
                 
                 # Strip comments from Command raw sql manually if sqlglot didn't
                 import re
                 raw_sql = re.sub(r'/\*.*?\*/', '', raw_sql, flags=re.DOTALL)
                 raw_sql = re.sub(r'--.*$', '', raw_sql, flags=re.MULTILINE)
                 raw_sql = " ".join(raw_sql.split()).upper() # Normalize whitespace and case for consistency

                 
                 # RLS Handling
                 if "ENABLE ROW LEVEL SECURITY" in sql_upper and "ALTER TABLE" in sql_upper:
                      match = re.search(r'ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?([^\s]+)\s+ENABLE\s+ROW\s+LEVEL\s+SECURITY', expression.sql(), re.IGNORECASE)
                      if match:
                           tname = match.group(1).replace('"', '').replace('`', '')
                           if '.' in tname: tname = tname.split('.')[-1] 
                           t = schema.get_table(tname)
                           if t: t.row_security = True
                 
                 name = "command"
                 obj_type = "COMMAND"
                 # Regex extraction for Create View/Function fallbacks
                 if "CREATE TABLE" in raw_sql:
                      m = re.search(r'CREATE\s+TABLE\s+([^\s(]+)', raw_sql, re.IGNORECASE)
                      if m:
                           name = m.group(1).replace('"', '').replace('`', '').replace("[", "").replace("]", "").replace("'", "").lower()
                           if '.' in name: name = name.split('.')[-1]
                           table = Table(name=name)
                           
                           # FALLBACK COLUMN PARSING
                           start_idx = raw_sql.find('(')
                           if start_idx != -1:
                               body = raw_sql[start_idx+1:].strip()
                               depth = 1
                               end_idx = -1
                               for i, char in enumerate(body):
                                   if char == '(': depth += 1
                                   elif char == ')': depth -= 1
                                   if depth == 0:
                                       end_idx = i
                                       break
                               
                               if end_idx != -1:
                                   body = body[:end_idx]
                                   defs = []
                                   current_def = []
                                   depth = 0
                                   for char in body:
                                       if char == ',' and depth == 0:
                                           defs.append("".join(current_def).strip())
                                           current_def = []
                                       else:
                                           if char == '(': depth += 1
                                           elif char == ')': depth -= 1
                                           current_def.append(char)
                                   if current_def:
                                       defs.append("".join(current_def).strip())
                                   
                                   for d in defs:
                                       d = d.strip()
                                       if not d: continue
                                       upper_d = d.upper()
                                       # Skip constraints if possible
                                       if any(upper_d.startswith(k) for k in ["CONSTRAINT", "PRIMARY KEY", "FOREIGN KEY", "CHECK", "INDEX", "KEY"]):
                                           continue
                                           
                                       parts = d.split(maxsplit=1)
                                       if len(parts) >= 2:
                                           cname = parts[0].replace('"', '').replace('`', '').replace("[", "").replace("]", "").replace("'", "").lower()
                                           ctype = parts[1]
                                           # Clean up common trailing constraints if possible, or just keep them.
                                           # For fallback, providing the full definition as type is acceptable visual info.
                                           table.columns.append(Column(name=cname, data_type=ctype))

                           schema.tables.append(table)
                           self._post_process_table(table, expression)
                           continue
                 elif "CREATE VIEW" in raw_sql:
                      m = re.search(r'CREATE\s+VIEW\s+([^\s(]+)', raw_sql, re.IGNORECASE)
                      if m: 
                           name = m.group(1).replace('"', '').replace('`', '').replace("'", "").lower()
                           obj_type = "VIEW"
                 elif "CREATE TYPE" in raw_sql:
                      m = re.search(r'CREATE\s+TYPE\s+([^\s(]+)', raw_sql, re.IGNORECASE)
                      if m: 
                           name = m.group(1).replace('"', '').replace('`', '').replace("'", "").lower()
                           obj_type = "TYPE"
                           schema.types.append(CustomObject(obj_type=obj_type, name=name, properties={'raw_sql': raw_sql}))
                           continue
                 elif "CREATE DOMAIN" in raw_sql:
                      m = re.search(r'CREATE\s+DOMAIN\s+([^\s(]+)', raw_sql, re.IGNORECASE)
                      if m: 
                           name = m.group(1).replace('"', '').replace('`', '').replace("'", "").lower()
                           obj_type = "DOMAIN"
                           schema.domains.append(CustomObject(obj_type=obj_type, name=name, properties={'raw_sql': raw_sql}))
                           continue
                 elif "CREATE FUNCTION" in raw_sql:
                      m = re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+([^\s(]+)', raw_sql, re.IGNORECASE)
                      if m: 
                           name = m.group(1).replace('"', '').replace('`', '').replace("'", "").lower()
                           obj_type = "FUNCTION"                  
                 elif "CREATE PROCEDURE" in raw_sql:
                      m = re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+([^\s(]+)', raw_sql, re.IGNORECASE)
                      if m: 
                           name = m.group(1).replace('"', '').replace('`', '').replace("'", "").lower()
                           obj_type = "PROCEDURE"
                 elif "CREATE PACKAGE" in raw_sql:
                      m = re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?PACKAGE\s+(?:BODY\s+)?([^\s(]+)', raw_sql, re.IGNORECASE)
                      if m: 
                           name = m.group(1).replace('"', '').replace('`', '').replace("'", "").lower()
                           obj_type = "PACKAGE"
                 elif "CREATE" in raw_sql and "INDEX" in raw_sql:
                      # Fallback for INDEX if parsed as Command (e.g. DB2 with INCLUDE or MSSQL CLUSTERED)
                      # Normalize whitespace for matching
                      norm_sql = " ".join(raw_sql.split())
                      m = re.search(r'CREATE\s+(?:UNIQUE\s+)?(CLUSTERED\s+|NONCLUSTERED\s+)?INDEX\s+([^\s]+)\s+ON\s+([^\s(]+)', norm_sql, re.IGNORECASE)
                      if m:
                           is_clustered = m.group(1) and 'CLUSTERED' in m.group(1).upper() and 'NONCLUSTERED' not in m.group(1).upper()
                           idx_name = m.group(2).replace('"', '').replace('`', '').replace("[", "").replace("]", "").replace("'", "").lower()
                           table_name = m.group(3).replace('"', '').replace('`', '').replace("[", "").replace("]", "").replace("'", "").lower()
                           if '.' in table_name: table_name = table_name.split('.')[-1]
                           table = schema.get_table(table_name)
                           if table:
                                # We need columns
                                m_cols = re.search(r'\((.*?)\)', norm_sql)

                                cols = [c.strip().replace("[", "").replace("]", "").lower() for c in m_cols.group(1).split(',')] if m_cols else []
                                is_unique = "UNIQUE" in raw_sql.upper()
                                
                                # Extract INCLUDE columns if present
                                include_cols_lower = []
                                include_cols_raw = []
                                m_inc = re.search(r'INCLUDE\s*\((.*?)\)', norm_sql, re.IGNORECASE)
                                if m_inc:
                                     include_cols_raw = [c.strip() for c in m_inc.group(1).split(',')]
                                     include_cols_lower = [c.lower() for c in include_cols_raw]
                                
                                table.indexes.append(Index(
                                     name=idx_name, 
                                     columns=cols, 
                                     is_unique=is_unique,
                                     is_clustered=is_clustered,
                                     include_columns=include_cols_lower,
                                     properties={'include_columns': include_cols_raw} if include_cols_raw else {}
                                ))

                 elif "CREATE ALIAS" in raw_sql:
                      m = re.search(r'CREATE\s+ALIAS\s+([^\s(]+)', raw_sql, re.IGNORECASE)
                      if m: 
                           name = m.group(1).replace('"', '').replace('`', '').replace("[", "").replace("]", "").replace("'", "").lower()
                           obj_type = "ALIAS"
                 elif "CREATE SEQUENCE" in raw_sql:
                      m = re.search(r'CREATE\s+SEQUENCE\s+([^\s(]+)', raw_sql, re.IGNORECASE)
                      if m: 
                           name = m.group(1).replace('"', '').replace('`', '').replace("'", "").lower()
                           obj_type = "SEQUENCE"
                 elif "ALTER SCHEMA" in raw_sql:
                      name = "public" 
                      m = re.search(r'ALTER\s+SCHEMA\s+([^\s]+)', raw_sql, re.IGNORECASE)
                      if m: name = m.group(1).lower()
                      obj_type = "ALTER SCHEMA"
                 elif "ALTER TYPE" in raw_sql:
                      name = "status" 
                      m = re.search(r'ALTER\s+TYPE\s+([^\s]+)', expression.sql(), re.IGNORECASE)
                      if m: name = m.group(1).replace("'", "").lower()
                      obj_type = "ALTER TYPE"
                 elif expression.this:
                      if isinstance(expression.this, str):
                           name = expression.this.split()[0]
                      elif hasattr(expression.this, 'name'):
                           name = expression.this.name
                 
                 schema.custom_objects.append(CustomObject(obj_type=obj_type, name=name, properties={'raw_sql': raw_sql}))
            elif isinstance(expression, exp.Comment):
                self._process_comment(expression, schema)

        return schema

    def _extract_create_table(self, expression: exp.Create) -> Optional[Table]:
        # Extract table name
        table_node = expression.this
        
        # Handle Schema object wrapper
        if isinstance(table_node, exp.Schema):
            real_table_name = table_node.this.name
            # Handle schema qualification
            if isinstance(table_node.this, exp.Table) and table_node.this.db:
                 real_table_name = f"{table_node.this.db}.{real_table_name}"
            column_defs = table_node.expressions
        else:
            real_table_name = table_node.name
            column_defs = expression.this.expressions
 
        table = Table(name=real_table_name.lower())

        
        self._process_create_table_properties(expression, table)
        
        for col_def in column_defs:
            if isinstance(col_def, exp.ColumnDef):
                self._process_column_def(col_def, table)
            elif isinstance(col_def, exp.Constraint):
                self._process_table_constraint(col_def, table)

            elif isinstance(col_def, exp.ForeignKey):
                self._process_foreign_key(col_def, table)
            elif isinstance(col_def, exp.PrimaryKey):
                 self._process_constraint_kind(col_def, table, f"pk_{table.name}")
            elif isinstance(col_def, exp.UniqueColumnConstraint):
                 self._process_constraint_kind(col_def, table, f"uk_{table.name}")
            elif isinstance(col_def, exp.CheckColumnConstraint):
                 self._process_constraint_kind(col_def, table, f"ck_{table.name}")
            elif isinstance(col_def, exp.Check):
                 self._process_constraint_kind(col_def, table, f"ck_{table.name}")

        return table

    def _process_create_table_properties(self, expression: exp.Create, table: Table):
        props = expression.args.get("properties")
        if props:
             for prop in props.expressions:
                  self._process_property(prop, table)

    def _process_property(self, prop: exp.Expression, table: Table):
        """Override in subclasses to handle dialect specific properties"""
        pass

    def _post_process_table(self, table: Table, expression: exp.Expression):
        """Hook for subclasses to process tables created via Command fallback."""
        pass

    def _process_column_def(self, col_def: exp.ColumnDef, table: Table):
        col_name = col_def.this.name.lower()
        
        col_type = "UNKNOWN"
        if col_def.kind:
            col_type = col_def.kind.sql(dialect=self.dialect)
        
        column = Column(name=col_name, data_type=self._clean_type(col_type))
        
        if col_def.constraints:
            for constraint in col_def.constraints:
                kind = constraint.kind
                if isinstance(kind, exp.PrimaryKeyColumnConstraint):
                    column.is_primary_key = True
                    column.is_nullable = False
                elif isinstance(kind, exp.NotNullColumnConstraint):
                    if kind.args.get("allow_null"):
                         column.is_nullable = True
                    else:
                         column.is_nullable = False
                elif isinstance(kind, exp.UniqueColumnConstraint):
                    idx_name = f"uk_{table.name}_{col_name}"
                    table.indexes.append(Index(name=idx_name, columns=[col_name], is_unique=True))
                elif isinstance(kind, exp.DefaultColumnConstraint):
                    column.default_value = kind.this.sql(dialect=self.dialect)
                elif isinstance(kind, exp.CheckColumnConstraint):
                    check_name = f"ck_{table.name}_{col_name}_{len(table.check_constraints)}"
                    try:
                        expr = kind.this.sql(dialect=self.dialect)
                    except: 
                        expr = str(kind)
                    table.check_constraints.append(CheckConstraint(name=check_name, expression=expr))
                elif isinstance(kind, exp.GeneratedAsIdentityColumnConstraint):
                     column.is_identity = True
                elif isinstance(kind, exp.GeneratedAsRowColumnConstraint):
                     column.is_generated = True
                     column.generation_expression = kind.this.sql(dialect=self.dialect)
        
        table.columns.append(column)
    
    def _process_table_constraint(self, constraint: exp.Constraint, table: Table):
        # constraint.this is the alias/name of the constraint
        name = constraint.this.name if constraint.this else None
        
        # constraint.expressions contains the actual constraint definitions (PK, FK, Check, Unique)
        # Some dialects/sqlglot versions put kind in args, others in expressions
        kind = constraint.args.get("kind")
        if kind:
             self._process_constraint_kind(kind, table, name)
        
        for expr in constraint.expressions:
             if isinstance(expr, (exp.PrimaryKey, exp.ForeignKey, exp.UniqueColumnConstraint, exp.Check, exp.CheckColumnConstraint, exp.PrimaryKeyColumnConstraint, exp.ClusteredColumnConstraint)):
                  self._process_constraint_kind(expr, table, name)
             elif isinstance(expr, exp.Schema):
                  # Sometimes Unique/PK is wrapped in Schema
                  for nested in expr.expressions:
                       if isinstance(nested, (exp.PrimaryKey, exp.ForeignKey, exp.UniqueColumnConstraint, exp.Check, exp.PrimaryKeyColumnConstraint, exp.ClusteredColumnConstraint)):
                            self._process_constraint_kind(nested, table, name)





    def _process_constraint_kind(self, kind, table, name: str = None):
        if isinstance(kind, (exp.PrimaryKey, exp.PrimaryKeyColumnConstraint, exp.ClusteredColumnConstraint, exp.NonClusteredColumnConstraint)):
             cols = []
             # If it's a PrimaryKeyColumnConstraint without columns, it might be inline on a ColumnDef
             # But here we are at table level. Table level PK usually has expressions.
             if hasattr(kind, 'expressions') and kind.expressions:
                  for c in kind.expressions:
                       # PrimaryKey expressions might be Ordered expressions
                       inner = c.this if isinstance(c, exp.Ordered) else c
                       if isinstance(inner, exp.Column):
                            cols.append(inner.name)
                       elif isinstance(inner, exp.Identifier):
                            cols.append(inner.this)
                       else:
                            cols.append(str(inner))
             
             # Fallback for Clustered/NonClustered which often have columns in .this
             if not cols and hasattr(kind, 'this') and kind.this:
                  target = kind.this
                  if isinstance(target, list):
                       for expr in target:
                            inner = expr.this if isinstance(expr, exp.Ordered) else expr
                            cols.append(inner.name if hasattr(inner, 'name') else str(inner))
                  elif isinstance(target, (exp.Tuple, exp.Schema)):
                       for expr in target.expressions:
                            # Handle Ordered or just Identifier/Column
                            inner = expr.this if isinstance(expr, exp.Ordered) else expr
                            cols.append(inner.name if hasattr(inner, 'name') else str(inner))
                  else:
                       cols.append(target.name if hasattr(target, 'name') else str(target))

             
             for col_name in cols:
                  # Strip quotes for model matching
                  clean_name = col_name.replace('"', '').replace('`', '').replace('[', '').replace(']', '').strip()
                  col = table.get_column(clean_name)
                  if not col:
                       # Try case-insensitive
                       for tc in table.columns:
                            if tc.name.upper() == clean_name.upper():
                                 col = tc
                                 break
                  
                  if col:
                      col.is_primary_key = True
                      col.is_nullable = False



        elif isinstance(kind, exp.ForeignKey):
             self._process_foreign_key(kind, table, name)
        elif isinstance(kind, exp.UniqueColumnConstraint):
             # Table level unique might be wrapped in Schema or just direct expressions
             if isinstance(kind.this, exp.Schema):
                  cols = []
                  for c in kind.this.expressions:
                       if isinstance(c, exp.Identifier):
                            cols.append(c.this)
                       elif hasattr(c, 'name'):
                            cols.append(c.name)
                       else:
                            cols.append(str(c))
             else:
                  cols = [kind.this.name] if kind.this else []
             
             table.indexes.append(Index(name=name, columns=cols, is_unique=True))
        elif isinstance(kind, exp.CheckColumnConstraint): 
             expr = kind.this.sql(dialect=self.dialect) 
             table.check_constraints.append(CheckConstraint(name=name, expression=expr))
        elif isinstance(kind, exp.Check):
             expr = kind.this.sql(dialect=self.dialect)
             table.check_constraints.append(CheckConstraint(name=name, expression=expr))

    def _process_foreign_key(self, fk_node: exp.ForeignKey, table: Table, name: str = None):
        # Extract columns
        cols = [c.name for c in fk_node.expressions]
        
        ref = fk_node.args.get("reference")
        if not ref:
            return

        ref_obj = ref.this
        if isinstance(ref_obj, exp.Schema):
             ref_table_node = ref_obj.this
             ref_table = ref_table_node.name
             if ref_table_node.db:
                   ref_table = f"{ref_table_node.db}.{ref_table}"
        else:
             ref_table_node = ref_obj
             ref_table = ref_table_node.name
             if hasattr(ref_table_node, 'db') and ref_table_node.db:
                  ref_table = f"{ref_table_node.db}.{ref_table}"
            
        ref_cols = [c.name for c in ref.expressions]
        
        # Validation checks
        if not name:
             name = f"fk_{table.name}_{len(table.foreign_keys)}"

        on_delete = None
        on_update = None
        
        # reference.options contains explicit ON DELETE/UPDATE strings usually
        options = ref.args.get("options")
        if options:
             for opt in options:
                  # opt is likely a string or Identifier-like
                  opt_str = str(opt).upper()
                  if "ON DELETE" in opt_str:
                       on_delete = opt_str.replace("ON DELETE", "").strip()
                  elif "ON UPDATE" in opt_str:
                       on_update = opt_str.replace("ON UPDATE", "").strip()
        
        # Check Deferrable
        is_deferrable = False
        if fk_node.args.get("deferrable"):
             is_deferrable = True
        elif ref and ref.args.get("deferrable"):
             is_deferrable = True
             
        # Manual check in options
        if ref and ref.args.get("options"):
             for opt in ref.args.get("options"):
                  val = opt.sql().upper() if hasattr(opt, 'sql') else str(opt).upper()
                  if val == "DEFERRABLE":
                        is_deferrable = True
                  elif hasattr(opt, 'this') and str(opt.this).upper() == "DEFERRABLE":
                        is_deferrable = True

        table.foreign_keys.append(ForeignKey(
            name=name,
            column_names=cols,
            ref_table=ref_table,
            ref_column_names=ref_cols,
            on_delete=on_delete,
            on_update=on_update,
            is_deferrable=is_deferrable
        ))

    def _extract_create_index(self, expression, schema, is_unique=False):
        # ... (same as before)
        index_node = expression.this
        if not isinstance(index_node, exp.Index):
            return
            
        table_node = index_node.args.get("table")
        if not table_node:
             return

        table_name = table_node.this.name if hasattr(table_node, 'this') else table_node.name
        # Handle schema qualification
        if isinstance(table_node, exp.Table) and table_node.db:
              table_name = f"{table_node.db}.{table_name}"

        table = schema.get_table(table_name) # Schema.get_table handles name lookup
        if not table:
             return 

        idx_name = index_node.name.lower()

        
        cols = []
        for c in index_node.expressions:
             if isinstance(c, exp.Column):
                  cols.append(c.name)
             elif isinstance(c, exp.Identifier):
                  cols.append(c.this)
             elif hasattr(c, 'name') and c.name:
                  cols.append(c.name)
             else:
                  cols.append(c.sql(dialect=self.dialect))
        
        # Extra properties in params
        params = index_node.args.get("params")
        method = None
        where_clause = None
        include_columns = []
        
        if params:
             # If no columns found yet, or functional index pushed to params (IndexParameters)
             if not cols:
                  # Check params args for 'columns' explicitly
                  p_cols = params.args.get("columns")
                  if p_cols:
                       for pc in p_cols:
                            # Handle Ordered wrapper only
                            col_expr = pc.this if isinstance(pc, exp.Ordered) else pc
                            
                            if isinstance(col_expr, exp.Column):
                                 cols.append(col_expr.name)
                            elif isinstance(col_expr, exp.Identifier):
                                 cols.append(col_expr.this)
                            elif hasattr(col_expr, 'name') and col_expr.name:
                                 cols.append(col_expr.name)
                            else:
                                 # Functional fallback
                                 val = col_expr.sql()
                                 if not val: val = col_expr.sql(dialect=self.dialect)
                                 if not val: val = str(col_expr)
                                 cols.append(val)
                  
                  # Check params expressions
                  if hasattr(params, 'expressions') and params.expressions:
                       for c in params.expressions:
                            # Handling Ordered wrapper
                            expr = c.this if isinstance(c, exp.Ordered) else c
                            val = expr.sql()
                            if not val: val = expr.sql(dialect=self.dialect)
                            if not val: val = str(expr)
                            cols.append(val)

             # method is in 'using'
             using = params.args.get("using")
             if using:
                  if hasattr(using, 'this'):
                       method = using.this.name if hasattr(using.this, 'name') else str(using.this)
                  else:
                       method = str(using)
             
             where_node = params.args.get("where")
             if where_node:
                  where_clause = where_node.sql(dialect=self.dialect)
            
             include_node = params.args.get("include")
             if include_node:
                  include_columns = []
                  for c in include_node:
                       if isinstance(c, exp.Identifier):
                            include_columns.append(c.this)
                       elif hasattr(c, 'name'):
                            include_columns.append(c.name)
                       else:
                            include_columns.append(str(c))
             
             # Fallback for columns if expressions were empty
             if not cols:
                  param_cols = params.args.get("columns")
                  if param_cols:
                       for pc in param_cols:
                            col_expr = pc.this if hasattr(pc, 'this') else pc
                            if isinstance(col_expr, exp.Column):
                                 cols.append(col_expr.name)
                            elif isinstance(col_expr, exp.Identifier):
                                 cols.append(col_expr.this)
                            elif hasattr(col_expr, 'name'):
                                 cols.append(col_expr.name)
                            else:
                                 cols.append(col_expr.sql(dialect=self.dialect))

        table.indexes.append(Index(
             name=idx_name, 
             columns=cols, 
             is_unique=is_unique,
             method=method,
             where_clause=where_clause,
             include_columns=include_columns
        ))

    def _process_alter_table(self, expression: exp.Alter, schema):
        # expression.this is the Table
        table_name = expression.this.name
        # Handle schema qualification if needed
        if expression.this.db:
             table_name = f"{expression.this.db}.{table_name}"
        
        table = schema.get_table(table_name.lower())

        if not table:
            return
            
        actions = expression.actions
        for action in actions:
            if isinstance(action, exp.AddConstraint):
                for constr in action.expressions:
                    if isinstance(constr, exp.Constraint):
                        self._process_table_constraint(constr, table)
            elif isinstance(action, exp.ColumnDef):
                 # Support ALTER TABLE ADD COLUMN
                 self._process_column_def(action, table)

            elif isinstance(action, exp.Drop):
                if action.kind == "CONSTRAINT":
                     names_to_drop = [c.name for c in action.expressions] if action.expressions else [action.this.name]
                     table.foreign_keys = [fk for fk in table.foreign_keys if fk.name not in names_to_drop]
                     table.check_constraints = [ck for ck in table.check_constraints if ck.name not in names_to_drop]
                     table.indexes = [idx for idx in table.indexes if idx.name not in names_to_drop]
                elif action.kind == "COLUMN":
                     col_names = [c.name for c in action.expressions] if action.expressions else [action.this.name]
                     table.columns = [c for c in table.columns if c.name not in col_names]

    def _process_comment(self, expression: exp.Comment, schema):
        # COMMENT ON COLUMN x.y IS 'z'
        # Kind is in args usually (COLUMN, TABLE)
        if not expression or not expression.expression: return

        kind = expression.args.get("kind")
        comment_text = expression.expression.name if expression.expression else ""

        if kind == "COLUMN":
             # expression.this is the column identifier (table.col)
             if isinstance(expression.this, exp.Column):
                  tbl = expression.this.table
                  col = expression.this.name
                  table = schema.get_table(tbl)
                  if table:
                       column_obj = table.get_column(col)
                       if column_obj:
                            column_obj.comment = comment_text
        elif kind == "TABLE":
             # expression.this is Table
             tbl = expression.this.name
             table = schema.get_table(tbl)
             if table:
                  table.comment = comment_text
        elif kind == "CONSTRAINT":
             # COMMENT ON CONSTRAINT x ON y IS 'z'
             const_name = expression.this.name if expression.this else None
             if const_name:
                  for table in schema.tables:
                       for fk in table.foreign_keys:
                            if fk.name == const_name:
                                 fk.comment = comment_text
                       for ck in table.check_constraints:
                            if ck.name == const_name:
                                 ck.comment = comment_text
                       for idx in table.indexes:
                            if idx.name == const_name:
                                 idx.comment = comment_text
