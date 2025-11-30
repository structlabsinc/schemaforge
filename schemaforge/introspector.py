import sqlalchemy
from sqlalchemy import inspect
from schemaforge.models import Schema, Table, Column, Index, ForeignKey

class DBIntrospector:
    def __init__(self, db_url: str):
        self.engine = sqlalchemy.create_engine(db_url)
        self.inspector = inspect(self.engine)

    def introspect(self, object_types: list = None) -> Schema:
        schema = Schema()
        
        # Default to tables if not specified
        if not object_types:
            object_types = ['table']
            
        object_types = [t.lower() for t in object_types]
        
        if 'table' in object_types:
            table_names = self.inspector.get_table_names()
    
            for table_name in table_names:
                table = Table(name=table_name)
                
                # Columns
                columns = self.inspector.get_columns(table_name)
                for col in columns:
                    # SQLAlchemy types can be complex objects, we convert to string representation
                    # and try to normalize to our tool's expected format if needed.
                    # For now, we use the string representation provided by SQLAlchemy.
                    col_type = str(col['type'])
                    
                    # Basic normalization to match our parser's output style
                    if 'VARCHAR' in col_type.upper():
                        # SQLAlchemy might return VARCHAR(length)
                        pass
                    elif 'INTEGER' in col_type.upper():
                        col_type = 'INT'
                    
                    dac_col = Column(
                        name=col['name'],
                        data_type=col_type,
                        is_nullable=col['nullable'],
                        default_value=str(col['default']) if col['default'] else None,
                        is_primary_key=False # Will set later via PK constraint
                    )
                    table.columns.append(dac_col)
    
                # Primary Keys
                pk_constraint = self.inspector.get_pk_constraint(table_name)
                if pk_constraint and pk_constraint['constrained_columns']:
                    pk_cols = pk_constraint['constrained_columns']
                    for col in table.columns:
                        if col.name in pk_cols:
                            col.is_primary_key = True
    
                # Foreign Keys
                fks = self.inspector.get_foreign_keys(table_name)
                for fk in fks:
                    dac_fk = ForeignKey(
                        name=fk['name'],
                        column_names=fk['constrained_columns'],
                        ref_table=fk['referred_table'],
                        ref_column_names=fk['referred_columns']
                    )
                    table.foreign_keys.append(dac_fk)
    
                # Indexes
                indexes = self.inspector.get_indexes(table_name)
                for idx in indexes:
                    dac_idx = Index(
                        name=idx['name'],
                        columns=idx['column_names'],
                        is_unique=idx['unique']
                    )
                    table.indexes.append(dac_idx)
    
                schema.tables.append(table)

        return schema
