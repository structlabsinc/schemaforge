"""
Final coverage gap fill - targeting comparator.py deep logic path.
"""
import pytest
from schemaforge.models import Table, Column, Index, CustomObject, CheckConstraint, ExclusionConstraint, ForeignKey, Schema
from schemaforge.comparator import Comparator

class TestComparatorDeepCoverage:
    """Target comparator.py deep comparison branches."""
    
    def test_compare_exclusion_constraints_modified(self):
        """Test modification of exclusion constraints."""
        comp = Comparator()
        t1 = Table('t')
        t2 = Table('t')
        
        # Add exclusion constraint
        ex1 = ExclusionConstraint(name='ex1', method='gist', elements='c1 WITH =')
        ex2 = ExclusionConstraint(name='ex1', method='btree', elements='c1 WITH =') # Modified method
        
        t1.exclusion_constraints.append(ex1)
        t2.exclusion_constraints.append(ex2)
        
        diff = comp._compare_tables(t1, t2)
        assert len(diff.modified_exclusion_constraints) == 1
        assert diff.modified_exclusion_constraints[0][0].method == 'gist'
        assert diff.modified_exclusion_constraints[0][1].method == 'btree'

    def test_compare_table_properties_all(self):
        """Test all table property change branches."""
        comp = Comparator()
        t1 = Table('t', cluster_by=['a'], retention_days=1, tablespace='ts1', stogroup='sg1', priqty=10, secqty=10, audit='NONE', ccsid='ASCII', without_rowid=False, inherits=['p1'], row_security=False, partition_of='p', partition_bound='def')
        t2 = Table('t', cluster_by=['b'], retention_days=2, tablespace='ts2', stogroup='sg2', priqty=20, secqty=20, audit='ALL', ccsid='UNICODE', without_rowid=True, inherits=['p2'], row_security=True, partition_of='p2', partition_bound='max')
        
        t1.partition_by = 'RANGE (id)'
        t2.partition_by = 'LIST (id)'
        
        t1.database_name = 'db1'
        t2.database_name = 'db2'
        
        t1.comment = 'old'
        t2.comment = 'new'
        
        t1.is_transient = False
        t2.is_transient = True
        
        diff = comp._compare_tables(t1, t2)
        
        changes = "\n".join(diff.property_changes)
        
        assert "Cluster Key" in changes
        assert "Retention" in changes
        assert "Tablespace" in changes
        assert "Stogroup" in changes
        assert "Priqty" in changes
        assert "Secqty" in changes
        assert "Audit" in changes
        assert "CCSID" in changes
        assert "Without RowID" in changes
        assert "Inherits" in changes
        assert "Row Security" in changes
        assert "Partition Of" in changes
        assert "Partition Bound" in changes
        assert "Partition:" in changes
        assert "Database" in changes
        assert "Comment" in changes
        assert "Transient" in changes

    def test_compare_columns_all_attributes(self):
        """Test all column modification branches."""
        comp = Comparator()
        t1 = Table('t')
        t2 = Table('t')
        
        c1 = Column('c', 'int', is_nullable=True, default_value='0', is_primary_key=False, comment='c', collation='C', masking_policy='mp1', is_identity=False, identity_start=1, identity_step=1, identity_cycle=False, is_generated=False, generation_expression=None)
        c2 = Column('c', 'int', is_nullable=False, default_value='1', is_primary_key=True, comment='C', collation='en_US', masking_policy='mp2', is_identity=True, identity_start=10, identity_step=2, identity_cycle=True, is_generated=True, generation_expression='x+1')
        
        # Change data type separately to be sure
        
        t1.columns.append(c1)
        t2.columns.append(c2)
        
        diff = comp._compare_tables(t1, t2)
        assert len(diff.modified_columns) == 1
        
        # Verify _is_column_modified output implicitly (it returns list of strings)
        # We can't access specific strings easily from diff.modified_columns (it stores tuple of Col objects)
        # But for coverage, executing the function branches is enough.
        # But let's verify logic works by calling internal method directly too
        changes = comp._is_column_modified(c1, c2)
        assert len(changes) >= 10 # Many changes
        assert any("Nullable" in c for c in changes)
        assert any("Collation" in c for c in changes)
        assert any("Identity" in c for c in changes)
        assert any("Masking Policy" in c for c in changes)
        assert any("Generated" in c for c in changes)

    def test_compare_policies_lists(self):
        """Test modification of policies list."""
        comp = Comparator()
        t1 = Table('t')
        t2 = Table('t')
        
        t1.policies = ["ROW ACCESS POLICY r1 ON (id)", "MASKING POLICY m1 ON c1"]
        t2.policies = ["ROW ACCESS POLICY r1 ON (id)", "MASKING POLICY m2 ON c1"] # Changed m1->m2
        
        diff = comp._compare_tables(t1, t2)
        changes = "\n".join(diff.property_changes)
        
        # Should see add m2, remove m1
        assert "Policy: MASKING POLICY m2" in changes
        assert "Unset Policy: MASKING POLICY m1" in changes

    def test_compare_tags(self):
        """Test modification of tags."""
        comp = Comparator()
        t1 = Table('t')
        t2 = Table('t')
        
        t1.tags = {'cost': 'low', 'owner': 'bob'}
        t2.tags = {'cost': 'high', 'env': 'prod'} # owner removed, env added, cost changed
        
        diff = comp._compare_tables(t1, t2)
        changes = "\n".join(diff.property_changes)
        
        assert "Tag: cost low -> high" in changes
        assert "Unset Tag: owner" in changes
        assert "Tag: env=prod" in changes

    def test_compare_custom_collections(self):
        """Test compare() main method looping over domains/types/policies."""
        comp = Comparator()
        s1 = Schema()
        s2 = Schema()
        
        # Add new domain
        s2.domains.append(CustomObject('DOMAIN', 'd1'))
        # Modify type
        s1.types.append(CustomObject('TYPE', 't1', properties={'a':1}))
        s2.types.append(CustomObject('TYPE', 't1', properties={'a':2}))
        # Drop policy
        s1.policies.append(CustomObject('POLICY', 'p1'))
        
        plan = comp.compare(s1, s2)
        
        assert len(plan.new_domains) == 1
        assert len(plan.modified_types) == 1
        assert len(plan.dropped_policies) == 1
