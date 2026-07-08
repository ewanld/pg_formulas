# Roadmap
Functions to implement:
  
**Aggregate data into a single database field**
  * 🟢DONE : SUM
  * 🟢DONE : COUNT
  * 🟢DONE : MIN
  * 🟢DONE : MAX
  * 🟢DONE : ID_OF_MIN
  * 🟠TODO : ID_OF_MAX
  * 🟢DONE : ARRAY_AGG
  * 🟠TODO : STRING_AGG

**Aggregate data into a dedicated table**:
  * 🟢DONE : MINMAX_TABLE
  * 🟠TODO : SUM_TABLE
  * 🟠TODO : COUNT_TABLE
  * 🟠TODO : TOPN_TABLE

**Merge, split, or join tables:**
  * 🟢DONE : INHERITANCE_TABLE
  * 🟢DONE : UNION_TABLE
  * 🟢DONE : INTERSECT_TABLE
  * 🟠TODO : EXCEPT_TABLE
  
**Synchronize database fields:**
  * 🟠TODO : JOIN
  * 🟢DONE : SYNC
  * 🟠TODO : JSON_FIELD
  * 🟠TODO : DIGEST

**Audit changes:**
  * 🟢DONE : REVDATE
  * 🟢DONE : AUDIT_TABLE
  * 🟠TODO : VERSION_TABLE (same as audit_table but with version_number, version_started_at, version_ended_at)
 
**Aggreate hierarchical data  into a single database field:**
  * 🟢DONE : TREE_LEVEL
  * 🟠TODO : TREEPATH
  * 🟠TODO : TREEHEIGHT

**Aggregate hierarchical data into a dedicated table:**
  * 🟠IN PROGRESS : TREECLOSURE_TABLE
  * 🟠

# Formula implementation checklist
* pg_formulas.sql: implement procedure pgf_XXXX
* pg_formulas.sql: add branch in pgf_set_enabled
* pg_formulas.sql: add branch in pgf_drop
* test.py: add branch in create_tables
* test.py: add kind in test_enable_disable_drop
* test.py: add test cases
* README.md: add doc
* TODO.md : update project status

# TODO
* pgf_inheritance_table: make pk column name configurable
* ALL: drop trigger if exists before creating them
* SYNC: add a filter clause
* SYNC: add a "mapping + inverse_mapping" function (lambda function)
* JOIN : add a filter clause
* ALL : replace all "pk TEXT" arguments to "pk TEXT[]", allowing multiple PK columns to be set. Add a check in code and README to indicate that only one PK column is supported at the time
* ALL : allow deducing PK columns from the meta model instead of passing as argument.
* MINMAX_TABLE: test case with no group by column
* MINMAX_TABLE: refactor: add all column rename arguments into a single hashmap 'rename_columns'
* ALL: add an option to make created tables unlogged (not written to WAL which is faster but is not crash safe.)
* COUNT_TABLE: add a 'multidimensional_aggregation' argument with values: ROLLUP or CUBE
* COUNT : implement filter (use SUM as example)
* PGF_MIN, PGF_MAX : add tests with row_filter clause
* publish extension and update README to show CREATE EXTENSION usage.
* SUM_TABLE: add stddev, skewness, kurtosis as an option
* 