# Roadmap
Functions to implement:
  
**Aggregate data into a single database field**
  * 🟢DONE : SUM
  * 🟢DONE : COUNT
  * 🟢DONE : MIN
  * 🟢DONE : MAX
  * 🟢DONE : ID_OF_MIN
  * 🟠TODO : ID_OF_MAX
  * 🟠DONE : ARRAY_AGG
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

**Auditing changes:**
  * 🟢DONE : REVDATE
  * 🟢DONE : AUDIT_TABLE
 
**Working with trees:**
  * 🟢DONE : TREELEVEL
  * 🟠TODO : TREEPATH
  * 🟠TODO : TREECLOSURE_TABLE

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