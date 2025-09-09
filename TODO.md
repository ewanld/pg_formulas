# Roadmap
Functions to implement :
| Tag         | Status       | Description |
|-------------|--------------|-------------|
| `TREELEVEL` | 🟢DONE  | Update a "level" column in a table representing a tree structure. |
| `TREEPATH`  | 🟠TODO         | Update a "path" column in a table representing a tree structure.<br>Arguments: table name, name of the column representing the path elements, delimiter string (e.g., `/`). |
| `TREECLOSURE`| 🟠TODO        | Update a closure table representing all ancestor-descendant pairs for each node.
| `COUNTLNK`  | 🟢DONE         | Update a column that counts the number of linked elements. |
| `REVDATE`   | 🟢DONE         | Update a column with the last modification date of the row (+ username retrieved from session context). |
| `CREDATE`   | 🟠TODO         | Same as above, but for the creation date of the row. |
| `AUDIT`     | 🟠TODO         | Populate a history (audit) table. |
| `AGG`       | 🟢DONE  | Create an aggregation function (count + min + max) for rows in a table, with optional GROUP BY.<br>(If no GROUP BY is provided, it counts all rows.) |
| `SUM`       | 🟠TODO         | Create an aggregation function (sum) for rows in a table, with optional GROUP BY.<br>(If no GROUP BY is provided, it sums all rows.)<br>Arguments: table name, group by column. |
| `TOPN`      | 🟠TODO         | Retrieve the top N min/max values from a table.<br>Arguments: table name, column to sort, group by column, number of top results to keep, filtering where condition, operation (min or max). |
| `UNION`     | 🟢DONE         | Merge multiple tables into one (useful for inheritance scenarios for instance). Synchronization between the base (union) table an sub-tables is unidirectional but can go any way (changes to base table are propagated to sub-tables, or changes to sub-tables are propagated to base table).
| `INTERSECT` | 🟠TODO         | Compute the intersection of multiple tables into one. |
| `JOIN`      | 🟠TODO         | In the case of a 1-to-0..1 join, copy the value(s) of one or more joined columns into the main table to avoid using a join in queries. |
| `JSON`      | 🟠TODO         | Extract contents of a JSON column and set the results in other table columns.

# TODO
* UNION_create: make pk column name configurable