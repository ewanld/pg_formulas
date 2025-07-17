# pg_reactive_toolbox
Postgresql toolbox to make the data more reactive ⚡ using triggers.

🚧 WORK IN PROGRESS 🚧

# Usage
Load the script in your database:
```bash
psql -U username -d database_name -f pg_reactive_toolbox.sql
```

## REVDATE : Update a column with the last modification date of the row

Example table ```customer```. We want to update the ```last_modified``` column automatically when a row is modified.

| id | name | last_modified |
|----------|----------|----------|
| 1  | John Doe  | 2025-01-01T10:00:00Z  |
| 2  | Benedict Cumberbatch  | 2025-05-15T10:00:05Z  |

Then from a Postgresql shell execute :
```sql
call REVDATE_create('customer_modified_at', 'customer', 'last_modified'); -- create the trigger

update customer set name'Jack Doe' where id=1; -- the trigger is called on update
select last_modified from customer where id=1; -- returns the timestamp of the update statement
```

Under the hood, pg_reactive_toolbox creates one or more triggers.

Arguments:
```sql
PROCEDURE REVDATE_create (id TEXT, table_name TEXT, column_name TEXT)
```

| Argument         | Description |
|-------------|------ |
| id | Id to identify this particular REVDATE trigger function. (e.g 'customer_modified_at')
| table_name | Name of the table containing the column to update
| column_name | Name of the column to update. The column must have a date or datetime type and must exist in the table structure (pg_reactive_toolbox does not create the column). 



To disable the trigger:
```sql
call REVDATE_disable('customer', 'last_modified');
```

To drop the trigger:
```sql
call REVDATE_disable('customer', 'last_modified');
```

# Roadmap
Functions to implement :
| Tag         | Status       | Description |
|-------------|--------------|-------------|
| `TREELEVEL` | TODO         | Update a "level" column in a table representing a tree structure. |
| `TREEPATH`  | TODO         | Update a "path" column in a table representing a tree structure.<br>Arguments: table name, name of the column representing the path elements, delimiter string (e.g., `/`). |
| `COUNTLNK`  | DONE         | Update a column that counts the number of linked elements.<br>Arguments: base table, base table PK, base table FK, foreign table, foreign table parent ID column, name of the count column in the base table. |
| `REVDATE`   | DONE         | Update a column with the last modification date of the row (+ username retrieved from session context). |
| `CREDATE`   | TODO         | Same as above, but for the creation date of the row. |
| `AUDIT`     | TODO         | Populate a history (audit) table. |
| `AGG`       | IN_PROGRESS  | Create an aggregation function (count + min or max) for rows in a table, with optional GROUP BY.<br>(If no GROUP BY is provided, it counts all rows.)<br>Arguments: table name, group by column, filtering "where" condition. |
| `SUM`       | TODO         | Create an aggregation function (sum) for rows in a table, with optional GROUP BY.<br>(If no GROUP BY is provided, it sums all rows.)<br>Arguments: table name, group by column, filtering "where" condition. |
| `TOPN`      | TODO         | Retrieve the top N min/max values from a table.<br>Arguments: table name, column to sort, group by column, number of top results to keep, filtering where condition, operation (min or max). |
| `UNION`     | TODO         | Merge multiple tables into one (useful for Hibernate inheritance scenarios for instance). |
| `INTERSECT` | TODO         | Compute the intersection of multiple tables into one. |
| `JOIN`      | TODO         | In the case of a 1-to-0..1 join, copy the value(s) of one or more joined columns into the main table to avoid using a join in queries. |


# Function naming convention
| Function name | Description |
|---------------|-------------|
| XXX_create(id, [args]) | Create the triggers. After creation of the triggers, a full refresh of the data is done (no need to call the refresh function manually). |
| XXX_enable(id, [arg]) | Enable the triggers (triggers are enabled by default) |
| XXX_disable(id, [args]) | Disable the triggers while keeping them in the database structure. |
| XXX_drop(id, args) | Drop (delete) the triggers. |
| XXX_refresh_{id} | Full refresh of the data. Useful after the triggers were disabled the re-enabled, to re-sync the data. |

