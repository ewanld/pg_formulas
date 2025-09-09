# pg_reactive_toolbox

![Build status](https://github.com/ewanld/pg_reactive_toolbox/actions/workflows/python-app.yml/badge.svg)

Postgresql toolbox to make the data more reactive ⚡ using triggers.

🚧 WORK IN PROGRESS 🚧

* [Usage](#Usage)
* [How does it work ?](#How-does-it-work-)
* [Functions summary](#functions-summary)
* [Functions details](#functions-details)
* [Function naming convention](#function-naming-convention)

# Usage
Load the script in your database:
```bash
psql -U username -d database_name -f pg_reactive_toolbox--1.0.sql
```

# How does it work ?
pg_reactive_toolbox is a set of postgresql procedures that make data react to changes, in the same way as Excel formulas can make cells reacts to other cell value changes. Around ~20 procedures (types of "formulas") are implemented, responding to common data synchronization needs.

# Functions summary
**Aggregate functions**:
  * [COUNTLNK](#COUNTLNK) : Update a column that counts the number of linked elements.
  * [AGG](#AGG): Create an aggregation function (min + max + id of min + id of max + row count) for rows in a table, with optional GROUP BY.<br>(If no GROUP BY is provided, it counts all rows.)
  * [SUM](#SUM): Create an aggregation function (sum + row count) for rows in a table, with optional GROUP BY.<br>(If no GROUP BY is provided, it sums all rows.)<br>Arguments: table name, group by column.
  * [COUNT](#COUNT): Count rows in a table, with optional GROUP BY.<br>(If no GROUP BY is provided, it sums all rows.)<br>Arguments: table name, group by column.
  * [ARRAY_AGG_LINKED](#ARRAY_AGG_LINKED): Update a column that aggregates linked elements in an ARRAY, similar to the built-in ARRAY_AGG function.
  * [STRING_AGG_LINKED](#STRING_AGG_LINKED): Update a column that joins linked elements in a string, similar to the built-in STRING_AGG function.
  * [TOPN](#TOPN): Retrieve the top N min/max values from a table. Arguments: table name, column to sort, group by column, number of top results to keep, filtering where condition, operation (min or max). |

**Merge, split, or join tables:**
  * [INHERITANCE](#INHERITANCE): Merge multiple tables into one, while keeping data in-sync between the union table and the sub-tables.
  * [UNION](#UNION): Compute the union of multiple tables into one.
  * [INTERSECT](#INTERSECT): Compute the intersection of multiple tables into one.
  * [JOIN](#JOIN): In the case of a 1-to-0..1 join, copy the value(s) of one or more joined columns into the main table to avoid using a join in queries.

**Audit functions:**
  * [REVDATE](#REVDATE): Automatically update a 'last_modified' column.
  * [CREDATE](#CREDATE): Automatically update a 'creation_date' column.
  * [AUDIT](#CREDATE): Populate a history (audit) table.
 
**Working with trees:**
  * [TREELEVEL](#TREELEVEL): Update a "level" column in a table representing a tree structure.
  * [TREEPATH](#TREEPATH): Update a "path" column in a table representing a tree structure.
  * [TREECLOSURE](#TREECLOSURE): Update a closure table representing all ancestor-descendant pairs for each node.

**Working with JSON:**
  * [JSON](#JSON): Set the value of a JSONB field to be the contents of a table column.

# Functions details

## REVDATE
**Automatically update a 'last_modified' column.**

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

Syntax:
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
call REVDATE_drop('customer', 'last_modified');
```

## CREDATE
**Automatically update a 'creation_date' column.**


## COUNTLNK : Update a column that counts the number of linked elements

TODO

## AGG : Create an aggregation function (count + min or max) for rows in a table, with optional GROUP BY

TODO


## INHERITANCE
**Merge multiple tables into one, while keeping data in-sync between the union table and the sub-tables.**
Synchronization between the base (union) table an sub-tables is unidirectional but can go any way (changes to the base table are propagated to sub-tables, or changes to sub-tables are propagated to the base table).

Syntax:
```sql
PROCEDURE UNION_create (
    id TEXT,
    base_table_name TEXT,
    discriminator_column TEXT DEFAULT 'discriminator',
    sub_tables TEXT[]
    sync_direction TEXT DEFAULT 'BASE_TO_SUB'
)

```
| Argument         | Description |
|-------------|------ |
| id | Id to identify this particular UNION trigger function.
| base_table_name | Name of the base (union) table. This table is created automatically on function call.
| discriminator_column | Name of the discriminator column, i.e the column from the base table that helps distinguish from which sub-table the row is from. (Optional. Default value : 'discriminator').
| sub_tables | Name of tables to be kept in sync with the base table.
| discriminator_values | Name of the discriminator values for each of the sub tables. The length of the array should be the same as the length of the ```sub_tables``` array, and the items should be in the same order. (Optional ; if not set, the discriminator values are the sub-table names).
| sync_direction | Allowed values: 'BASE_TO_SUB' to propagate changes unidirectionally from the base table to the sub-tables ; 'BASE_TO_SUB' to propagate changes unidirectionally from the sub-tables table to the base table. 

Example:
Given the following two tables ```bike``` and ```car```, we would like to synchronize data to a ```vehicle``` table cointaining data from both tables:

<table>
    <thead>
        <tr>
            <th colspan="2">Bike</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><b>PK</b></td>
            <td><b>id</b></td>
        </tr>
        <tr>
            <td/>
            <td>common_atribute1</td>
        </tr>
        <tr>
            <td/>
            <td>bike_atribute1</td>
        </tr>
    </tbody>
</table>

<table>
    <thead>
        <tr>
            <th colspan="2">Car</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><b>PK</b></td>
            <td><b>id</b></td>
        </tr>
        <tr>
            <td/>
            <td>common_atribute1</td>
        </tr>
        <tr>
            <td/>
            <td>car_atribute1</td>
        </tr>
    </tbody>
</table>

From a Postgresql shell execute :
```sql
call UNION_create('uvehicle', 'vehicle', ARRAY['bike', 'car'], 'BASE_To_SUB'); -- create the trigger
```

This will :
* Create the ```vehicle``` table containing columns from both ```bike``` and ```car``` tables, plus a discriminator column (named ```discriminator``` by default).

<table>
    <thead>
        <tr>
            <th colspan="2">Vehicle</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><b>PK</b></td>
            <td><b>id</b></td>
        </tr>
        <tr>
            <td/>
            <td>common_atribute1</td>
        </tr>
        <tr>
            <td/>
            <td>bike_atribute1</td>
        </tr>
        <tr>
            <td/>
            <td>car_atribute1</td>
        </tr>
    </tbody>
</table>

* Create the triggers to synchronize changes from the ```vehicle``` table to the ```bike``` and ```car``` tables;

> **NB**: to synchronize changes from ```bike``` and ```car``` to ```vehicle``` instead, use the argument 'SUB_TO_BASE'.




# Function naming convention
| Function name | Description |
|---------------|-------------|
| XXX_create(id, [args]) | Create the triggers. After creation of the triggers, a full refresh of the data is done (no need to call the refresh function manually). |
| XXX_enable(id, [arg]) | Enable the triggers (triggers are enabled by default) |
| XXX_disable(id, [args]) | Disable the triggers while keeping them in the database structure. |
| XXX_drop(id) | Drop (delete) the triggers. |
| XXX_refresh(id) | Full refresh of the data. Useful after the triggers were disabled the re-enabled, to re-sync the data. |

