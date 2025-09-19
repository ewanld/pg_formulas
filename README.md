
<p align="center">
  <h4 align="center">

![my logo](./docs/icon.png)

[Overview](#overview) • [Getting Started](#getting-started) • [Formulas list](#formulas) • [API Reference](#api-reference) • [Implementation details](#implementation-details)

  ![Build status](https://github.com/ewanld/pg_formulas/actions/workflows/python-app.yml/badge.svg)  
  </h4>
</p>


pg_formulas is a Postgresql extension that brings Excel-like formulas to make your data more **reactive** ⚡, by leveraging triggers.

🚧 WORK IN PROGRESS 🚧

# Overiew
pg_formulas provides a collection of PostgreSQL procedures that allow data to automatically respond to changes—similar to how Excel formulas update cells based on other cells’ values.

The extension currently includes around **20 types of formulas**, covering common data synchronization needs.

Each formula works by creating **one or more triggers** that listen for changes and perform **incremental updates** through the appropriate ```INSERT```, ```UPDATE```, or ```DELETE``` operations. These triggers run synchronously, ensuring that updates are both **instant** and **atomic** (propagated changes occur within the same transaction as the original modification).

# Getting started
Load the script in your database:
```bash
psql -U username -d database_name -f pg_formulas--0.9.sql
```

Create your first formula. The ```COUNT_TABLE``` formula counts table rows:
```sql
pgf_count_table(
    'my_first_formula', -- formula id
    'customers',        -- source table name,
    ARRAY['country'],   -- group by column(s)
    'customers_count'   -- generated table name, holding row counts.
);
```

Given the following ```customers``` table:

| id         | name | country |
|------------|------| -------|
| 1 | ACME | USA |
| 2 | QuantumCore Analytics | USA |
| 2 | NovaDyne Systems | NL |

The ```customers_count``` table is created following the call to ```pgf_count_table```:

| country | row_count |
|------| -------|
| USA | 2 |
| NL  | 1 |

All subsequent ```INSERT```/```UPDATE```/```DELETE``` operations on the ```customers``` table are **propagated automatically** to the ```customers_count``` table. The updates are **incremental**, i.e. do not require an expensive ```COUNT(*)``` computation on each change.

# Formulas
**Aggregate data into a single database field**
* [SUM](#SUM) : Update a field that sums linked elements.
* [COUNT](#COUNT) : Update a field that counts the number of linked elements.
* [MIN](#MIN) : Update a field to represent the min value among linked elements.
* [MAX](#MAX) : Update a field to represent the max value among linked elements.
* [ID_OF_MIN](#ID_OF_MIN) : Update a field to represent the id of min value among linked elements.
* [ID_OF_MAX](#ID_OF_MAX) : Update a field to represent the id of max value among linked elements.
* [ARRAY_AGG](#ARRAY_AGG): Update a field that aggregates linked elements in an ARRAY, similar to the built-in ARRAY_AGG function. Arguments: limit(optional): limit the number of items in the ARRAY.
* [STRING_AGG](#STRING_AGG): Update a field that joins linked elements in a string, similar to the built-in STRING_AGG function. Arguments: limit(optional): limit the number of items in the string.

**Aggregate data into a dedicated table**:
  * [MINMAX_TABLE](#MINMAX_TABLE): Store min and max values from a table (along with the id of those rows), with optional GROUP BY. (If no GROUP BY is provided, it counts all rows.)
  * [SUM_TABLE](#SUM_TABLE): Sum rows from a table, with optional GROUP BY.<br>(If no GROUP BY is provided, it sums all rows.)<br>Arguments: table name, group by column.
  * [COUNT_TABLE](#COUNT_TABLE): Count rows from a table, with optional GROUP BY.<br>(If no GROUP BY is provided, it sums all rows.)<br>Arguments: table name, group by column.
  * [TOPN_TABLE](#TOPN_TABLE): Retrieve the top N min/max values from a table. Arguments: table name, column to sort, group by column, number of top results to keep, filtering where condition, operation (min or max).

**Merge, split, or join tables:**
  * [INHERITANCE_TABLE](#INHERITANCE_TABLE): Merge multiple tables into one, while keeping data in-sync between the base table and the sub-tables.
  * [UNION_TABLE](#UNION_TABLE): Compute the union of multiple tables into one.
  * [INTERSECT_TABLE](#INTERSECT_TABLE): Compute the intersection of multiple tables into one.
  * [EXCEPT_TABLE](#EXCEPT_TABLE): Compute the difference of two tables (```A EXCEPT B```).
  * [JOIN](#JOIN): In the case of a 1-to-0..1 join, copy the value(s) of one or more joined columns into the main table.

**Auditing changes:**
  * [REVDATE](#REVDATE): Automatically update a 'last_modified' column.
  * [CREDATE](#CREDATE): Automatically update a 'creation_date' column.
  * [AUDIT_TABLE](#AUDIT_TABLE): Populate a history (audit) table.
 
**Working with trees:**
  * [TREELEVEL](#TREELEVEL): Update a "level" column in a table representing a tree structure.
  * [TREEPATH](#TREEPATH): Update a "path" column in a table representing a tree structure.
  * [TREECLOSURE_TABLE](#TREECLOSURE_TABLE): Update a closure table representing all ancestor-descendant pairs for each node.

**Working with JSON:**
  * [JSON_FIELD](#JSON): Set the value of a JSONB field to be the contents of a table column.

# API reference
## Common functions
| Function name | Description |
|---------------|-------------|
| ```pgf_set_enabled(formula_id TEXT, enabled BOOLEAN)``` | Enable or disable the triggers associated with this formula (NB: triggers are enabled by default after creation.) |
| ```pgf_drop(formula_id)``` | Drop (delete) the triggers associated with this formula. |
| ```pgf_refresh(formula_id)``` | Full refresh of the data (force a full re-sync).



## REVDATE formula
**_Automatically update a 'last_modified' column._**

### Syntax
```sql
PROCEDURE pgf_revdate(formula_id TEXT, table_name TEXT, column_name TEXT)
```

| Argument         | Description |
|-------------|------ |
| formula_id | Id to identify this particular formula instance (must be unique across all declared formulas).
| table_name | Name of the table containing the column to update
| column_name | Name of the column to update. The column must have a date or datetime type and must exist in the table structure (pg_formulas does not create the column). 

### Example
From the below table ```customer```, we want to update the ```last_modified``` column automatically when a row is modified.

| id | name | last_modified |
|----------|----------|----------|
| 1  | John Doe  | ⚡2025-01-01T10:00:00Z  |
| 2  | Benedict Cumberbatch  | ⚡2025-05-15T10:00:05Z  |

Then from a Postgresql shell execute :
```sql
call pgf_revdate('customer_modified_at', 'customer', 'last_modified'); -- create the trigger

update customer set name'Jack Doe' where id=1; -- the trigger is called on update
select last_modified from customer where id=1; -- returns the timestamp of the update statement
```



## CREDATE formula
**Automatically update a 'creation_date' column.**

TODO

## COUNT formula
**_Update a column that counts the number of linked elements_**

TODO

## AGG formula
**_Create an aggregation function (count + min or max) for rows in a table, with optional GROUP BY_**

TODO


## INHERITANCE_TABLE formula
**_Merge multiple tables into one, while keeping data in-sync between the base table and the sub-tables._**
Synchronization between the base (union) table an sub-tables is unidirectional but can go any way (changes to the base table are propagated to sub-tables, or changes to sub-tables are propagated to the base table).

Syntax:
```sql
PROCEDURE pgf_inheritance_table (
    id TEXT,
    base_table_name TEXT,
    discriminator_column TEXT DEFAULT 'discriminator',
    sub_tables TEXT[]
    sync_direction TEXT DEFAULT 'BASE_TO_SUB'
)

```
| Argument         | Description |
|-------------|------ |
| formula_id | formula_id | Id to identify this particular formula instance (must be unique across all declared formulas).
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
call pgf_inheritance_table('uvehicle', 'vehicle', ARRAY['bike', 'car'], 'BASE_To_SUB'); -- create the trigger
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

# Implementation details
A metadata table named ```pgf_metadata``` is created to track all formula declarations.