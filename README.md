
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

The extension includes **25 types of formulas**, covering common data synchronization needs.

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

| id  | name                  | country |
| --- | --------------------- | ------- |
| 1   | ACME                  | USA     |
| 2   | QuantumCore Analytics | USA     |
| 2   | NovaDyne Systems      | NL      |

The ```customers_count``` table is created following the call to ```pgf_count_table```:

| country | row_count |
| ------- | --------- |
| USA     | 2         |
| NL      | 1         |

All subsequent ```INSERT```/```UPDATE```/```DELETE``` operations on the ```customers``` table are **propagated automatically** to the ```customers_count``` table. The updates are **incremental**, i.e. do not require an expensive ```COUNT(*)``` computation on each change.

# Formulas
**Aggregate data into a single database field**
* [SUM](#SUM-formula) : Update a field that sums linked elements.
* [COUNT](#COUNT-formula) : Update a field that counts the number of linked elements.
* [MIN](#MIN-formula) : Update a field that stores the min value among linked elements.
* [MAX](#MAX-formula) : Update a field that stores the max value among linked elements.
* [ID_OF_MIN](#ID_OF_MIN-formula) : Update a field that stores the id of the linked row with the minimum value.
* [ID_OF_MAX](#ID_OF_MAX-formula) : Update a field that stores the id of the linked row with the maximum value.
* [ARRAY_AGG](#ARRAY_AGG-formula): Update a field that aggregates linked elements in an ARRAY, similar to the built-in ARRAY_AGG function.
* [STRING_AGG](#STRING_AGG-formula): Update a field that joins linked elements in a string, similar to the built-in STRING_AGG function. Arguments: limit(optional): limit the number of items in the string.

**Aggregate data into a dedicated table**:
  * [MINMAX_TABLE](#MINMAX_TABLE-formula): Store min and max values from a table (along with the id of those rows), with optional GROUP BY. (If no GROUP BY is provided, it counts all rows.)
  * [SUM_TABLE](#SUM_TABLE-formula): Sum rows from a table, with optional GROUP BY (If no GROUP BY is provided, it sums all rows.)
  * [COUNT_TABLE](#COUNT_TABLE-formula): Count rows from a table, with optional GROUP BY (If no GROUP BY is provided, it counts all rows.)
  * [TOPN_TABLE](#TOPN_TABLE-formula): Retrieve the top N min/max values from a table. Arguments: table name, column to sort, group by column, number of top results to keep, filtering where condition, operation (min or max).

**Aggregate hierarchical data into a single database field:**
  * [TREE_LEVEL](#TREE_LEVEL-formula): Update a "level" column in a table representing a tree structure.
  * [TREE_PATH](#TREE_PATH-formula): Update a "path" column in a table representing a tree structure.
  * [TREE_HEIGHT](#TREEHEIGHT-formula): Update a "height" column in a table representing a tree structure.
  * [TREE_SUM](#SUM-formula) : Update a field that sums descendant elements.
  * [TREE_COUNT](#COUNT-formula) : Update a field that counts the number of descendant elements.
  * [TREE_MIN](#MIN-formula) : Update a field that stores the min value among descendant elements.
  * [TREE_MAX](#MAX-formula) : Update a field that stores the max value among descendant elements.
  * [TREE_ID_OF_MIN](#ID_OF_MIN-formula) : Update a field that stores the id of the descendant row with the minimum value.
  * [TREE_ID_OF_MAX](#ID_OF_MAX-formula) : Update a field that stores the id of the descendant row with the maximum value.
  
**Aggregate hierarchical data into a dedicated table:**
  * [TREE_CLOSURE_TABLE](#TREE_CLOSURE_TABLE-formula): Update a closure table representing all ancestor-descendant pairs for each node.
  * [TREE_SUM_TABLE](#SUM_TABLE-formula): Sum descendant rows from a table, with optional GROUP BY.<br>(If no GROUP BY is provided, it sums all descendant rows.)
  * [TREE_COUNT_TABLE](#COUNT_TABLE-formula): Count descendant rows from a table, with optional GROUP BY.<br>(If no GROUP BY is provided, it counts all descendant rows.)
  
**Combine and compare tables:**
  * [INHERITANCE_TABLE](#INHERITANCE_TABLE-formula): Merge multiple tables into one while keeping the base table and sub-tables synchronized.
  * [UNION_TABLE](#UNION_TABLE-formula): Compute the union of multiple tables into one.
  * [INTERSECT_TABLE](#INTERSECT_TABLE-formula): Compute the intersection of multiple tables into one.
  * [EXCEPT_TABLE](#EXCEPT_TABLE-formula): Compute the difference of two tables (```A EXCEPT B```).
  * [DIFF_TABLE](#DIFF_TABLE-formula): compare two tables.

**Synchronize database fields:**
  * [JOIN](#JOIN-formula): In the case of a 1-to-0..1 join, copy the value(s) of one or more joined columns into the main table.
  * [SYNC](#SYNC-formula): Synchronize two fields from the same table row.
  * [JSON_FIELD](#JSON-formula): Set the value of a JSONB field to be the contents of a table column.

**Audit changes:**
  * [REVDATE](#REVDATE-formula): Automatically update a 'last_modified' column.
  * [AUDIT_TABLE](#AUDIT_TABLE-formula): Populate a history (audit) table.
  * [VERSION_TABLE](#VERSION_TABLE-formula): Enable versionning of table rows in a dedicated versionning table.

 

# API reference
## Common functions
| Function name                                   | Description                                                                                                       |
| ----------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| ```pgf_set_enabled(id TEXT, enabled BOOLEAN)``` | Enable or disable the triggers associated with this formula (NB: triggers are enabled by default after creation.) |
| ```pgf_drop(id TEXT)```                         | Drop (delete) the triggers associated with this formula.                                                          |
| ```pgf_refresh(id TEXT)```                      | Full refresh of the data (force a full re-sync).                                                                  |



## REVDATE formula
**_Automatically update a 'last_modified' column._**

### Syntax
```sql
PROCEDURE pgf_revdate(
    id TEXT,
    table_name TEXT,
    column_name TEXT
)
```

| Argument          | Description                                                                                                                                                |
| ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ```id```          | Id to identify this particular formula instance (must be unique across all declared formulas).                                                             |
| ```table_name```  | Name of the table containing the column to update                                                                                                          |
| ```column_name``` | Name of the column to update. The column must have a date or datetime type and must exist in the table structure (pg_formulas does not create the column). |

### Example
From the below table ```customer```, we want to update the ```last_modified``` column automatically when a row is modified.

| id  | name                 | ⚡ last_modified      |
| --- | -------------------- | -------------------- |
| 1   | John Doe             | 2025-01-01T10:00:00Z |
| 2   | Benedict Cumberbatch | 2025-05-15T10:00:05Z |

Then from a Postgresql shell execute :
```sql
call pgf_revdate('customer_modified_at', 'customer', 'last_modified'); -- create the trigger

update customer set name'Jack Doe' where id=1; -- the trigger is called on update
select last_modified from customer where id=1; -- returns the timestamp of the update statement
```


## SUM formula
**_Update a field that sums linked elements_**

### Syntax
```sql
PROCEDURE pgf_sum (
    id TEXT,
    base_table_name TEXT,
    base_pk TEXT,
    base_aggregate_column TEXT,
    linked_table_name TEXT,
    linked_fk TEXT,
    linked_value_column TEXT,
    options JSONB DEFAULT '{}'
)
```

| Argument                      | Description                                                                                                                                                                  |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ```id```                      | Id to identify this particular formula instance (must be unique across all declared formulas).                                                                               |
| ```base_table_name```         | Name of the base table holding the "sum" field.                                                                                                                              |
| ```base_pk```                 | Name of the primary key column in the base table.                                                                                                                            |
| ⚡ ```base_aggregate_column``` | Name of the column from the base table that will store the sum. **The column must have a default value of 0, and all insertions must be done with this default value of 0.** |
| ```linked_table_name```       | Name of the linked table containing rows to be summed.                                                                                                                       |
| ```linked_fk```               | Name of the foreign key column in the linked table referencing the base table primary key.                                                                                   |
| ```linked_value_column```     | Name of the numeric column in the linked table whose values are summed.                                                                                                      |
| ```options```                 | Additional optional arguments, passed as a JSONB object (see available options below).                                                                                       |

Additional options :
| JSONB field  | Default value | Description                                                                                                                                                                                                                                                                                   |
| ------------ | ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ```filter``` | ```'true'```  | SQL expression applied to rows from the linked table. The expression must evaluate to a boolean result. Only rows matching this filter are included in the sum. The SQL expression can reference columns from the linked table, unprefixed (except for the ```linked_value_column``` column). |

### Example
From the below tables, we want to maintain `customer.total_spent` as the sum of `order.amount` for each customer.

`customer` table:
| id  | name     | ⚡ total_spent |
| --- | -------- | ------------- |
| 1   | John Doe | 0             |
| 2   | Jane Roe | 0             |

`order` table:
| id  | customer_id | amount |
| --- | ----------- | ------ |
| 1   | 1           | 100    |
| 2   | 1           | 50     |
| 3   | 2           | 200    |

Then from a PostgreSQL shell execute:
```sql
call pgf_sum(
    'customer_total_spent', -- id
    'customer',           -- base_table_name
    'id',                 -- base_pk
    'total_spent',        -- base_aggregate_column
    'order',              -- linked_table_name
    'customer_id',        -- linked_fk
    'amount'              -- linked_value_column
);
```

After each order (insert/update/delete), the `customer.total_spent` column is updated automatically :

| id  | name     | ⚡ total_spent |
| --- | -------- | ------------- |
| 1   | John Doe | 150           |
| 2   | Jane Roe | 200           |


## MIN formula
**_Update a field to represent the min value among linked elements._**

### Syntax
```sql
PROCEDURE pgf_min (
    id TEXT,
    base_table_name TEXT,
    base_pk TEXT,
    base_aggregate_column TEXT,
    linked_table_name TEXT,
    linked_fk TEXT,
    linked_value_column TEXT,
    options JSONB DEFAULT '{}'
)
```

| Argument                      | Description                                                                                                                                                                                                                                           |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ```id```                      | Id to identify this particular formula instance (must be unique across all declared formulas).                                                                                                                                                        |
| ```base_table_name```         | Name of the base table holding the target (min) field.                                                                                                                                                                                                |
| ```base_pk```                 | Name of the primary key column in the base table.                                                                                                                                                                                                     |
| ⚡ ```base_aggregate_column``` | Name of the column from the base table that will store the min value. **The column must be created with a default value of ```NULL```. All insertions must be done with this ```NULL``` value and no updates should be done manually to this field.** |
| ```linked_table_name```       | Name of the linked table containing rows to be considered.                                                                                                                                                                                            |
| ```linked_fk```               | Name of the foreign key column in the linked table referencing the base table primary key.                                                                                                                                                            |
| ```linked_value_column```     | Name of the column in the linked table whose minimum value will be tracked.                                                                                                                                                                           |
| ```options```                 | Additional optional arguments, passed as a JSONB object (see available options below).                                                                                                                                                                |

Additional options :
| JSONB field  | Default value | Description                                                                                                                                                                                                                                                                                                     |
| ------------ | ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ```filter``` | ```'true'```  | SQL expression applied to rows from the linked table. The expression must evaluate to a boolean result. Only rows matching this filter are considered when computing the minimum. The SQL expression can reference columns from the linked table, unprefixed (except for the ```linked_value_column``` column). |

### Example
From the below tables, we want to maintain `product.min_price` as the minimum `listing.price` for each product.

`product` table:
| id  | name     | ⚡ min_price |
| --- | -------- | ----------- |
| 1   | Widget A | NULL        |
| 2   | Widget B | NULL        |

`listing` table:
| id  | product_id | price |
| --- | ---------- | ----- |
| 1   | 1          | 100   |
| 2   | 1          | 50    |
| 3   | 2          | 200   |

Then from a PostgreSQL shell execute:
```sql
call pgf_min(
    'product_min_price', -- id
    'product',            -- base_table_name
    'id',                 -- base_pk
    'min_price',          -- base_aggregate_column
    'listing',            -- linked_table_name
    'product_id',         -- linked_fk
    'price'               -- linked_value_column
);
```

After each change to the `listing` table, `product.min_price` is updated automatically:

| id  | name     | ⚡ min_price |
| --- | -------- | ----------- |
| 1   | Widget A | 50          |
| 2   | Widget B | 200         |


## ID_OF_MIN formula
**_Update a field to store the id of the linked row with the minimum value._**

### Syntax
```sql
PROCEDURE pgf_id_of_min (
    id TEXT,
    base_table_name TEXT,
    base_pk TEXT,
    base_aggregate_column TEXT,
    linked_table_name TEXT,
    linked_fk TEXT,
    linked_value_column TEXT,
    options JSONB DEFAULT '{}'
)
```

| Argument                      | Description                                                                                                                                                                                                                                                                             |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ```id```                      | Id to identify this particular formula instance (must be unique across all declared formulas).                                                                                                                                                                                          |
| ```base_table_name```         | Name of the base table holding the "id of min" target field.                                                                                                                                                                                                                            |
| ```base_pk```                 | Name of the primary key column in the base table.                                                                                                                                                                                                                                       |
| ⚡ ```base_aggregate_column``` | Name of the column from the base table that will store the id of the linked row with the minimum value. **The column must be created with a default value of ```NULL```. All insertions must be done with this ```NULL``` value and no updates should be done manually to this field.** |
| ```linked_table_name```       | Name of the linked table containing rows to be considered.                                                                                                                                                                                                                              |
| ```linked_fk```               | Name of the foreign key column in the linked table referencing the base table primary key.                                                                                                                                                                                              |
| ```linked_value_column```     | Name of the column in the linked table whose minimum value is used to determine the id to track.                                                                                                                                                                                        |
| ```options```                 | Additional optional arguments, passed as a JSONB object (see available options below).                                                                                                                                                                                                  |

Additional options :
| JSONB field  | Default value | Description                                                                                                                                                                                                                                                                   |
| ------------ | ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ```filter``` | ```'true'```  | SQL expression applied to rows from the linked table. The expression must evaluate to a boolean result. Only rows matching this filter are considered when computing the id of the minimum value. The SQL expression can reference columns from the linked table, unprefixed. |

### Example
From the below tables, we want to maintain `product.min_price_listing_id` as the id of the listing with the minimum `price` for each product.

`product` table:
| id  | name     | ⚡ min_price_listing_id |
| --- | -------- | ---------------------- |
| 1   | Widget A | NULL                   |
| 2   | Widget B | NULL                   |

`listing` table:
| id  | product_id | price |
| --- | ---------- | ----- |
| 1   | 1          | 100   |
| 2   | 1          | 50    |
| 3   | 2          | 200   |

Then from a PostgreSQL shell execute:
```sql
call pgf_id_of_min(
    'product_min_price_listing_id', -- id
    'product',                        -- base_table_name
    'id',                             -- base_pk
    'min_price_listing_id',           -- base_aggregate_column
    'listing',                        -- linked_table_name
    'product_id',                     -- linked_fk
    'price'                           -- linked_value_column
);
```

After each change to the `listing` table, `product.min_price_listing_id` is updated automatically:

| id  | name     | ⚡ min_price_listing_id |
| --- | -------- | ---------------------- |
| 1   | Widget A | 2                      |
| 2   | Widget B | 3                      |

## ID_OF_MAX formula
**_Update a field to store the id of the linked row with the maximum value._**

### Syntax
```sql
PROCEDURE pgf_id_of_max (
    id TEXT,
    base_table_name TEXT,
    base_pk TEXT,
    base_aggregate_column TEXT,
    linked_table_name TEXT,
    linked_fk TEXT,
    linked_value_column TEXT,
    options JSONB DEFAULT '{}'
)
```

| Argument                      | Description                                                                                                                                                                                                                                                                             |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ```id```                      | Id to identify this particular formula instance (must be unique across all declared formulas).                                                                                                                                                                                          |
| ```base_table_name```         | Name of the base table holding the "id of max" target field.                                                                                                                                                                                                                            |
| ```base_pk```                 | Name of the primary key column in the base table.                                                                                                                                                                                                                                       |
| ⚡ ```base_aggregate_column``` | Name of the column from the base table that will store the id of the linked row with the maximum value. **The column must be created with a default value of ```NULL```. All insertions must be done with this ```NULL``` value and no updates should be done manually to this field.** |
| ```linked_table_name```       | Name of the linked table containing rows to be considered.                                                                                                                                                                                                                              |
| ```linked_fk```               | Name of the foreign key column in the linked table referencing the base table primary key.                                                                                                                                                                                              |
| ```linked_value_column```     | Name of the column in the linked table whose maximum value is used to determine the id to track.                                                                                                                                                                                        |
| ```options```                 | Additional optional arguments, passed as a JSONB object (see available options below).                                                                                                                                                                                                  |

Additional options :
| JSONB field  | Default value | Description                                                                                                                                                                                                                                                                   |
| ------------ | ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ```filter``` | ```'true'```  | SQL expression applied to rows from the linked table. The expression must evaluate to a boolean result. Only rows matching this filter are considered when computing the id of the maximum value. The SQL expression can reference columns from the linked table, unprefixed. |

### Example
From the below tables, we want to maintain `product.max_price_listing_id` as the id of the listing with the maximum `price` for each product.

`product` table:
| id  | name     | ⚡ max_price_listing_id |
| --- | -------- | ---------------------- |
| 1   | Widget A | NULL                   |
| 2   | Widget B | NULL                   |

`listing` table:
| id  | product_id | price |
| --- | ---------- | ----- |
| 1   | 1          | 100   |
| 2   | 1          | 50    |
| 3   | 2          | 200   |

Then from a PostgreSQL shell execute:
```sql
call pgf_id_of_max(
    'product_max_price_listing_id', -- id
    'product',                        -- base_table_name
    'id',                             -- base_pk
    'max_price_listing_id',           -- base_aggregate_column
    'listing',                        -- linked_table_name
    'product_id',                     -- linked_fk
    'price'                           -- linked_value_column
);
```

After each change to the `listing` table, `product.max_price_listing_id` is updated automatically:

| id  | name     | ⚡ max_price_listing_id |
| --- | -------- | ---------------------- |
| 1   | Widget A | 1                      |
| 2   | Widget B | 3                      |


## ARRAY_AGG formula
**_Update a field that aggregates linked elements in an ARRAY, similar to the built-in ARRAY_AGG function._**

### Syntax
```sql
PROCEDURE pgf_array_agg (
    id TEXT,
    base_table_name TEXT,
    base_pk TEXT,
    base_aggregate_column TEXT,
    linked_table_name TEXT,
    linked_fk TEXT,
    linked_value_column TEXT,
    options JSONB DEFAULT '{}'
)
```

| Argument                      | Description                                                                                                                                                                                                                                       |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ```id```                      | Id to identify this particular formula instance (must be unique across all declared formulas).                                                                                                                                                    |
| ```base_table_name```         | Name of the base table holding the target ARRAY field.                                                                                                                                                                                            |
| ```base_pk```                 | Name of the primary key column in the base table.                                                                                                                                                                                                 |
| ⚡ ```base_aggregate_column``` | Name of the column from the base table that will store the ARRAY. **The column must be created with a default value of ```NULL```. All insertions must be done with this ```NULL``` value and no updates should be done manually to this field.** |
| ```linked_table_name```       | Name of the linked table containing rows to be aggregated.                                                                                                                                                                                        |
| ```linked_fk```               | Name of the foreign key column in the linked table referencing the base table primary key.                                                                                                                                                        |
| ```linked_value_column```     | Name of the column in the linked table whose values will be aggregated into an ARRAY.                                                                                                                                                             |
| ```options```                 | Additional optional arguments, passed as a JSONB object (see available options below).                                                                                                                                                            |

Additional options :
| JSONB field    | Default value | Description                                                                                                                                                                                                                                   |
| -------------- | ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ```filter```   | ```'true'```  | SQL expression applied to rows from the linked table. The expression must evaluate to a boolean result. Only rows matching this filter are included in the ARRAY. The SQL expression can reference columns from the linked table, unprefixed. |
| ```order_by``` | ```NULL```    | SQL expression appended to the inner query `ORDER BY` clause to control the order of values in the resulting ARRAY. The expression can reference columns from the linked table, unprefixed.                                                   |
| ```distinct``` | ```true```    | When set to ```true```, duplicate values are removed before aggregation. When set to ```false```, duplicates are preserved.                                                                                                                   |
| ```limit```    | ```NULL```    | Maximum number of items to include in the aggregated ARRAY. If omitted, all matching values are included.                                                                                                                                     |

### Example
From the below tables, we want to maintain `product.prices` as the array of `listing.price` values for each product.

`product` table:
| id  | name     | ⚡ prices |
| --- | -------- | -------- |
| 1   | Widget A | NULL     |
| 2   | Widget B | NULL     |

`listing` table:
| id  | product_id | price |
| --- | ---------- | ----- |
| 1   | 1          | 100   |
| 2   | 1          | 50    |
| 3   | 2          | 200   |

Then from a PostgreSQL shell execute:
```sql
call pgf_array_agg(
    'product_price_history', -- id
    'product',                -- base_table_name
    'id',                     -- base_pk
    'price_history',          -- base_aggregate_column
    'listing',                -- linked_table_name
    'product_id',             -- linked_fk
    'price',                   -- linked_value_column
    '{
        "order_by": "id desc"
    }'::JSONB
);
```

After each change to the `listing` table, `product.prices` is updated automatically:

| id  | name     | ⚡ prices  |
| --- | -------- | --------- |
| 1   | Widget A | [50, 100] |
| 2   | Widget B | [200]     |


## MAX formula
**_Update a field to represent the max value among linked elements._**

### Syntax
```sql
PROCEDURE pgf_max (
    id TEXT,
    base_table_name TEXT,
    base_pk TEXT,
    base_aggregate_column TEXT,
    linked_table_name TEXT,
    linked_fk TEXT,
    linked_value_column TEXT,
    options JSONB DEFAULT '{}'
)
```

| Argument                      | Description                                                                                                                                                                                                                                             |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ```id```                      | Id to identify this particular formula instance (must be unique across all declared formulas).                                                                                                                                                          |
| ```base_table_name```         | Name of the base table holding the target (max) field.                                                                                                                                                                                                  |
| ```base_pk```                 | Name of the primary key column in the base table.                                                                                                                                                                                                       |
| ⚡ ```base_aggregate_column``` | Name of the column from the base table that will store the max value.                                                                                                                                                                                   |
| ```linked_table_name```       | Name of the linked table containing rows to be considered.                                                                                                                                                                                              |
| ```linked_fk```               | Name of the foreign key column in the linked table referencing the base table primary key.                                                                                                                                                              |
| ```linked_value_column```     | Name of the column in the linked table whose maximum value will be tracked. The column must be created with a default value of ```NULL```. All insertions must be done with this ```NULL``` value and no updates should be done manually to this field. |
| ```options```                 | Additional optional arguments, passed as a JSONB object (see available options below).                                                                                                                                                                  |

Additional options :
| JSONB field  | Default value | Description                                                                                                                                                                                                                                                                                                     |
| ------------ | ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ```filter``` | ```'true'```  | SQL expression applied to rows from the linked table. The expression must evaluate to a boolean result. Only rows matching this filter are considered when computing the maximum. The SQL expression can reference columns from the linked table, unprefixed (except for the ```linked_value_column``` column). |

### Example
From the below tables, we want to maintain `product.max_price` as the maximum `listing.price` for each product.

`product` table:
| id  | name     | ⚡ max_price |
| --- | -------- | ----------- |
| 1   | Widget A | NULL        |
| 2   | Widget B | NULL        |

`listing` table:
| id  | product_id | price |
| --- | ---------- | ----- |
| 1   | 1          | 100   |
| 2   | 1          | 50    |
| 3   | 2          | 200   |

Then from a PostgreSQL shell execute:
```sql
call pgf_max(
    'product_max_price', -- id
    'product',            -- base_table_name
    'id',                 -- base_pk
    'max_price',          -- base_aggregate_column
    'listing',            -- linked_table_name
    'product_id',         -- linked_fk
    'price'               -- linked_value_column
);
```

After each change to the `listing` table, `product.max_price` is updated automatically:

| id  | name     | ⚡ max_price |
| --- | -------- | ----------- |
| 1   | Widget A | 50          |
| 2   | Widget B | 200         |




## COUNT formula
**_Update a field that counts the number of linked elements._**

### Syntax
```sql
PROCEDURE pgf_count (
    id TEXT,
    base_table_name TEXT,
    base_pk TEXT,
    base_count_column TEXT,
    linked_table_name TEXT,
    linked_fk TEXT,
    options JSONB DEFAULT '{}'
)
```

| Argument                  | Description                                                                                    |
| ------------------------- | ---------------------------------------------------------------------------------------------- |
| ```id```                  | Id to identify this particular formula instance (must be unique across all declared formulas). |
| ```base_table_name```     | Name of the base table holding the count field.                                                |
| ```base_pk```             | Name of the primary key column in the base table.                                              |
| ⚡ ```base_count_column``` | Name of the column from the base table that will store the count.                              |
| ```linked_table_name```   | Name of the linked table containing rows to be counted.                                        |
| ```linked_fk```           | Name of the foreign key column in the linked table referencing the base table primary key.     |
| ```options```             | Additional optional arguments, passed as a JSONB object (see available options below).         |

Additional options :
| JSONB field  | Default value | Description                                                                                                                                                                                                                             |
| ------------ | ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ```filter``` | ```'true'```  | SQL expression applied to rows from the linked table. The SQL expression must evaulate to a boolean. Only rows matching this filter are included in the count. The expression can reference columns from the linked table (unprefixed). |

### Example
From the below tables, we want to keep `customer.order_count` updated with the number of orders for each customer.

`customer` table:
| id  | name     | ⚡ order_count |
| --- | -------- | ------------- |
| 1   | John Doe | 0             |
| 2   | Jane Roe | 0             |

`order` table:
| id  | customer_id | amount |
| --- | ----------- | ------ |
| 1   | 1           | 100    |
| 2   | 1           | 50     |
| 3   | 2           | 200    |

Then from a PostgreSQL shell execute:
```sql
call pgf_count(
    'customer_order_count', -- id
    'customer',             -- base_table_name
    'id',                   -- base_pk
    'order_count',          -- base_count_column
    'order',                -- linked_table_name
    'customer_id'           -- linked_fk
);
```

After each change to the `order` table, `customer.order_count` is updated automatically:

| id  | name     | ⚡ order_count |
| --- | -------- | ------------- |
| 1   | John Doe | 2             |
| 2   | Jane Roe | 1             |




## MINMAX_TABLE formula
**_Create an aggregate table that computes, for each group of rows from a given table: the row count, the min and max values, the ID of min and max values_**

### Synax

```sql
PROCEDURE pgf_minmax_table (
	id text,
    table_name TEXT,
	pk TEXT,
    aggregate_column TEXT,
    options JSONB DEFAULT '{
        group_by_column: [],
        agg_table: null
    '}
)
```

| Argument                 | Description                                                                                    |
| ------------------------ | ---------------------------------------------------------------------------------------------- |
| ```id```                 | Id to identify this particular formula instance (must be unique across all declared formulas). |
| ```table_name```         | Name of the source table containing the data to be aggregated.                                 |
| ```pk```                 | Name of the primary key column name from the source table.                                     |
| ⚡ ```aggregate_column``` | name of the column from the source table containing the data to be aggregated.                 |
| ```options```            | Additional optional arguments, passed as a JSONB object (see available options below).         |

Additional options :
| JSONB field           | Default value                   | Description                                                                                                  |
| --------------------- | ------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| ```group_by_column``` | ```'[]'```                      | Allows grouping aggregated data according to the specified columns (similar to a ```GROUP BY``` expression). |
| ```agg_table```       | ```table_name \|\| '_minmax'``` | Name of the aggregate table to be created.                                                                   |

### Example

TODO

## INHERITANCE_TABLE formula
**_Merge multiple tables into one while keeping the base table and sub-tables synchronized._**

Synchronization between the base (union) table an sub-tables is unidirectional but can go any way (changes to the base table are propagated to sub-tables, or changes to sub-tables are propagated to the base table).

### Syntax
```sql
PROCEDURE pgf_inheritance_table (
    id TEXT,
    base_table_name TEXT,
    sub_tables TEXT[]
    sync_direction TEXT,
    options JSONB DEFAULT '{
        "discriminator_column": "discriminator"
        "discriminator_values": NULL
    }'::jsonb
)
```

| Argument              | Description                                                                                                                                                                                                              |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| ```id```              | Id to identify this particular formula instance (must be unique across all declared formulas).                                                                                                                           |
| ```base_table_name``` | Name of the base (union) table. This table is created automatically on function call.                                                                                                                                    |
| ```sub_tables```      | Name of tables to be kept in sync with the base table.                                                                                                                                                                   |
| ```sync_direction```  | Allowed values: ```'BASE_TO_SUB'``` to propagate changes unidirectionally from the base table to the sub-tables ; ```'SUB_TO_BASE'``` to propagate changes unidirectionally from the sub-tables table to the base table. |
| ```options```         | Additional optional arguments, passed as a JSONB object (see available options below).                                                                                                                                   |

Additional options :
| JSONB field                | Default value         | Description                                                                                                                                                                                                                                                   |
| -------------------------- | --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ```discriminator_column``` | ```'discriminator'``` | Name of the discriminator column, i.e the column from the base table that helps distinguish from which sub-table the row is from.                                                                                                                             |
| ```discriminator_values``` | Sub-table names       | Name of the discriminator values for each of the sub tables. The length of the array should be the same as the length of the ```sub_tables``` array, and the items should be in the same order. If not set, the discriminator values are the sub-table names. |


### Example
Given the following two tables ```car``` and ```truck```, we would like to synchronize data to a ```vehicle``` table cointaining data from both tables:

```car``` table:

| id          | model         | weight | num_doors | fuel_type |
| ----------- | ------------- | ------ | --------- | --------- |
| tesla_m3    | Tesla Model 3 | 1800   | 4         | electric  |
| mini_cooper | Mini Cooper S | 1300   | 2         | petrol    |

```truck``` table:
| id        | model      | weight | payload_kg | num_axles |
| --------- | ---------- | ------ | ---------- | --------- | 
| volvo_fh  | Volvo FH16 | 8000   | 25000      | 3         |
| ford_f750 | Ford F-750 | 6500   | 11000      | 2         |


From a Postgresql shell execute :
```sql
call pgf_inheritance_table('uvehicle', 'vehicle', ARRAY['bike', 'car'], 'BASE_To_SUB'); -- create the trigger
```

This will :
* Create the ```vehicle``` table containing columns from both ```bike``` and ```car``` tables, plus a discriminator column (named ```discriminator``` by default).

```vehicle``` table:

| discriminator | id          | model         | weight | num_doors  | fuel_type  | payload_kg | num_axles  |
| ------------- | ----------- | ------------- | ------ | ---------- | ---------- | ---------- | ---------- |
| car           | tesla_m3    | Tesla Model 3 | 1800   | 4          | electric   | ```NULL``` | ```NULL``` |
| car           | mini_cooper | Mini Cooper S | 1300   | 2          | petrol"    | ```NULL``` | ```NULL``` |
| truck         | volvo_fh    | Volvo FH16    | 8000   | ```NULL``` | ```NULL``` | 25000      | 3          |
| truck         | ford_f750   | Ford F-750    | 6500   | ```NULL``` | ```NULL``` | 11000      | 2          |

* Create the triggers to synchronize changes from the ```vehicle``` table to the ```bike``` and ```car``` tables;

> **NB**: to synchronize changes from ```bike``` and ```car``` to ```vehicle``` instead, use the argument 'SUB_TO_BASE'.

## AUDIT_TABLE formula
**_Populate a history (audit) table._**

This formula creates an audit table allowing to track insert, update and delete events across one or several tables.
Each row from the audit table corresponds to a single insert, update or delete operation on a single row.
The date of the row before and after the operation are both stored in the audit table.
### Syntax

```sql
PROCEDURE pgf_audit_table(
    id TEXT,
    audit_table_name TEXT,
    audited_table_names TEXT[],
    options JSONB DEFAULT '{
        "operation_column_name": "OPERATION",
        operations_mapping: {
            "INSERT": "INSERT"
            "UPDATE": "UPDATE"
            "DELETE": "DELETE"
        },
        "old_value_column_name": "OLD_VALUE",
        "new_value_column_name": "NEW_VALUE",
        "audited_operations": ["INSERT", "UPDATE", "DELETE"]
    }'::JSONB
)
```

| Argument                  | Description                                                                                    |
| ------------------------- | ---------------------------------------------------------------------------------------------- |
| ```id```                  | Id to identify this particular formula instance (must be unique across all declared formulas). |
| ```audited_table_names``` | Array of table names to audit (tracked tables).                                                |
| ```audit_table_name```    | Name of the table storing audit (tracking) events.                                             |
| ```options```             | Additional optional arguments, passed as a JSONB object (see available options below).         |

Additional options :
| JSONB field                 | Default value                          | Description                                                                                                                                                                                                       |
| --------------------------- | -------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ```operation_column_name``` | ```'operation'```                      | Name of column from the audit table storing the operation type (insert, update or delete).                                                                                                                        |
| ```operations_mapping```    |                                        | a JSONB sub-object allowing to remap operation type names. Default names are: INSERT, UPDATE, DELETE. To remap an operation name, add it to the JSONB object e.g ```{ "UPDATE": "This row has been updated" }```. |
| ```old_value_column_name``` | ```'OLD_VALUE'```                      | The name of the column containing the state of the row before the change event.                                                                                                                                   |
| ```new_value_column_name``` | ```'NEW_VALUE'```                      | The name of the column containing the state of the row after the change event.                                                                                                                                    |
| ```audited_operations```    | ```'["INSERT", "UPDATE", "DELETE"]'``` | The types of operations to audit.                                                                                                                                                                                 |

### Example

Consider a `users` table with columns `id`, `name`, and `email`:

**Initial setup:**
```sql
SELECT pgf_audit_table(
    'audit_users',
    'users_audit',
    ARRAY['users']::TEXT[]
);
```

**Step 1: INSERT a new user**
```sql
INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com');
```

Users table:
| id | name  | email              |
|----|-------|------------------|
| 1  | Alice | alice@example.com |

Users audit table:
| id | table_name | OPERATION | OLD_VALUE                              | NEW_VALUE                                    | event_time          |
|----|------------|-----------|----------------------------------------|----------------------------------------------|---------------------|
| 1  | users      | INSERT    | NULL                                   | {"id": 1, "name": "Alice", "email": "alice@example.com"} | 2024-01-15 10:00:00 |

**Step 2: UPDATE the user's email**
```sql
UPDATE users SET email = 'alice.new@example.com' WHERE id = 1;
```

Users table:
| id | name  | email                  |
|----|-------|------------------------|
| 1  | Alice | alice.new@example.com |

Users audit table (cumulative):
| id | table_name | OPERATION | OLD_VALUE                              | NEW_VALUE                                    | event_time          |
|----|------------|-----------|----------------------------------------|----------------------------------------------|---------------------|
| 1  | users      | INSERT    | NULL                                   | {"id": 1, "name": "Alice", "email": "alice@example.com"} | 2024-01-15 10:00:00 |
| 2  | users      | UPDATE    | {"id": 1, "name": "Alice", "email": "alice@example.com"} | {"id": 1, "name": "Alice", "email": "alice.new@example.com"} | 2024-01-15 10:05:00 |

**Step 3: DELETE the user**
```sql
DELETE FROM users WHERE id = 1;
```

Users table:
(empty)

Users audit table (cumulative):
| id | table_name | OPERATION | OLD_VALUE                              | NEW_VALUE                                    | event_time          |
|----|------------|-----------|----------------------------------------|----------------------------------------------|---------------------|
| 1  | users      | INSERT    | NULL                                   | {"id": 1, "name": "Alice", "email": "alice@example.com"} | 2024-01-15 10:00:00 |
| 2  | users      | UPDATE    | {"id": 1, "name": "Alice", "email": "alice@example.com"} | {"id": 1, "name": "Alice", "email": "alice.new@example.com"} | 2024-01-15 10:05:00 |
| 3  | users      | DELETE    | {"id": 1, "name": "Alice", "email": "alice.new@example.com"} | NULL                                   | 2024-01-15 10:10:00 |

## SYNC formula
**_Synchronize two fields from the same table row._**

### Syntax
```sql
PROCEDURE pgf_sync(
    id TEXT,
    table_name TEXT,
    column1 TEXT,
    column2 TEXT
);
```

Given the table table_name:
* For insert operations, when ```column1``` is set to a non-null value but not ```column2```, ```column2``` is set to ```column1```'s value as well. (and vice versa). When both columns are set, ```column1``` takes precedence : ```column2``` is set to ```column1```'s value.
* For update operations, when ```column1```'s value is modified but not ```column2```, ```column2``` is set to ```column1```'s value (and vice versa). In particular, if ```column1```'s value is modified to ```NULL```, ```column2```'s value is modified to ```NULL``` as well. (and vice versa). When both columns are modified, ```column1``` takes precedence : ```column2``` is set to ```column1```'s value.

This formula is particularly useful for renaming a column in zero-downtime migration scenarios, as it allows both the old version (which updates the original column) and the new version (which updates the renamed column) to run concurrently.



# Recipes
## Automatically update a 'creation_date' column.
A trigger is not required in this case. It is faster to use a default value for the creation date column:

```sql
CREATE TABLE my_table (
    id serial PRIMARY KEY,
    data text,
    creation_date timestamp DEFAULT now()
);
```

## Update a field that is the computation of other fields from the same row
A trigger is not required in this case. It can be achieved using GENERATED columns:

```sql
CREATE TABLE my_table (
    a int,
    b int,
    c int GENERATED ALWAYS AS (a + b) STORED
);
```

## Update a field that is the computation of other fields from joined tables
Retrive all inputs in the main table using the [JOIN](#JOIN) formula. Then, use a GENERATED column to compute the result.

## Archive rows instead of deleting them
Row archiving is useful for later inspection or restoration of deleted rows.
Two patterns can help acheive this:
* Pattern 1 : "Soft-delete": instead of deleting the row, set a ```soft_deleted``` row to ```true```. Be careful to always select rows with soft_deleted=false in subequent queries.
  This pattern can be further improved by partitioning data according to the soft_deleted column, to avoid any performance penalty from keeping deleted records.
  For instance:

```sql
CREATE TABLE my_table (
    id serial PRIMARY KEY,
    data text
    is_deleted boolean NOT NULL DEFAULT false,
) PARTITION BY LIST (is_deleted);
```

* Pattern 2 : call the ```AUDIT_TABLE``` formula while setting the ```options``` argument to ```{ audited_operations : ['DELETE'] }```. This will store all deleted in a dedicated table.

# Implementation details
A metadata table named ```pgf_metadata``` is created to track all formula declarations.

All objects (procedures, functions, triggers etc) starting with "_pgf_internal" are part of the internal implementation and should not be manipulated directly ; use public API instead.