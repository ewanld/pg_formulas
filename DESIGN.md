| Formula    | row_filter |
| ---------- | ---------- |
| SUM        | Y          |
| COUNT      | Y          |
| MIN        | Y          |
| MAX        | Y          |
| ID_OF_MIN  | Y          |
| ID_OF_MAX  | Y          |
| ARRAY_AGG  | Y          |
| STRING_AGG | Y          |


| Formula      | group_by         | row_count | min/max | id_of_min/max | sum | sum_of_squares | stddev/etc | row_filter |
| ------------ | ---------------- | --------- | ------- | ------------- | --- | -------------- | ---------- | ---------- |
| MINMAX_TABLE | Y (multi-column) | Y         | Y       | Y             | N   | N              | N          | Y (TODO)   |
| SUM_TABLE    | Y (multi-column) | Y         | N       | N             | Y   | Y              | Y          | Y          |
| COUNT_TABLE  | Y (multi-column) | Y         | N       | N             | N   | N              | N          | Y          |