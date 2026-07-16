from abc import ABC, abstractmethod
from enum import Enum
from random import Random, choice, randint
import random
from typing import Any, Optional
import logging

import psycopg2
import pprint

from tests.test_data_helper import TestDataHelper

# Configure logging to show output
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(name)s - %(message)s'
)

logger = logging.getLogger(__name__)

rng = Random()

class SqlOperationType(Enum):
    insert = "insert"
    update = "update"
    delete = "delete"

class ColumnModel:
    def __init__(self, name: str, data_type: str, is_nullable=True, pgf_managed=False):
        self.name = name
        self.data_type = data_type
        self.is_nullable = is_nullable
        self.pgf_managed = pgf_managed

    def __repr__(self) -> str:
        return (
            f"ColumnModel(name={self.name!r}, data_type={self.data_type!r}, "
            f"is_nullable={self.is_nullable!r}, pgf_managed={self.pgf_managed!r})"
        )

    __str__ = __repr__

    def __eq__(self, other):
        """Compare by content, not memory address"""
        if not isinstance(other, ColumnModel):
            return False
        return self.name == other.name and self.data_type == other.data_type and self.is_nullable == other.is_nullable and self.pgf_managed == other.pgf_managed
    

class ForeignKeyModel:
    def __init__(self, name: str, columns: list[str], referenced_table: str, referenced_columns: list[str]):
        self.name = name
        self.columns = columns  # list of column names in this table
        self.referenced_table = referenced_table
        self.referenced_columns = referenced_columns  # list of column names in referenced table

class TableId:
    def __init__(self, values: dict[str, Any]):
        # runtime validation: ensure values is a dict with string keys
        if not isinstance(values, dict):
            raise TypeError("values must be a dict[str, Any]")
        for k in values.keys():
            if not isinstance(k, str):
                raise TypeError("all keys in values must be of type str")

        self.values = values
    

class TableModel:
    def __init__(self, name, columns: list[ColumnModel], pk: list[ColumnModel], ids: list[TableId], foreign_keys: list[ForeignKeyModel] | None = None, pgf_managed=False):
        self.name = name
        self.columns = columns
        self.pk = pk
        self.foreign_keys = foreign_keys or []
        self.__ids_values_tuples = {self.convert_table_id_to_tuple(id) for id in ids}
        self.pgf_managed=pgf_managed
        self.refresh()
    
    # manual refresh after a ColumnModel has changed
    def refresh(self):
        pk_names = {col.name for col in self.pk}
        self.non_pk_columns = [col for col in self.columns if col.name not in pk_names and not col.pgf_managed]

    def convert_table_id_to_tuple(self, id: TableId):
        return tuple(id.values[col.name] for col in self.pk)
    
    def convert_tuple_to_table_id(self, id):
        if not isinstance(id, tuple):
            raise TypeError("Expected a tuple of primary key values")

        return TableId({col.name: value for col, value in zip(self.pk, id)})

    def contains_id(self, id: TableId):
        return self.convert_table_id_to_tuple(id) in self.__ids_values_tuples

    def select_random_id(self):
        if self.is_empty():
            raise ValueError("Table is empty")
        return self.convert_tuple_to_table_id(rng.choice(list(self.__ids_values_tuples)))

    def remove_id(self, id: TableId) -> None:
        """Remove a TableId from internal set if present."""
        tup = self.convert_table_id_to_tuple(id)
        self.__ids_values_tuples.discard(tup)

    def get_column_by_name(self, name: str) -> Optional[ColumnModel]:
        return next((c for c in self.columns if c.name == name), None)
    
    # generated id is added to the ids attribute.
    def generate_random_id(self) -> TableId:
        i = 0
        candidate: dict[str, Any] = {}
        while i < 10000:
            for col in self.pk:
                candidate[col.name] = DbFuzzer.generate_random_value(col)
            candidate_tableId = TableId(candidate)
            if not self.contains_id(candidate_tableId):
                self.__ids_values_tuples.add(self.convert_table_id_to_tuple(candidate_tableId))
                return candidate_tableId
            i+=1
        raise ValueError("Cannot generate random id after 10000 iterations")

    def is_empty(self):
        return len(self.__ids_values_tuples) == 0
    
class FuzzOptions:
    def __init__(self, table_names: list[str], pgf_managed_object: str, iteration_count: int):
        self.table_names = table_names
        self.pgf_managed_object = pgf_managed_object
        self.iteration_count = iteration_count

class DbFuzzer:
    def __init__(self, conn, schema_name):
        self.conn = conn

        # Open a cursor to perform database operations
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        self.cur.execute(f'SET search_path TO {schema_name}')
        self.test_data_helper = TestDataHelper(self.cur)


    @classmethod
    def generate_random_value(cls, column: ColumnModel) -> Any:
        data_type = column.data_type
        
        # 30% of values are NULL
        if column.is_nullable and rng.random() < 0.3:
            return None
        
        normalized = data_type.lower()

        if normalized in {
            'integer', 'int', 'int4',
            'smallint', 'int2',
            'bigint', 'int8',
            'serial', 'bigserial', 'smallserial'
        }:
            return randint(1, 1000)

        if normalized in {'numeric', 'decimal'}:
            decimal = __import__('decimal')
            return decimal.Decimal(f"{randint(0, 1000)}.{randint(0, 99):02d}")

        if normalized in {'real', 'double precision', 'float4', 'float8'}:
            return float(randint(0, 1000) + randint(0, 99) / 100)

        if normalized in {'boolean', 'bool'}:
            return choice([True, False])

        if normalized in {'text', 'character varying', 'varchar', 'character', 'char'}:
            chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
            return ''.join(choice(chars) for _ in range(10))

        if normalized == 'date':
            dt = __import__('datetime')
            start = dt.date(2000, 1, 1)
            return start + dt.timedelta(days=randint(0, 3650))

        if normalized in {'timestamp without time zone', 'timestamp'}:
            dt = __import__('datetime')
            start = dt.datetime(2000, 1, 1, 0, 0, 0)
            return start + dt.timedelta(seconds=randint(0, 10 * 365 * 24 * 60 * 60))

        if normalized == 'timestamp with time zone':
            dt = __import__('datetime')
            start = dt.datetime(2000, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc)
            return start + dt.timedelta(seconds=randint(0, 10 * 365 * 24 * 60 * 60))

        if normalized == 'uuid':
            return str(__import__('uuid').uuid4())

        if normalized in {'json', 'jsonb'}:
            return __import__('json').dumps({'value': randint(1, 1000), 'active': choice([True, False])})

        if normalized == 'bytea':
            return bytes(randint(0, 255) for _ in range(10))

        raise ValueError(f"Unknown data type : {data_type}")

    def __introspect(self, table_names: list[str]) -> list[TableModel]:
        tables = []
        for table_name in table_names:
            query = """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = %s
                ORDER BY ordinal_position
            """
            self.cur.execute(query, (table_name,))
            columns_info = self.cur.fetchall()
            
            columns = [
                ColumnModel(
                    name=col['column_name'],
                    data_type=col['data_type'],
                    is_nullable=(col['is_nullable'].lower() == 'yes')
                )
                for col in columns_info
            ]
            # fetch primary key columns for this table
            query = """
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                    WHERE tc.constraint_type = 'PRIMARY KEY'
                        AND tc.table_schema = current_schema()
                        AND tc.table_name = %s
                    ORDER BY kcu.ordinal_position
            """
            self.cur.execute(query, (table_name,))
            cols_info = self.cur.fetchall()
            col_names = [r['column_name'] for r in cols_info]

            # map ColumnModel instances that are part of the primary key
            pk_models = [c for c in columns if c.name in col_names]
            pk_column_names = ', '.join(c.name for c in pk_models)

            fk_query = """
                SELECT tc.constraint_name,
                       kcu.column_name,
                       ccu.table_name AS foreign_table_name,
                       ccu.column_name AS foreign_column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage ccu
                    ON tc.constraint_name = ccu.constraint_name
                    AND tc.table_schema = ccu.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_schema = current_schema()
                    AND tc.table_name = %s
                ORDER BY tc.constraint_name, kcu.ordinal_position
            """
            self.cur.execute(fk_query, (table_name,))
            fk_rows = self.cur.fetchall()

            foreign_keys = []
            if fk_rows:
                grouped_rows: dict[str, list[dict[str, Any]]] = {}
                for row in fk_rows:
                    grouped_rows.setdefault(row['constraint_name'], []).append(row)

                for constraint_name, rows in grouped_rows.items():
                    foreign_keys.append(ForeignKeyModel(
                        name=constraint_name,
                        columns=[row['column_name'] for row in rows],
                        referenced_table=rows[0]['foreign_table_name'],
                        referenced_columns=[row['foreign_column_name'] for row in rows],
                    ))

            query = f"SELECT {pk_column_names} FROM {table_name}"
            self.cur.execute(query)
            ids_info = self.cur.fetchall()
            ids = [ TableId(i) for i in ids_info ]

            tables.append(TableModel(name=table_name, columns=columns, pk=pk_models, ids=ids, foreign_keys=foreign_keys))

        return tables

    class DbOperation(ABC):
        @abstractmethod
        def apply(self, cur):
            pass

    class DbInsertOperation(DbOperation):
        def __init__(self, table: TableModel, id: TableId, values: dict[str, Any]):
            self.table = table
            self.id = id
            self.values = values

        def apply(self, cur):
            all_values: dict[str, Any] = self.id.values | self.values
            logger.info(f"INSERT into {self.table.name} with values: {all_values}")
            columns = ', '.join(all_values.keys())
            placeholders = ', '.join(['%s'] * len(all_values))
            query = f"INSERT INTO {self.table.name} ({columns}) VALUES ({placeholders})"
            cur.execute(query, list(all_values.values()))
    
    class DbDeleteOperation(DbOperation):
        def __init__(self, table: TableModel, id: TableId):
            self.table = table
            self.id = id

        def apply(self, cur):
            if not self.table.pk:
                raise ValueError(f"Cannot delete from table {self.table.name} without a primary key")

            pk_columns = [col.name for col in self.table.pk]
            where_clause = ' AND '.join(f"{col} = %s" for col in pk_columns)
            query = f"DELETE FROM {self.table.name} WHERE {where_clause}"

            values = [self.id.values[col] for col in pk_columns]
            logger.info(f"DELETE from {self.table.name} with id: {self.id.values}")
            cur.execute(query, values)
            
            # Remove the deleted ID from table.ids
            self.table.remove_id(self.id)
            logger.info(f"Removed ID from {self.table.name}: {self.id.values}")
    
    # values should not contain PK columns
    class DbUpdateOperation(DbOperation):
        def __init__(self, table: TableModel, id: TableId, values: dict[str, Any]):
            self.table = table
            self.id = id
            self.values = values

        def apply(self, cur):
            if not self.table.pk:
                raise ValueError(f"Cannot update table {self.table.name} without a primary key")

            pk_columns = [col.name for col in self.table.pk]
            set_clause = ', '.join(f"{col} = %s" for col in self.values.keys())
            where_clause = ' AND '.join(f"{col} = %s" for col in pk_columns)
            query = f"UPDATE {self.table.name} SET {set_clause} WHERE {where_clause}"

            values = list(self.values.values()) + [self.id.values[col] for col in pk_columns]
            logger.info(f"UPDATE {self.table.name} with id: {self.id.values}, values: {self.values}")
            cur.execute(query, values)


    def fuzz(self, opts : FuzzOptions) -> None:
        for i in range(opts.iteration_count):
            self.fuzz_single_iteration(opts)
    
    def create_db_model(self, opts: FuzzOptions) -> list[TableModel]:
        table_models = self.__introspect(opts.table_names)

        # apply pgf_managed_object to TableModel
        if '.' in opts.pgf_managed_object:
            pgf_managed_table_name = opts.pgf_managed_object.split('.')[0]
            pgf_managed_column_name = opts.pgf_managed_object.split('.')[1]
        else:
            pgf_managed_table_name = opts.pgf_managed_object
            pgf_managed_column_name = None

        pgf_managed_table = next((model for model in table_models if model.name == pgf_managed_table_name), None)
        if pgf_managed_table is None:
            raise ValueError(f"No table model found for managed object {opts.pgf_managed_object}")
        
        pgf_managed_table.pgf_managed = True
        if pgf_managed_column_name:
            pgf_managed_column = pgf_managed_table.get_column_by_name(pgf_managed_column_name)
            if pgf_managed_column is None:
                raise ValueError(f"No column model found for managed object {opts.pgf_managed_object}")
            pgf_managed_column.pgf_managed = True

        for t in table_models:
            t.refresh()
        return table_models

    def fuzz_single_iteration(self, opts: FuzzOptions):
        table_models = self.create_db_model(opts)
        
        sql_op = choice(list(SqlOperationType))
        table = choice(table_models)
        logger.info(f"Executing {sql_op.value} operation on table {table.name}")
        self.apply_sql_operation(sql_op, table)
        
    def apply_sql_operation(self, op_type: SqlOperationType, table: TableModel):
        match(op_type):
            case SqlOperationType.insert:
                id: TableId = table.generate_random_id()

                insert_values: dict[str, Any] = {}
                for col in table.non_pk_columns:
                    insert_values[col.name] = self.generate_random_value(col)
                self.DbInsertOperation(table, id, insert_values).apply(self.cur)
                
            case SqlOperationType.update:
                if table.is_empty():
                    return
                update_id = table.select_random_id()
                update_values: dict[str, Any] = {}
                number_of_columns_to_update = 1 if rng.random() > 0.5 else randint(1, len(table.non_pk_columns))
                columns_to_update: list[ColumnModel] = random.sample(table.non_pk_columns, number_of_columns_to_update)

                for col in columns_to_update:
                    update_values[col.name] = self.generate_random_value(col)

                self.DbUpdateOperation(table, update_id, update_values).apply(self.cur)

            case SqlOperationType.delete:
                if table.is_empty():
                    return
                delete_id = table.select_random_id()
                self.DbDeleteOperation(table, delete_id).apply(self.cur)