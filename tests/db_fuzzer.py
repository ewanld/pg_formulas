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

class SqlOpertionType(Enum):
    insert = "insert"
    update = "update"
    delete = "delete"

class ColumnModel:
    def __init__(self, name: str, data_type: str, is_nullable=True):
        self.name = name
        self.data_type = data_type
        self.is_nullable = is_nullable

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
    def __init__(self, name, columns: list[ColumnModel], pk: list[ColumnModel], ids: list[TableId]):
        self.name = name
        self.columns = columns
        self.pk = pk
        pk_names = {col.name for col in pk}
        self.non_pk_columns = [col for col in columns if col.name not in pk_names]
        self.__ids_values_tuples = {self.convert_table_id_to_tuple(id) for id in ids}
    
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

    
    # generated id is added to the ids attribute.
    def generate_random_id(self) -> TableId:
        i = 0
        candidate: dict[str, Any] = {}
        while i < 10000:
            for col in self.pk:
                candidate[col.name] = DbFuzzer.generate_random_value(col.data_type)
            candidate_tableId = TableId(candidate)
            if not self.contains_id(candidate_tableId):
                self.__ids_values_tuples.add(self.convert_table_id_to_tuple(candidate_tableId))
                return candidate_tableId
            i+=1
        raise ValueError("Cannot generate random id after 10000 iterations")

    def is_empty(self):
        return len(self.__ids_values_tuples) == 0
    
class FuzzOptions:
    def __init__(self, table_names: list[str], iteration_count):
        self.table_names = table_names
        self.iteration_count = iteration_count

class DbFuzzer:
    def __init__(self, conn, schema_name):
        self.conn = conn

        # Open a cursor to perform database operations
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        self.cur.execute(f'SET search_path TO {schema_name}')
        self.test_data_helper = TestDataHelper(self.cur)


    @classmethod
    def generate_random_value(cls, data_type: str) -> Any:
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

    def introspect(self, table_names: list[str]) -> list[TableModel]:
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

            query = f"SELECT {pk_column_names} FROM {table_name}"
            self.cur.execute(query)
            ids_info = self.cur.fetchall()
            ids = [ TableId(i) for i in ids_info ]

            tables.append(TableModel(name=table_name, columns=columns, pk=pk_models, ids = ids))

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
    
    def fuzz_single_iteration(self, opts: FuzzOptions):
        table_models = self.introspect(opts.table_names)
        sql_op = choice(list(SqlOpertionType))
        table = choice(table_models)
        logger.info(f"Executing {sql_op.value} operation on table {table.name}")
        self.apply_sql_operation(sql_op, table)
        
    def apply_sql_operation(self, op_type: SqlOpertionType, table: TableModel):
        match(op_type):
            case SqlOpertionType.insert:
                id: TableId = table.generate_random_id()

                insert_values: dict[str, Any] = {}
                for col in table.non_pk_columns:
                    if not col.is_nullable or rng.random() > 0.3:
                        insert_values[col.name] = self.generate_random_value(col.data_type)
                self.DbInsertOperation(table, id, insert_values).apply(self.cur)
                
            case SqlOpertionType.update:
                if table.is_empty():
                    return
                update_id = table.select_random_id()
                update_values: dict[str, Any] = {}
                number_of_columns_to_update = 1 if rng.random() > 0.5 else randint(1, len(table.non_pk_columns))
                columns_to_update: list[ColumnModel] = random.sample(table.non_pk_columns, number_of_columns_to_update)

                for col in columns_to_update:
                    update_values[col.name] = self.generate_random_value(col.data_type)

                self.DbUpdateOperation(table, update_id, update_values).apply(self.cur)

            case SqlOpertionType.delete:
                if table.is_empty():
                    return
                delete_id = table.select_random_id()
                self.DbDeleteOperation(table, delete_id).apply(self.cur)