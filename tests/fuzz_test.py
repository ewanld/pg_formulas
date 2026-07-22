from decimal import Decimal
import sys
from typing import Literal
import psycopg2
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from psycopg2.extras import execute_values
import time
from tests import settings
from tests.db_fuzzer import DbFuzzer, FuzzOptions
from tests.test_data_helper import TestDataHelper, TestDataStructure


class TestDbFuzzerModule(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Connect to your postgres DB
        cls.conn = psycopg2.connect(
            host=settings.DATABASE["host"],
            port=settings.DATABASE["port"],
            dbname=settings.DATABASE["name"],
            user=settings.DATABASE["user"],
            password=settings.DATABASE["password"],
        )

        # Open a cursor to perform database operations
        cls.cur = cls.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        schemaName = settings.DATABASE["schema"]
        cls.cur.execute(f'drop schema if exists {schemaName} cascade;')
        cls.cur.execute(f'create schema {schemaName};')
        cls.cur.execute(f'SET search_path TO {schemaName};')
        
        current_dir = Path(__file__).resolve().parent

        cls.execute_sql_file(current_dir / '../pg_formulas--0.9.sql')

        cls.cur.execute("commit;")
        
        cls.test_data_helper = TestDataHelper(cls.cur)

        cls.fuzzer = DbFuzzer(cls.conn, schemaName)

    @classmethod
    def execute_sql_file(cls, sql_file):
        with open(sql_file, 'r') as file:
            sql_commands = file.read()
        cls.cur.execute(sql_commands)
        

    @classmethod
    def tearDownClass(cls):
        cls.cur.execute("commit;")
        cls.cur.close()  # Close cursor
        cls.conn.close()  # Close the connection
    
    def create_tables(self, kind, id, create_formula=True):
        return self.test_data_helper.create_tables(kind, id, create_formula)
        
    def __test_kind(self, kind):
        formula_id = f"{kind}1"
        testDataStructure: TestDataStructure = self.create_tables(kind, formula_id)

        opts = FuzzOptions(testDataStructure.created_tables, testDataStructure.pgf_managed_object, 0, 1000, formula_id=formula_id)
        self.fuzzer.fuzz(opts)
    
    def test_revdate(self):
        self.__test_kind('revdate')
    def test_count(self):
        self.__test_kind('count')
    def test_minmax_table(self):
        self.__test_kind('minmax_table')
    def test_tree_level(self):
        self.__test_kind('tree_level')
    def test_inheritance_table(self):
        self.__test_kind('inheritance_table')
    def test_audit_table(self):
        self.__test_kind('audit_table')
    def test_sum(self):
        self.__test_kind('sum')
    def test_intersect_table(self):
        self.__test_kind('intersect_table')
    def test_union_table(self):
        self.__test_kind('union_table')
    def test_min(self):
        self.__test_kind('min')
    def test_max(self):
        self.__test_kind('max')
    def test_id_of_min(self):
        self.__test_kind('id_of_min')
    def test_array_agg(self):
        self.__test_kind('array_agg')