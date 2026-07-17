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
        
    def test_count(self):
        testDataStructure: TestDataStructure = self.create_tables('count', 'count_formula1')
        # opts = FuzzOptions(['customer'], testDataStructure.pgf_managed_object, 100, 100)
        opts = FuzzOptions(testDataStructure.created_tables, testDataStructure.pgf_managed_object, 100, 10000)
        self.fuzzer.fuzz(opts)