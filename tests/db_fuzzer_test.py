import unittest
import psycopg2
import psycopg2.extras
from pathlib import Path

from tests import settings
from tests.db_fuzzer import ColumnModel, DbFuzzer, FuzzOptions
from tests.test_data_helper import TestDataHelper

class TestModule(unittest.TestCase):
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

    @classmethod
    def execute_sql_file(cls, sql_file):
        with open(sql_file, 'r') as file:
            sql_commands = file.read()
        cls.cur.execute(sql_commands)
        

    def test_create_db_model(self):
        formula_id = 'customer_invoices_count'
        testDataStructure = self.test_data_helper.create_tables('count', formula_id)
        fuzzer = DbFuzzer(self.conn, settings.DATABASE["schema"])

        opts = FuzzOptions(testDataStructure.created_tables, testDataStructure.pgf_managed_object, 100, 1000, formula_id)
        db_model = fuzzer.create_db_model(opts)

        invoice_table: TableModel = db_model.get_table_by_name('invoice')
        customer_table: TableModel = db_model.get_table_by_name('customer')

        # foreign key assertions
        self.assertEqual(len(invoice_table.foreign_keys), 1)
        self.assertEqual(invoice_table.foreign_keys[0].columns, ['customer_id'])
        self.assertEqual(invoice_table.foreign_keys[0].parent_table, 'customer')
        self.assertEqual(invoice_table.foreign_keys[0].parent_columns, ['id'])

        self.assertEqual(customer_table.foreign_keys, [])

        # primary key assertions
        # expect single-column primary keys named 'id' on both tables
        self.assertEqual(invoice_table.pk, [ColumnModel('id', 'integer', False, False)])
        self.assertEqual(customer_table.pk, [ColumnModel('id', 'integer', False, False)])

        # other columns assertions
        self.assertEqual(customer_table.get_column_by_name('invoice_count'), ColumnModel('invoice_count', 'integer', True, True))
        self.assertEqual(customer_table.get_column_by_name('name'), ColumnModel('name', 'text', True, False))