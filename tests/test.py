from decimal import Decimal
import psycopg2
import psycopg2.extras
import unittest
import settings
from datetime import datetime, timedelta
from pathlib import Path

class TestModule(unittest.TestCase):
    def setUp(self):
        # Connect to your postgres DB
        self.conn = psycopg2.connect(
            host=settings.DATABASE["host"],
            port=settings.DATABASE["port"],
            dbname=settings.DATABASE["name"],
            user=settings.DATABASE["user"],
            password=settings.DATABASE["password"],
        )

        # Open a cursor to perform database operations
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        schemaName = settings.DATABASE["schema"]
        self.cur.execute(f'SET search_path TO {schemaName}')
        
        current_dir = Path(__file__).resolve().parent

        self.execute_sql_file(current_dir / '../pg_reactive_toolbox.sql')

    def execute_sql_file(self, sql_file):
        with open(sql_file, 'r') as file:
            sql_commands = file.read()
        self.cur.execute(sql_commands)
        

    def tearDown(self):
        self.cur.close()  # Close cursor
        self.conn.close()  # Close the connection
    

    # assert that the sql query returns a single row containing a single scalar equal to expected value.
    def assert_sql_equal(self, sql, expected):
        self.cur.execute(sql)
        result = self.cur.fetchone()
        self.assertEqual(list(result.values())[0], expected)

    def testREVDATE(self):
        self.cur.execute("drop table if exists customer cascade;");
        self.cur.execute("create table customer (id SERIAL PRIMARY KEY, name text, last_modified timestamp default null);");

        func_id="customer_last_modified"

        self.cur.execute(f"call REVDATE_create('{func_id}', 'customer', 'last_modified')")
        self.cur.execute("insert into customer(name) values('Cust1')")
        self.cur.execute("insert into customer(name) values('Cust2')")
        
        # test 1: check that and check on insert the last_modified field is set to the current time
        self.cur.execute("select * from customer;")
        records = self.cur.fetchall()
        self.assertEqual(len(records), 2)
        for record in records:
            last_modified = record["last_modified"]
            self.assertTrue(datetime.now() - last_modified < timedelta(seconds=10));
        
        # test 2: check that disable works
        self.cur.execute(f"call REVDATE_disable('{func_id}', 'customer')")
        self.cur.execute("insert into customer(name) values('Cust3')")
        self.cur.execute("select * from customer where name='Cust3';")
        record = self.cur.fetchone()
        last_modified = record["last_modified"]
        self.assertIsNone(last_modified)

        # test 3: check that enable+update works
        self.cur.execute(f"call REVDATE_enable('{func_id}', 'customer')")
        self.cur.execute("update customer set name='Cust4' where name='Cust3'")
        self.cur.execute("select * from customer where name='Cust4';")
        record = self.cur.fetchone()
        last_modified = record["last_modified"]
        self.assertTrue(datetime.now() - last_modified < timedelta(seconds=10));

        # to make test data available for inspection after the test
        self.conn.commit()

    # data model: customer -1-N-> invoice
    def testCOUNTLNK(self):
        self.cur.execute("drop table if exists invoice cascade;");
        self.cur.execute("drop table if exists customer cascade;");

        self.cur.execute("create table customer (id int PRIMARY KEY, name text, invoice_count int default 0);")
        self.cur.execute("create table invoice(id int PRIMARY KEY, name text, customer_id int references customer(id));")

        func_id = 'customer_invoices_count'

        self.cur.execute(f"call COUNTLNK_create('{func_id}', 'customer', 'id', 'invoice_count', 'invoice', 'customer_id');")
        self.cur.execute("commit;")

        # test 1 : insert invoices
        self.cur.execute("insert into customer(id, name) values(1, 'customer A'), (2, 'customer B');")
        self.cur.execute("insert into invoice (id, name, customer_id) values(1, 'invoice 1', 1), (2, 'invoice 2', 1), (3, 'invoice 3', 2);")
        self.cur.execute("commit;")

        self.assert_sql_equal("select invoice_count from customer where id=1;", 2)
        self.assert_sql_equal("select invoice_count from customer where id=2;", 1)

        # test 2 : delete invoices
        self.cur.execute("delete from invoice where id in (1, 3);")
        self.assert_sql_equal("select invoice_count from customer where id=1;", 1)
        self.assert_sql_equal("select invoice_count from customer where id=2;", 0)

        # test 3 : manual refresh
        self.cur.execute("update customer set invoice_count=0;")
        self.cur.execute(f"call COUNTLNK_refresh('{func_id}');")
        self.assert_sql_equal("select invoice_count from customer where id=1;", 1)
        self.assert_sql_equal("select invoice_count from customer where id=2;", 0)

        # test 4 : truncate table
        self.cur.execute("truncate table invoice;")
        self.assert_sql_equal("select invoice_count from customer where id=1;", 0)
        self.assert_sql_equal("select invoice_count from customer where id=2;", 0)

        # test 5 : delete whole table
        self.cur.execute("insert into invoice (id, name, customer_id) values(1, 'invoice 1', 1), (2, 'invoice 2', 1), (3, 'invoice 3', 2);")
        self.cur.execute("delete from invoice;")
        self.assert_sql_equal("select invoice_count from customer where id=1;", 0)
        self.assert_sql_equal("select invoice_count from customer where id=2;", 0)

        # test 6 : update invoice customer, verify that counts are OK
        self.cur.execute("insert into invoice (id, name, customer_id) values(1, 'invoice 1', 1), (2, 'invoice 2', 1), (3, 'invoice 3', 2);")
        self.assert_sql_equal("select invoice_count from customer where id=1;", 2)
        self.assert_sql_equal("select invoice_count from customer where id=2;", 1)
        
        self.cur.execute("update invoice set customer_id=2 where id=2")
        self.assert_sql_equal("select invoice_count from customer where id=1;", 1)
        self.assert_sql_equal("select invoice_count from customer where id=2;", 2)
        
        self.cur.execute("update invoice set customer_id=1")
        self.assert_sql_equal("select invoice_count from customer where id=1;", 3)
        self.assert_sql_equal("select invoice_count from customer where id=2;", 0)

        # clean up
        # self.cur.execute("drop table if exists invoice cascade;");
        # self.cur.execute("drop table if exists customer cascade;");

    def testAGG(self):
        func_id = 'customer_invoices_agg'
        agg_table = 'agg';

        self.cur.execute("drop table if exists customer cascade;");
        self.cur.execute("drop table if exists invoice cascade;");
        self.cur.execute(f"drop table if exists {agg_table} cascade;");

        self.cur.execute("create table invoice(id int PRIMARY KEY, name text, customer_id int, country text, amount NUMERIC(10, 2));")

        

        self.cur.execute(f"call AGG_create('{func_id}', 'invoice', 'id', 'amount', ARRAY['customer_id', 'country'], '{agg_table}');")
        self.cur.execute("commit;")

        # test 1 : insert invoices
        self.cur.execute("insert into invoice (id, name, customer_id, country, amount) values"
        "(1, 'invoice 1', 1, 'FR', 5.5)," \
        "(2, 'invoice 2', 1, 'US', 6.6)," \
        "(3, 'invoice 3', 2, 'FR', 2.2)," \
        "(4, 'invoice 4', 2, 'FR', 3.3)" \
        ";")
        self.cur.execute("commit;")
        self.cur.execute(f"select * from {agg_table} where customer_id=1 and country='FR'")
        record = self.cur.fetchone()
        self.assertEqual(record['min_value'], Decimal('5.50'))
        self.assertEqual(record['id_of_min'], 1)
        self.assertEqual(record['max_value'], Decimal('5.50'))
        self.assertEqual(record['id_of_max'], 1)
        self.assertEqual(record['row_count'], 1)

        self.cur.execute(f"select * from {agg_table} where customer_id=2 and country='FR'")
        record = self.cur.fetchone()
        self.assertEqual(record['min_value'], Decimal('2.20'))
        self.assertEqual(record['id_of_min'], 3)
        self.assertEqual(record['max_value'], Decimal('3.30'))
        self.assertEqual(record['id_of_max'], 4)
        self.assertEqual(record['row_count'], 2)

        # test : insert invoice with new grouping key
        self.cur.execute("insert into invoice (id, name, customer_id, country, amount) values" \
        "(5, 'invoice 5', 3, 'FR', 5.5);")
        self.cur.execute(f"select * from {agg_table} where customer_id=3 and country='FR'")
        record = self.cur.fetchone()
        self.assertEqual(record['min_value'], Decimal('5.50'))
        self.assertEqual(record['id_of_min'], 5)
        self.assertEqual(record['max_value'], Decimal('5.50'))
        self.assertEqual(record['id_of_max'], 5)
        self.assertEqual(record['row_count'], 1)

        # test : insert invoice with existing grouping key, amount is min
        self.cur.execute("insert into invoice (id, name, customer_id, country, amount) values" \
        "(6, 'invoice 6', 2, 'FR', 1.1);")
        self.cur.execute(f"select * from {agg_table} where customer_id=2 and country='FR'")
        record = self.cur.fetchone()
        self.assertEqual(record['min_value'], Decimal('1.10'))
        self.assertEqual(record['id_of_min'], 6)
        self.assertEqual(record['max_value'], Decimal('3.30'))
        self.assertEqual(record['id_of_max'], 4)
        self.assertEqual(record['row_count'], 3)

        # test : insert invoice with existing grouping key, amount is max
        self.cur.execute("insert into invoice (id, name, customer_id, country, amount) values" \
        "(7, 'invoice 7', 2, 'FR', 7.7);")
        self.cur.execute(f"select * from {agg_table} where customer_id=2 and country='FR'")
        record = self.cur.fetchone()
        self.assertEqual(record['min_value'], Decimal('1.10'))
        self.assertEqual(record['id_of_min'], 6)
        self.assertEqual(record['max_value'], Decimal('7.70'))
        self.assertEqual(record['id_of_max'], 7)
        self.assertEqual(record['row_count'], 4)

        # test : insert invoice with existing grouping key, amount is inbetween (neither min nor max)
        self.cur.execute("insert into invoice (id, name, customer_id, country, amount) values" \
        "(8, 'invoice 8', 2, 'FR', 5.5);")
        self.cur.execute(f"select * from {agg_table} where customer_id=2 and country='FR'")
        record = self.cur.fetchone()
        self.assertEqual(record['min_value'], Decimal('1.10'))
        self.assertEqual(record['id_of_min'], 6)
        self.assertEqual(record['max_value'], Decimal('7.70'))
        self.assertEqual(record['id_of_max'], 7)
        self.assertEqual(record['row_count'], 5)

        # test : delete invoice, amount is inbetween (neither min nor max)
        self.cur.execute("delete from invoice where id=8")
        self.cur.execute(f"select * from {agg_table} where customer_id=2 and country='FR'")
        record = self.cur.fetchone()
        self.assertEqual(record['min_value'], Decimal('1.10'))
        self.assertEqual(record['id_of_min'], 6)
        self.assertEqual(record['max_value'], Decimal('7.70'))
        self.assertEqual(record['id_of_max'], 7)
        self.assertEqual(record['row_count'], 4)

        # test : delete invoice, id is min id (neither min nor max)
        self.cur.execute("delete from invoice where id=8")
        self.cur.execute(f"select * from {agg_table} where customer_id=2 and country='FR'")
        record = self.cur.fetchone()
        self.assertEqual(record['min_value'], Decimal('1.10'))
        self.assertEqual(record['id_of_min'], 6)
        self.assertEqual(record['max_value'], Decimal('7.70'))
        self.assertEqual(record['id_of_max'], 7)
        self.assertEqual(record['row_count'], 4)

if __name__ == '__main__':
    unittest.main()

