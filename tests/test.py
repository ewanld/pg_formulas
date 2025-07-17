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
    
    def testREVDATE(self):
        self.cur.execute("drop table if exists customer;");
        self.cur.execute("create table customer (id SERIAL PRIMARY KEY, name text, last_modified timestamp default null);");

        func_id="customer_last_modified"

        self.cur.execute(f"call REVDATE_create('{func_id}', 'customer', 'last_modified')")
        self.cur.execute("insert into customer(name) values('Cust1')")
        self.cur.execute("insert into customer(name) values('Cust2')")
        
        # step 1: check that and check on insert the last_modified field is set to the current time
        self.cur.execute("select * from customer;")
        records = self.cur.fetchall()
        self.assertEqual(len(records), 2)
        for record in records:
            last_modified = record["last_modified"]
            self.assertTrue(datetime.now() - last_modified < timedelta(seconds=10));
        
        # step 2: check that disable works
        self.cur.execute(f"call REVDATE_disable('{func_id}', 'customer')")
        self.cur.execute("insert into customer(name) values('Cust3')")
        self.cur.execute("select * from customer where name='Cust3';")
        record = self.cur.fetchone()
        last_modified = record["last_modified"]
        self.assertIsNone(last_modified)

        # step 3: check that enable+update works
        self.cur.execute(f"call REVDATE_enable('{func_id}', 'customer')")
        self.cur.execute("update customer set name='Cust4' where name='Cust3'")
        self.cur.execute("select * from customer where name='Cust4';")
        record = self.cur.fetchone()
        last_modified = record["last_modified"]
        self.assertTrue(datetime.now() - last_modified < timedelta(seconds=10));

        # to make test data available for inspection after the test
        self.conn.commit()


if __name__ == '__main__':
    unittest.main()

