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
    
    # insert two records into the customer table   
    # and check that the last_modified field is set to the current time
    def testREVDATE(self):
        self.cur.execute("drop table if exists customer;");
        self.cur.execute("create table customer (id SERIAL PRIMARY KEY, name text, last_modified timestamp default null);");

        self.cur.execute("call REVDATE_create('customer_last_modified', 'customer', 'last_modified')")
        self.cur.execute("insert into customer(name) values('Cust1')")
        self.cur.execute("insert into customer(name) values('Cust2')")
        self.conn.commit()

        self.cur.execute("select * from customer;")
        records = self.cur.fetchall()
        self.assertEqual(len(records), 2)
        for record in records:
            last_modified = record["last_modified"]
            self.assertTrue(datetime.now() - last_modified < timedelta(seconds=10));
        


if __name__ == '__main__':
    unittest.main()

