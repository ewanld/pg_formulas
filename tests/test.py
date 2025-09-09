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

        self.execute_sql_file(current_dir / '../pg_reactive_toolbox--1.0.sql')
        self.cur.execute('drop table if exists pgrt_metadata;')

    def execute_sql_file(self, sql_file):
        with open(sql_file, 'r') as file:
            sql_commands = file.read()
        self.cur.execute(sql_commands)
        

    def tearDown(self):
        self.cur.execute("commit;")
        self.cur.close()  # Close cursor
        self.conn.close()  # Close the connection
    

    # assert that the sql query returns a single row containing a single scalar equal to expected value.
    def assert_sql_equal(self, sql, expected, params=()):
        self.cur.execute(sql, params)
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
        self.cur.execute(f"call REVDATE_disable(%s)", (func_id,))
        self.cur.execute("insert into customer(name) values('Cust3')")
        record = self.fetch_one("select * from customer where name='Cust3';")
        last_modified = record["last_modified"]
        self.assertIsNone(last_modified)

        # test 3: check that enable+update works
        self.cur.execute(f"call REVDATE_enable(%s)", (func_id,))
        self.cur.execute("update customer set name='Cust4' where name='Cust3'")
        record = self.fetch_one("select * from customer where name='Cust4';")
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

        self.cur.execute(f"call COUNTLNK_create(%s, 'customer', 'id', 'invoice_count', 'invoice', 'customer_id');", (func_id,))

        # test 1 : insert invoices
        self.cur.execute("insert into customer(id, name) values(1, 'customer A'), (2, 'customer B');")
        self.cur.execute("insert into invoice (id, name, customer_id) values(1, 'invoice 1', 1), (2, 'invoice 2', 1), (3, 'invoice 3', 2);")

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

    def fetch_one(self, sql):
        self.cur.execute(sql)
        return self.cur.fetchone()

    def testAGG(self):
        func_id = 'customer_invoices_agg'

        self.cur.execute("drop table if exists customer cascade;");
        self.cur.execute("drop table if exists invoice cascade;");
        self.cur.execute(f"drop table if exists agg cascade;");

        self.cur.execute("create table invoice(id int PRIMARY KEY, name text, customer_id int, country text, amount NUMERIC(10, 2));")

        self.cur.execute(f"call AGG_create('{func_id}', 'invoice', 'id', 'amount', ARRAY['customer_id', 'country'], 'agg');")

        # test 1 : insert invoices
        self.cur.execute("insert into invoice (id, name, customer_id, country, amount) values"
        "(1, 'invoice 1', 1, 'FR', 5.5)," \
        "(2, 'invoice 2', 1, 'US', 6.6)," \
        "(3, 'invoice 3', 2, 'FR', 2.2)," \
        "(4, 'invoice 4', 2, 'FR', 3.3)" \
        ";")
        record = self.fetch_one(f"select * from agg where customer_id=1 and country='FR'")
        self.assertEqual(record['min_value'], Decimal('5.50'))
        self.assertEqual(record['id_of_min'], 1)
        self.assertEqual(record['max_value'], Decimal('5.50'))
        self.assertEqual(record['id_of_max'], 1)
        self.assertEqual(record['row_count'], 1)

        record = self.fetch_one(f"select * from agg where customer_id=2 and country='FR'")
        self.assertEqual(record['min_value'], Decimal('2.20'))
        self.assertEqual(record['id_of_min'], 3)
        self.assertEqual(record['max_value'], Decimal('3.30'))
        self.assertEqual(record['id_of_max'], 4)
        self.assertEqual(record['row_count'], 2)

        # test : insert invoice with new grouping key
        self.cur.execute("insert into invoice (id, name, customer_id, country, amount) values" \
        "(5, 'invoice 5', 3, 'FR', 5.5);")
        record = self.fetch_one(f"select * from agg where customer_id=3 and country='FR'")
        self.assertEqual(record['min_value'], Decimal('5.50'))
        self.assertEqual(record['id_of_min'], 5)
        self.assertEqual(record['max_value'], Decimal('5.50'))
        self.assertEqual(record['id_of_max'], 5)
        self.assertEqual(record['row_count'], 1)

        # test : insert invoice with existing grouping key, amount is min
        self.cur.execute("insert into invoice (id, name, customer_id, country, amount) values" \
        "(6, 'invoice 6', 2, 'FR', 1.1);")
        record = self.fetch_one(f"select * from agg where customer_id=2 and country='FR'")
        self.assertEqual(record['min_value'], Decimal('1.10'))
        self.assertEqual(record['id_of_min'], 6)
        self.assertEqual(record['max_value'], Decimal('3.30'))
        self.assertEqual(record['id_of_max'], 4)
        self.assertEqual(record['row_count'], 3)

        # test : insert invoice with existing grouping key, amount is max
        self.cur.execute("insert into invoice (id, name, customer_id, country, amount) values" \
        "(7, 'invoice 7', 2, 'FR', 7.7);")
        record = self.fetch_one(f"select * from agg where customer_id=2 and country='FR'")
        self.assertEqual(record['min_value'], Decimal('1.10'))
        self.assertEqual(record['id_of_min'], 6)
        self.assertEqual(record['max_value'], Decimal('7.70'))
        self.assertEqual(record['id_of_max'], 7)
        self.assertEqual(record['row_count'], 4)

        # test : insert invoice with existing grouping key, amount is inbetween (neither min nor max)
        self.cur.execute("insert into invoice (id, name, customer_id, country, amount) values" \
        "(8, 'invoice 8', 2, 'FR', 5.5);")
        record = self.fetch_one(f"select * from agg where customer_id=2 and country='FR'")
        self.assertEqual(record['min_value'], Decimal('1.10'))
        self.assertEqual(record['id_of_min'], 6)
        self.assertEqual(record['max_value'], Decimal('7.70'))
        self.assertEqual(record['id_of_max'], 7)
        self.assertEqual(record['row_count'], 5)

        # test : delete invoice, amount is inbetween (neither min nor max)
        self.cur.execute("delete from invoice where id=8")
        record = self.fetch_one(f"select * from agg where customer_id=2 and country='FR'")
        self.assertEqual(record['min_value'], Decimal('1.10'))
        self.assertEqual(record['id_of_min'], 6)
        self.assertEqual(record['max_value'], Decimal('7.70'))
        self.assertEqual(record['id_of_max'], 7)
        self.assertEqual(record['row_count'], 4)

        # test : delete invoice, amount is min amount
        self.cur.execute("insert into invoice (id, name, customer_id, country, amount) values" \
        "(8, 'invoice 8', 2, 'FR', 1.0);")
        record = self.fetch_one(f"select * from agg where customer_id=2 and country='FR'")
        self.assertEqual(record['min_value'], Decimal('1.00'))
        self.assertEqual(record['id_of_min'], 8)
        self.assertEqual(record['max_value'], Decimal('7.70'))
        self.assertEqual(record['id_of_max'], 7)
        self.assertEqual(record['row_count'], 5)
        self.cur.execute("delete from invoice where id=8")
        record = self.fetch_one(f"select * from agg where customer_id=2 and country='FR'")
        self.assertEqual(record['min_value'], Decimal('1.10'))
        self.assertEqual(record['id_of_min'], 6)
        self.assertEqual(record['max_value'], Decimal('7.70'))
        self.assertEqual(record['id_of_max'], 7)
        self.assertEqual(record['row_count'], 4)

        # test : delete invoice, amount is max amount
        self.cur.execute("insert into invoice (id, name, customer_id, country, amount) values" \
        "(9, 'invoice 9', 2, 'FR', 9.9);")
        record = self.fetch_one(f"select * from agg where customer_id=2 and country='FR'")
        self.assertEqual(record['min_value'], Decimal('1.10'))
        self.assertEqual(record['id_of_min'], 6)
        self.assertEqual(record['max_value'], Decimal('9.90'))
        self.assertEqual(record['id_of_max'], 9)
        self.assertEqual(record['row_count'], 5)
        self.cur.execute("delete from invoice where id=9")
        record = self.fetch_one(f"select * from agg where customer_id=2 and country='FR'")
        self.assertEqual(record['min_value'], Decimal('1.10'))
        self.assertEqual(record['id_of_min'], 6)
        self.assertEqual(record['max_value'], Decimal('7.70'))
        self.assertEqual(record['id_of_max'], 7)
        self.assertEqual(record['row_count'], 4)


        # test : update invoice amount, invoice is inbetween (neither min nor max)
        self.cur.execute("update invoice set amount=4.4 where id=4")
        self.assertEqual(record['min_value'], Decimal('1.10'))
        self.assertEqual(record['id_of_min'], 6)
        self.assertEqual(record['max_value'], Decimal('7.70'))
        self.assertEqual(record['id_of_max'], 7)
        self.assertEqual(record['row_count'], 4)

        # test : update invoice amount, invoice is min amount
        self.cur.execute("update invoice set amount=0.9 where id=4")
        record = self.fetch_one(f"select * from agg where customer_id=2 and country='FR'")
        self.assertEqual(record['min_value'], Decimal('0.90'))
        self.assertEqual(record['id_of_min'], 4)
        self.assertEqual(record['max_value'], Decimal('7.70'))
        self.assertEqual(record['id_of_max'], 7)
        self.assertEqual(record['row_count'], 4)

        # test : update invoice amount, invoice is max amount
        self.cur.execute("update invoice set amount=8.8 where id=4")
        record = self.fetch_one(f"select * from agg where customer_id=2 and country='FR'")
        self.assertEqual(record['min_value'], Decimal('1.10'))
        self.assertEqual(record['id_of_min'], 6)
        self.assertEqual(record['max_value'], Decimal('8.80'))
        self.assertEqual(record['id_of_max'], 4)
        self.assertEqual(record['row_count'], 4)

        # test : update invoice amount, amount goes back from max to min
        self.cur.execute("update invoice set amount=0.9 where id=4")
        record = self.fetch_one(f"select * from agg where customer_id=2 and country='FR'")
        self.assertEqual(record['min_value'], Decimal('0.90'))
        self.assertEqual(record['id_of_min'], 4)
        self.assertEqual(record['max_value'], Decimal('7.70'))
        self.assertEqual(record['id_of_max'], 7)
        self.assertEqual(record['row_count'], 4)

        # test : update invoice group by column
        self.cur.execute("update invoice set country='US' where id=4")
        record = self.fetch_one(f"select * from agg where customer_id=2 and country='FR'")
        self.assertEqual(record['min_value'], Decimal('1.10'))
        self.assertEqual(record['id_of_min'], 6)
        self.assertEqual(record['max_value'], Decimal('7.70'))
        self.assertEqual(record['id_of_max'], 7)
        self.assertEqual(record['row_count'], 3)
        record = self.fetch_one(f"select * from agg where customer_id=2 and country='US'")
        self.assertEqual(record['min_value'], Decimal('0.90'))
        self.assertEqual(record['id_of_min'], 4)
        self.assertEqual(record['max_value'], Decimal('0.90'))
        self.assertEqual(record['id_of_max'], 4)
        self.assertEqual(record['row_count'], 1)


    def testTREELEVEL(self):
        self.cur.execute("drop table if exists node cascade;");
        self.cur.execute("create table node(id int PRIMARY KEY, name text, parent_id int, level int)")

        self.cur.execute("call TREELEVEL_create('treelevel', 'node', 'id', 'parent_id', 'level')")
        
        # test : insert root node
        self.cur.execute("insert into node(id, name, parent_id) values(1, 'node 1', null)")
        self.assert_sql_equal("select level from node where id=1;", 0)

        # test : insert level 1 node
        self.cur.execute("insert into node(id, name, parent_id) values(2, 'node 2', 1)")
        self.assert_sql_equal("select level from node where id=1;", 0)
        self.assert_sql_equal("select level from node where id=2;", 1)

        # test : insert level 2 node
        self.cur.execute("insert into node(id, name, parent_id) values(3, 'node 3', 2)")
        self.assert_sql_equal("select level from node where id=3;", 2)

        # test : update parent of level 2 node with no children --> should become level 1
        self.cur.execute("insert into node(id, name, parent_id) values(4, 'node 4', null)")
        self.cur.execute("update node set parent_id=4 where id=3")
        self.assert_sql_equal("select level from node where id=3;", 1)

        # test : remove parent of level 1 node with no children --> should become level 0
        self.cur.execute("update node set parent_id=null where id=3")
        self.assert_sql_equal("select level from node where id=3;", 0)

        # test : update parent of level 1 node with children --> should become level 2 and update children
        self.cur.execute("insert into node(id, name, parent_id) values(5, 'node 5', 1)")
        self.cur.execute("insert into node(id, name, parent_id) values(6, 'node 6', 5)")
        self.cur.execute("insert into node(id, name, parent_id) values(7, 'node 7', 1)")
        self.assert_sql_equal("select level from node where id=5;", 1)
        self.assert_sql_equal("select level from node where id=6;", 2)
        self.assert_sql_equal("select level from node where id=7;", 1)
        self.cur.execute("update node set parent_id=7 where id=5")
        self.assert_sql_equal("select level from node where id=5;", 2)
        self.assert_sql_equal("select level from node where id=6;", 3)
        self.cur.execute("commit");

    def testUNION_BASE_TO_SUB(self):
        # set up
        self.cur.execute("drop table if exists bike cascade;");
        self.cur.execute("drop table if exists car cascade;");
        self.cur.execute("drop table if exists vehicle cascade;");
        self.cur.execute("create table bike(id int, common_attribute1 TEXT, bike_attribute1 TEXT)")
        self.cur.execute("create table car(id int, common_attribute1 TEXT, car_attribute1 DECIMAL)")
        self.cur.execute("call UNION_create('uvehicle2', 'vehicle', ARRAY['bike', 'car'], 'BASE_To_SUB')");

        # test : insert bike
        bike_id = 1
        self.cur.execute(f"insert into vehicle(discriminator, id, common_attribute1, bike_attribute1, car_attribute1) values('bike', {bike_id}, 'commonval1', 'bikeval1', null)")
        self.assert_sql_equal("select count(*) from bike;", 1)
        self.assert_sql_equal("select count(*) from car;", 0)
        record = self.fetch_one(f"select * from bike where id={bike_id}")
        self.assertEqual(record['id'], bike_id)
        self.assertEqual(record['common_attribute1'], 'commonval1')
        self.assertEqual(record['bike_attribute1'], 'bikeval1')
        
        # test : insert car
        car_id = 2
        self.cur.execute(f"insert into vehicle(discriminator, id, common_attribute1, bike_attribute1, car_attribute1) values('car', {car_id}, 'commonval2', null, 2.0)")
        self.assert_sql_equal("select count(*) from bike;", 1)
        self.assert_sql_equal("select count(*) from car;", 1)
        record = self.fetch_one(f"select * from car where id={car_id}")
        self.assertEqual(record['id'], car_id)
        self.assertEqual(record['common_attribute1'], 'commonval2')
        self.assertEqual(record['car_attribute1'], 2.0)

        # test : update bike attribute bike_attribute1
        self.cur.execute(f"update vehicle set bike_attribute1='val2' where id={bike_id}")
        self.assert_sql_equal(f"select bike_attribute1 from bike where id={bike_id}", 'val2')

        # test : update bike attribute common_attribute1
        self.cur.execute(f"update vehicle set common_attribute1='commonval3' where id={bike_id}")
        self.assert_sql_equal(f"select common_attribute1 from bike where id={bike_id}", 'commonval3')

        # test : update car attribute car_attribute1
        self.cur.execute(f"update vehicle set car_attribute1=3.0 where id={car_id}")
        self.assert_sql_equal(f"select car_attribute1 from car where id={car_id}", Decimal(3.0))

        # test : update car attribute common_attribute1
        self.cur.execute(f"update vehicle set common_attribute1='commonval4' where id={car_id}")
        self.assert_sql_equal(f"select common_attribute1 from car where id={car_id}", 'commonval4')

        # test : delete bike
        self.cur.execute(f"delete from vehicle where id={bike_id}")
        self.assert_sql_equal("select count(*) from bike;", 0)
        self.assert_sql_equal("select count(*) from car;", 1)
        
        # test : delete car
        self.cur.execute(f"delete from vehicle where id={car_id}")
        self.assert_sql_equal("select count(*) from bike;", 0)
        self.assert_sql_equal("select count(*) from car;", 0)

        self.cur.execute("commit");

    def testUNION_SUB_TO_BASE(self):
        # set up
        self.cur.execute("drop table if exists bike cascade;");
        self.cur.execute("drop table if exists car cascade;");
        self.cur.execute("drop table if exists vehicle cascade;");
        self.cur.execute("create table bike(id int, common_attribute1 TEXT, bike_attribute1 TEXT)")
        self.cur.execute("create table car(id int, common_attribute1 TEXT, car_attribute1 DECIMAL)")
        self.cur.execute("call UNION_create('uvehicle', 'vehicle', ARRAY['bike', 'car'], 'SUB_TO_BASE')");

        # test : insert bike
        bike_id = 1
        self.cur.execute(f"insert into bike(id, common_attribute1, bike_attribute1) values({bike_id}, 'commonval1', 'bikeval1')")
        self.assert_sql_equal("select count(*) from vehicle;", 1)
        self.assert_sql_equal("select count(*) from bike;", 1)
        self.assert_sql_equal("select count(*) from car;", 0)
        record = self.fetch_one(f"select * from vehicle where id={bike_id}")
        self.assertEqual(record['id'], bike_id)
        self.assertEqual(record['discriminator'], 'bike')
        self.assertEqual(record['common_attribute1'], 'commonval1')
        self.assertEqual(record['bike_attribute1'], 'bikeval1')
        self.assertEqual(record['car_attribute1'], None)

        # test : insert car
        car_id = 2
        self.cur.execute(f"insert into car(id, common_attribute1, car_attribute1) values({car_id}, 'commonval2', 2.0)")
        self.assert_sql_equal("select count(*) from vehicle;", 2)
        self.assert_sql_equal("select count(*) from bike;", 1)
        self.assert_sql_equal("select count(*) from car;", 1)
        record = self.fetch_one(f"select * from vehicle where id={car_id}")
        self.assertEqual(record['id'], car_id)
        self.assertEqual(record['discriminator'], 'car')
        self.assertEqual(record['common_attribute1'], 'commonval2')
        self.assertEqual(record['bike_attribute1'], None)
        self.assertEqual(record['car_attribute1'], Decimal(2.0))

        # test : update bike attribute bike_attribute1
        self.cur.execute(f"update bike set bike_attribute1='val2' where id={bike_id}")
        self.assert_sql_equal(f"select bike_attribute1 from vehicle where id={bike_id}", 'val2')

        # test : update bike attribute common_attribute1
        self.cur.execute(f"update bike set common_attribute1='commonval3' where id={bike_id}")
        self.assert_sql_equal(f"select common_attribute1 from vehicle where id={bike_id}", 'commonval3')

        # test : update car attribute car_attribute1
        self.cur.execute(f"update car set car_attribute1=3.0 where id={car_id}")
        self.assert_sql_equal(f"select car_attribute1 from vehicle where id={car_id}", Decimal(3.0))

        # test : update car attribute common_attribute1
        self.cur.execute(f"update car set common_attribute1='commonval4' where id={car_id}")
        self.assert_sql_equal(f"select common_attribute1 from vehicle where id={car_id}", 'commonval4')

        # test : delete bike
        self.cur.execute(f"delete from bike where id={bike_id}")
        self.assert_sql_equal("select count(*) from vehicle;", 1)
        self.assert_sql_equal("select count(*) from bike;", 0)
        self.assert_sql_equal("select count(*) from car;", 1)
        
        # test : delete car
        self.cur.execute(f"delete from car where id={car_id}")
        self.assert_sql_equal("select count(*) from vehicle;", 0)
        self.assert_sql_equal("select count(*) from bike;", 0)
        self.assert_sql_equal("select count(*) from car;", 0)

        self.cur.execute("commit");

    def testUNION_metadata(self):
        self.cur.execute("drop table if exists bike cascade;");
        self.cur.execute("drop table if exists car cascade;");
        self.cur.execute("drop table if exists vehicle cascade;");
        self.cur.execute("create table bike(id int, common_attribute1 TEXT, bike_attribute1 TEXT)")
        self.cur.execute("create table car(id int, common_attribute1 TEXT, car_attribute1 DECIMAL)")

        id = 'uvehicle_metadata';
        self.cur.execute("call UNION_create(%s, 'vehicle', ARRAY['bike', 'car'], 'SUB_TO_BASE')", (id,));
        self.assert_sql_equal("select count(*) from pgrt_metadata m where m.id=%s;", 1, (id,))
        #self.cur.execute("call UNION_drop(%s)", (id,));
        #self.assert_sql_equal("select count(*) from pgrt_metadata m where m.id=%s;", 0, (id,))


        self.cur.execute("commit");

    
if __name__ == '__main__':
    unittest.main()

