from decimal import Decimal
from typing import Literal
import psycopg2
import psycopg2.extras
import unittest
import settings
from datetime import datetime, timedelta
from pathlib import Path
from psycopg2.extras import execute_values
import time

SyncDirection = Literal["BASE_TO_SUB", "SUB_TO_BASE"]

class PerfTestModule(unittest.TestCase):
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

    def execute_sql_file(self, sql_file):
        with open(sql_file, 'r') as file:
            sql_commands = file.read()
        self.cur.execute(sql_commands)
        

    def tearDown(self):
        self.cur.execute("commit;")
        self.cur.close()  # Close cursor
        self.conn.close()  # Close the connection

    def batch_insert(self, table, columns, rows, page_size=100):
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s"
        
        for i in range(0, len(rows), page_size):
            chunk = rows[i:i+page_size]
            execute_values(self.cur, sql, chunk, page_size=page_size)

    def test_perf_UNION_insert(self):
        page_size=1
        row_count=50

        for sync_direction in ['BASE_TO_SUB', 'SUB_TO_BASE']:
            duration_in_ms_ref = self.measure_perf_UNION_insert(row_count, False, sync_direction, page_size)
            duration_in_ms = self.measure_perf_UNION_insert(row_count, True, sync_direction, page_size)
            overhead = (duration_in_ms - duration_in_ms_ref) / duration_in_ms_ref * 100.0
            print(f"Run UNION with sync_direction={sync_direction} page_size={page_size}. Overhead={overhead}% Average duration:  {duration_in_ms} ms vs ref {duration_in_ms_ref} ms ")


    def measure_perf_UNION_insert(self, row_count, enable_triggers, sync_direction, page_size):
        durations_in_ms = []
        for i in range(1, 50):
            duration_in_ms = self.measure_perf_UNION_insert_single_run(row_count, enable_triggers, sync_direction, page_size)
            durations_in_ms.append(duration_in_ms)
        avg_duration_in_ms = sum(durations_in_ms) / len(durations_in_ms)
        return avg_duration_in_ms

    def measure_avg_duration(self, callback):
        durations_in_ms = []
        for i in range(1, 50):
            duration_in_ms = callback()
            durations_in_ms.append(duration_in_ms)
        avg_duration_in_ms = sum(durations_in_ms) / len(durations_in_ms)
        return avg_duration_in_ms
    
    def measure_perf_UNION_insert_single_run(self, row_count, enable_triggers, sync_direction: SyncDirection, page_size):
        self.cur.execute("drop table if exists bike cascade;");
        self.cur.execute("drop table if exists car cascade;");
        self.cur.execute("drop table if exists vehicle cascade;");
        self.cur.execute("create table bike(id int primary key, common_attribute1 TEXT, bike_attribute1 TEXT)")
        self.cur.execute("create table car(id int primary key, common_attribute1 TEXT, car_attribute1 DECIMAL)")

        if (enable_triggers):
            self.cur.execute(f"call UNION_create('uvehicle', 'vehicle', ARRAY['bike', 'car'], '{sync_direction}')");
        else:
            self.cur.execute(f"create table vehicle(id int primary key, discriminator TEXT, common_attribute1 TEXT, bike_attribute1 TEXT, car_attribute1 DECIMAL)")
        self.conn.commit()

        start = time.perf_counter()

        if sync_direction == 'SUB_TO_BASE':
            # insert bikes
            rows = [];
            for i in range(1, row_count):
                rows.append((
                    i,
                    f"commonval{i}",
                    f"bikeval{i}",
                ))
            self.batch_insert("bike", ['id', 'common_attribute1', 'bike_attribute1'], rows, page_size)

            # insert cars
            rows = [];
            for i in range(row_count + 1, row_count * 2):
                rows.append((
                    i,
                    f"commonval{i}",
                    i,
                ))
            self.batch_insert("car", ['id', 'common_attribute1', 'car_attribute1'], rows, page_size)
        else:
            # insert vehicles
            rows = [];
            for i in range(1, row_count):
                rows.append((
                    i,
                    'bike',
                    f"commonval{i}",
                    f"bikeval{i}",
                    None,
                ))
            for i in range(row_count + 1, row_count * 2):
                rows.append((
                    i,
                    'car',
                    f"commonval{i}",
                    None,
                    i,
                ))
            self.batch_insert("vehicle", ['id', 'discriminator', 'common_attribute1', 'bike_attribute1', 'car_attribute1'], rows, page_size)

        self.conn.commit()
        end = time.perf_counter()
        
        duration_in_ms = (end - start) * 1000
        #print(f"Execution time: {duration_in_ms} ms")
        return duration_in_ms

    def test_perf_UNION_update(self, ):
        # insert test data
        sync_direction: SyncDirection = 'SUB_TO_BASE'
        row_count = 1000
        page_size = 100

        self.measure_perf_UNION_insert_single_run(row_count, True, sync_direction, page_size)
        duration_in_ms = self.measure_avg_duration(lambda: self.measure_perf_UNION_update_single_run(sync_direction, 100)
        print(f"Run UNION_update with sync_direction={sync_direction} page_size={page_size}. Overhead={overhead}% Average duration:  {duration_in_ms} ms vs ref {duration_in_ms_ref} ms ")                                         
        
    
    def measure_perf_UNION_update_single_run(self, sync_direction: SyncDirection, row_count_to_update):
        if (sync_direction == 'SUB_TO_BASE'):
            self.cur.execute("update bike set common_attribute1=%s where id <= %s", ('newval1', row_count_to_update))
        else:
            self.cur.execute("update vehicle set common_attribute1=%s where id <= %s", ('newval1', row_count_to_update))
        self.conn.commit()