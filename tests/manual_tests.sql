----------------------------------------------------------------------------------------------------
-- test "COUNTLNK": 
----------------------------------------------------------------------------------------------------
drop table customer;
drop table invoice;

create table customer (id SERIAL PRIMARY KEY, name text, invoice_count int default 0);
create table invoice(id SERIAL PRIMARY KEY, name text, customer_id int references customer(id));

insert into customer(name) values('customer A'), ('customer B');
insert into invoice (name, customer_id) values('invoice 1', 1), ('invoice 2', 1), ('invoice 3', 2);

select * from customer;
select * from invoice;

update customer set invoice_count=0;
call COUNTLNK_refresh();

insert into invoice (name, customer_id) values('invoice 7', 2);
update invoice set customer_id=1 where id in (15,16);

delete from invoice where id=12;
truncate table invoice;


call COUNTLNK_refresh(
	id := '1',
	base_table_name := 'customer',
	base_pk := 'id',
    base_count_column := 'invoice_count',
    linked_table_name := 'invoice',
    linked_fk := 'customer_id'
);

-- list created triggers
SELECT tgname AS trigger_name, *
FROM pg_trigger
WHERE tgrelid = 'invoice'::regclass
  AND NOT tgisinternal;

call linked_refresh_customer_invoice_count();

----------------------------------------------------------------------------------------------------
-- test "AGG"
----------------------------------------------------------------------------------------------------
drop table test_agg;
create table test_agg(id serial primary key, status text, amount integer);
drop table agg_a;
call AGG_create(
	id := 'a',
	table_name := 'test_agg',
    aggregate_column := 'amount',
    group_by_column := 'status',
    agg_table := 'agg_a'
);
select * from test_agg;
delete from test_agg;
insert into test_agg(status, amount) values ('actif', 2);
select * from agg_a; -- expected : actif	2	2	1
insert into test_agg(status, amount) values ('actif', 3);
select * from agg_a; -- expected: actif	2	3	2
insert into test_agg(status, amount) values ('inactif', 1);
select * from agg_a; -- expected: actif	2	3	2/inactif	1	1	1
update test_agg set amount=4 where status='actif' and amount=3; 
select * from agg_a; -- expected : 		inactif	1	1	1/ actif	2	4	2
delete from test_agg where status='actif' and amount=4;
select * from agg_a; -- expected: inactif	1	1	1/actif	2	2	1
delete from test_agg where status='inactif';
select * from agg_a; -- expected: actif	2	2	1 / inactif	NULL	NULL	0
insert into test_agg(status, amount) values ('actif', 0);
select * from agg_a; -- expected: actif	0	2	2
update test_agg set amount=1 where status='actif' and amount=0; 
select * from agg_a; -- expected: actif	0	2	2

call AGG_refresh_a;


select * from agg_a;
select status, count(*) as row_count, min(amount) as min_value, max(amount) as max_value
from test_agg
group by status;