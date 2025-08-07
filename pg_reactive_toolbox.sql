--------------------------------------------------------------------------------
-- REVDATE
--------------------------------------------------------------------------------
CREATE or replace PROCEDURE REVDATE_create (
	id TEXT,
    table_name TEXT,
    column_name TEXT
)
LANGUAGE plpgsql
AS $proc$
BEGIN
	execute format($fun$
		CREATE OR REPLACE FUNCTION "REVDATE_trgfun_%I"()
		RETURNS TRIGGER AS $inner_trg$
			BEGIN
			    NEW.%I := CURRENT_TIMESTAMP;
			    RETURN NEW;
			END;
			$inner_trg$ LANGUAGE plpgsql;
		$fun$, id,
		column_name
	);

    execute format($trg$
		CREATE or replace TRIGGER "REVDATE_trg_%I"
		before insert or UPDATE ON %I
		FOR EACH ROW
		execute procedure "REVDATE_trgfun_%I"();
		$trg$, id,
		table_name,
		id
	);

END;
$proc$;


CREATE or replace PROCEDURE REVDATE_enable (
	id TEXT,
    table_name TEXT
)
LANGUAGE plpgsql
AS $proc$
begin
	execute format($$
		ALTER TABLE %I enable TRIGGER "REVDATE_trg_%I";
		$$, table_name, id);
end;
$proc$;

CREATE or replace PROCEDURE REVDATE_disable (
	id TEXT,
    table_name TEXT
)
LANGUAGE plpgsql
AS $proc$
begin
	execute format($$
		ALTER TABLE %I disable TRIGGER "REVDATE_trg_%I";
		$$, table_name, id);
end;
$proc$;

CREATE or replace PROCEDURE REVDATE_drop (
	id TEXT,
    table_name TEXT
)
LANGUAGE plpgsql
AS $proc$
declare
	my_sql_state TEXT;
begin
	execute format($$
		DROP TRIGGER "REVDATE_trg_%I" ON %I;
		$$, id, table_name);

	execute format($$
		DROP function "REVDATE_trgfun_%I";
		$$, id);
end;
$proc$;


CREATE or replace PROCEDURE REVDATE_refresh (
	id TEXT
)
LANGUAGE plpgsql
AS $proc$
begin
	execute format($$
		call "REVDATE_refresh_%I"();
		$$, id);
end;
$proc$;

--------------------------------------------------------------------------------
-- COUNTLNK
--------------------------------------------------------------------------------
CREATE or replace PROCEDURE COUNTLNK_create (
	id TEXT,
    base_table_name TEXT,
    base_pk TEXT,
    base_count_column TEXT,
    linked_table_name TEXT,
    linked_fk TEXT
)
LANGUAGE plpgsql
AS $proc$
BEGIN
	execute format($fun$
		CREATE OR REPLACE FUNCTION "COUNTLNK_trgfun_%I"()
		RETURNS TRIGGER AS $inner_trg$
			BEGIN
				IF TG_OP='INSERT' then
			    	update %I set %I=%I+1 where %I=NEW.%I;
				ELSIF TG_OP='DELETE' then
					update %I set %I=%I-1 where %I=OLD.%I;
				ELSIF TG_OP='UPDATE' and OLD.%I <> NEW.%I then
					update %I set %I=%I-1 where %I=OLD.%I;
					update %I set %I=%I+1 where %I=NEW.%I;
				ELSIF TG_OP='TRUNCATE' then
					update %I set %I=0;
				END IF;
				RETURN NEW;
			END;
			$inner_trg$ LANGUAGE plpgsql;
		$fun$,
			id, -- function name
			quote_ident(base_table_name), quote_ident(base_count_column), quote_ident(base_count_column), quote_ident(base_pk), quote_ident(linked_fk),  -- update operation for insert
			quote_ident(base_table_name), quote_ident(base_count_column), quote_ident(base_count_column), quote_ident(base_pk), quote_ident(linked_fk),  -- update operation for delete
			quote_ident(linked_fk), quote_ident(linked_fk),  -- if condition for update
			quote_ident(base_table_name), quote_ident(base_count_column), quote_ident(base_count_column), quote_ident(base_pk), quote_ident(linked_fk),  -- update operation for update/row 1
			quote_ident(base_table_name), quote_ident(base_count_column), quote_ident(base_count_column), quote_ident(base_pk), quote_ident(linked_fk),  -- update operation for update/row 2
			quote_ident(base_table_name), quote_ident(base_count_column)  -- update operation for truncate
		);

	execute format($inner_proc$
		CREATE or replace PROCEDURE "COUNTLNK_refresh_%I"()
		LANGUAGE plpgsql
		AS $inner_proc2$
			begin
			    update %I set %I = sub.cpt
				from (
					select %I as id, count(*) as cpt
					from %I
					group by %I
				) as sub
				where %I.%I = sub.id;
			end;
			$inner_proc2$;
		$inner_proc$,
		id, -- function name
		quote_ident(base_table_name), quote_ident(base_count_column),
		quote_ident(linked_fk),
		quote_ident(linked_table_name),
		quote_ident(linked_fk),
		quote_ident(base_table_name), quote_ident(base_pk)
	);

    execute format($trg$
		CREATE or replace TRIGGER COUNTLNK_trg_%I
		after delete or insert or update ON %I
		FOR EACH ROW
		execute procedure "COUNTLNK_trgfun_%I"();
		$trg$,
		id, -- function name
		linked_table_name,
		id -- trigger function name
	);

    execute format($trg$
		CREATE or replace TRIGGER "COUNTLNK_trg_truncate_%I"
		after truncate ON %I
		FOR EACH STATEMENT
		execute procedure "COUNTLNK_trgfun_%I"();
		$trg$,
		id, -- trigger name
		linked_table_name,
		id -- trigger function name
	);

	execute format('call "COUNTLNK_refresh_%I"()', id);

END;
$proc$;


CREATE or replace PROCEDURE COUNTLNK_refresh (
	id TEXT
)
LANGUAGE plpgsql
AS $proc$
begin
	execute format($$
		call "COUNTLNK_refresh_%I"();
		$$, id);
end;
$proc$;

--------------------------------------------------------------------------------
-- AGG
--------------------------------------------------------------------------------
CREATE or replace PROCEDURE AGG_create (
	id text,
    table_name TEXT,
    aggregate_column TEXT,
    group_by_column TEXT,
    agg_table TEXT default null
)
LANGUAGE plpgsql
AS $proc$
BEGIN
	IF agg_table is NULL then
		agg_table := 'agg_' || id;
	END IF;
	execute format($tbl$
		create table %I as
		select %I as group_value, %I as min_value, %I as max_value, 0::int as row_count
		from %I
		limit 0;
		$tbl$,
		quote_ident(agg_table),
		quote_ident(group_by_column), quote_ident(aggregate_column), quote_ident(aggregate_column),
		quote_ident(table_name)
	);

	execute format($tbl$
		alter table %I ADD CONSTRAINT %I_PK PRIMARY KEY (group_value);
		$tbl$,
		quote_ident(agg_table),
		agg_table
	);

	execute format($fun$
		CREATE OR REPLACE FUNCTION AGG_trgfun_%I()
		RETURNS TRIGGER AS $inner_trg$
			BEGIN
				IF TG_OP='INSERT' then
			    	insert into %I(group_value, min_value, max_value, row_count) values(NEW.%I, NEW.%I, NEW.%I, 1)
					on conflict(group_value) do update set min_value = least(%I.min_value, NEW.%I), max_value=GREATEST(%I.max_value, NEW.%I), row_count=%I.row_count+1;
				ELSIF TG_OP='DELETE' then
			    	update %I set
						min_value = (select min(%I) from %I where %I = OLD.%I),
						max_value = (select max(%I) from %I where %I = OLD.%I),
						row_count=%I.row_count-1
					where group_value = OLD.%I;
				ELSIF TG_OP = 'UPDATE' and OLD.%I = NEW.%I and NEW.%I < OLD.%I then -- update case with aggregation value decrease
					update %I
					set min_value = least(%I.min_value, NEW.%I)
					where group_value=OLD.%I;
				ELSIF TG_OP = 'UPDATE' and OLD.%I = NEW.%I and NEW.%I > OLD.%I then -- update case with aggregation value increase
					update %I set
						max_value = greatest(%I.max_value, NEW.%I)
					where group_value=OLD.%I;
				END IF;
				RETURN NEW;
			END;
			$inner_trg$ LANGUAGE plpgsql;
		$fun$,
		id, -- function name
		agg_table, quote_ident(group_by_column), quote_ident(aggregate_column), quote_ident(aggregate_column), -- insert case 
		agg_table, quote_ident(aggregate_column), agg_table, quote_ident(aggregate_column), agg_table,  -- insert case/on conflict part
		agg_table, -- delete case
		aggregate_column, table_name, group_by_column, group_by_column, -- delete case
		aggregate_column, table_name, group_by_column, group_by_column, -- delete case
		agg_table, -- delete case
		group_by_column, -- delete case
		group_by_column, group_by_column, aggregate_column, aggregate_column, agg_table, agg_table, aggregate_column, group_by_column, -- update case with aggregation value decrease
		group_by_column, group_by_column, aggregate_column, aggregate_column, agg_table, agg_table, aggregate_column, group_by_column -- update case with aggregation value increase
		-- update case with grouping value change: TODO
		
		-- TODO : plutot que de s'embeter, on peut recalculer le count() max() min() a chaque fois, ça devrait être a peu pres aussi rapide.
	);

    execute format($trg$
		CREATE or replace TRIGGER AGG_trg_%I
		after delete or insert or update ON %I
		FOR EACH ROW
		execute procedure AGG_trgfun_%I();
		$trg$,
		id,
		quote_ident(table_name),
		id
	);

	execute format($inner_proc$
		CREATE or replace PROCEDURE AGG_refresh_%I()
		LANGUAGE plpgsql
		AS $body$
			begin
			    delete from %I;
				insert into %I(group_value,row_count, min_value, max_value)
				select %I, count(*) as row_count, min(%I) as min_value, max(%I) as max_value
				from %I
				group by %I;
			end;
			$body$;
		$inner_proc$,
		id, -- function name
		agg_table, -- delete from %I;
		agg_table, -- insert into %I
		group_by_column, aggregate_column, aggregate_column, -- select %I, count(*) as row_count, min(%I) as min_value, max(%I) as max_value
		table_name, -- from %I
		group_by_column -- group by %I;
	);

	execute format('call "AGG_refresh_%I"()', id);
END;
$proc$;

