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
	pk TEXT,
    aggregate_column TEXT,
    group_by_column TEXT[] default '{}',
    agg_table TEXT default null
)
LANGUAGE plpgsql
AS $proc$
DECLARE
	str TEXT := ''; -- string buffer
	c TEXT; -- loop index
	i int; -- loop index
	group_by_column_quoted TEXT[]; -- group by column names array where each item is quoted
	group_by_columns_joined TEXT; -- group by column names, joined with ','
	group_by_columns_new_joined TEXT; -- group by column names where each name is prefixed with 'NEW.', joined with ','
	where_condition_on_group_by TEXT := ''; -- SQL fragment : "grp1 = OLD.grp1 AND grp2 = OLD.grp2..."
	where_condition_on_group_by_qual TEXT := ''; -- SQL fragment : "grp1 = table_name.grp1 AND grp2 = table_name.grp2..."
BEGIN
	-- set aggregate table name
	IF agg_table is NULL then
		agg_table := 'agg_' || id;
	END IF;

	-- create array of quoted 'group by' column names
	group_by_column_quoted := group_by_column;
	for i in 1..array_length(group_by_column, 1) LOOP
		group_by_column_quoted[i] := quote_ident(group_by_column[i]);
	end loop;
	group_by_columns_joined := array_to_string(group_by_column_quoted, ', ');
	group_by_columns_new_joined := 'NEW.' || array_to_string(group_by_column_quoted, ', NEW.');
	
	-- init where_condition_on_group_by
	for i in 1..array_length(group_by_column, 1) LOOP
		where_condition_on_group_by := where_condition_on_group_by || group_by_column_quoted[i] || ' = OLD.' || group_by_column_quoted[i];
		if i < array_length(group_by_column, 1) then
			where_condition_on_group_by := where_condition_on_group_by || ' AND ';
		end if;
	end loop;

	-- init where_condition_on_group_by_qual
	for i in 1..array_length(group_by_column, 1) LOOP
		where_condition_on_group_by_qual := where_condition_on_group_by_qual || format('%I = %I.%I', group_by_column_quoted[i], table_name, group_by_column_quoted[i]);
		if i < array_length(group_by_column, 1) then
			where_condition_on_group_by_qual := where_condition_on_group_by_qual || ' AND ';
		end if;
	end loop;

	-- create aggregate table
	execute format($tbl$
		create table %I as
		select %s, %I as min_value, %I as id_of_min, %I as max_value, %I as id_of_max, 0::int as row_count
		from %I
		limit 0;
		$tbl$,
		agg_table,
		group_by_columns_joined,
		aggregate_column, pk, aggregate_column, pk,
		table_name
	);

	execute format($tbl$
		alter table %I ADD CONSTRAINT %I_PK PRIMARY KEY (%s);
		$tbl$,
		agg_table,
		agg_table,
		group_by_columns_joined
	);

	-- create main trigger
	execute format($fun$
		CREATE OR REPLACE FUNCTION AGG_trgfun_%I() --id
		RETURNS TRIGGER AS $inner_trg$
			DECLARE
				id_of_min_val %I.%I%%TYPE; -- table_name, pk
				id_of_max_val %I.%I%%TYPE; -- table_name, pk
			BEGIN
				IF TG_OP='INSERT' then
			    	insert into %I(%s, min_value, id_of_min, max_value, id_of_max, row_count) --agg_table, group_by_columns_joined
					values(%s, NEW.%I, NEW.%I, NEW.%I, NEW.%I, 1) --group_by_columns_new_joined, aggregate_column, pk, aggregate_column, pk
					on conflict(%s) do update set -- group_by_columns_joined
						min_value = least(%I.min_value, NEW.%I), --agg_table, aggregate_column
						id_of_min=case when NEW.%I < %I.min_value then NEW.%I else %I.id_of_min END, --aggregate_column, agg_table, pk, agg_table
						max_value=GREATEST(%I.max_value, NEW.%I), --agg_table, aggregate_column
						id_of_max=case when  NEW.%I > %I.max_value then NEW.%I else %I.id_of_max END, --aggregate_column, agg_table, pk, agg_table
						row_count=%I.row_count+1; -- agg_table
				ELSIF TG_OP='DELETE' then
					select id_of_min, id_of_max
					into id_of_min_val, id_of_max_val
					from %I --agg_table
					where %s; -- where_condition_on_group_by

					update %I set row_count=row_count-1 -- agg_table
					where %s; -- where_condition_on_group_by

					if id_of_min_val = OLD.%I then -- pk
						update %I set -- agg_table
							min_value = (select min(%I) from %I where %s), -- aggregate_column, agg_table, where_condition_on_group_by
							id_of_min = (select %I from %I where %s order by %I asc limit 1) --pk, agg_table, where_condition_on_group_by, aggregate_column
						where %s; -- where_condition_on_group_by
					end if;
					if id_of_max_val = OLD.%I then -- pk
						update %I set -- agg_table
							max_value = (select max(%I) from %I where %s), -- aggregate_column, agg_table, where_condition_on_group_by
							id_of_max = (select %I from %I where %s order by %I desc limit 1) --pk, agg_table, where_condition_on_group_by, aggregate_column
						where %s; -- where_condition_on_group_by
					end if;
				-- ELSIF TG_OP = 'UPDATE' then
				END IF;
				RETURN NEW;
			END;
			$inner_trg$ LANGUAGE plpgsql;
		$fun$,
		id
		, table_name, pk
		, table_name, pk
		, agg_table, group_by_columns_joined
		, group_by_columns_new_joined, aggregate_column, pk, aggregate_column, pk
		, group_by_columns_joined
		, agg_table, aggregate_column
		, aggregate_column, agg_table, pk, agg_table
		, agg_table, aggregate_column
		, aggregate_column, agg_table, pk, agg_table
		, agg_table
		, agg_table
		, where_condition_on_group_by
		, agg_table
		, where_condition_on_group_by
		, pk
		, agg_table
		, aggregate_column, agg_table, where_condition_on_group_by
		, pk, agg_table, where_condition_on_group_by, aggregate_column
		, where_condition_on_group_by
		, pk
		, agg_table
		, aggregate_column, agg_table, where_condition_on_group_by
		, pk, agg_table, where_condition_on_group_by, aggregate_column
		, where_condition_on_group_by
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
		CREATE or replace PROCEDURE AGG_refresh_%I() -- id
		LANGUAGE plpgsql
		AS $body$
			begin
			    delete from %I; -- agg_table
				insert into %I(%s,row_count, min_value, max_value, id_of_min, id_of_max) -- agg_table, group_by_columns_joined
				select %s, -- group_by_columns_joined
					count(*) as row_count,
					min(%I) as min_value, -- aggregate_column
					max(%I) as max_value, -- aggregate_column,
					(SELECT %I FROM %I WHERE %s ORDER BY %I ASC LIMIT 1) AS id_of_min, -- pk, table_name, where_condition_on_group_by_qual, aggregate_column
					(SELECT %I FROM %I WHERE %s ORDER BY %I DESC LIMIT 1) AS id_of_max -- pk, table_name, where_condition_on_group_by_qual, aggregate_column
				from %I -- table_name
				group by %s; -- group_by_columns_joined
			end;
			$body$;
		$inner_proc$,
		id,
		agg_table,
		agg_table, group_by_columns_joined,
		group_by_columns_joined,
		aggregate_column,
		aggregate_column,
		pk, table_name, where_condition_on_group_by_qual, aggregate_column,
		pk, table_name, where_condition_on_group_by_qual, aggregate_column,
		table_name,
		group_by_columns_joined
	);

	execute format('call AGG_refresh_%I()', id);
END;
$proc$;

