--------------------------------------------------------------------------------
-- INTERNAL/UTILITY FUNCTIONS
--------------------------------------------------------------------------------
-- Insert a row inot the metadata table. args is a JSON object containing procedure arguments.
CREATE or replace PROCEDURE _pgf_internal_insert_metadata (
	id TEXT,
	kind TEXT,
	args JSONB
)
LANGUAGE plpgsql AS $proc$
BEGIN
	CREATE TABLE IF NOT EXISTS pgf_metadata(id TEXT primary key, kind TEXT, args JSONB, created_at TIMESTAMP);
	insert into pgf_metadata values(id, kind, args, CURRENT_TIMESTAMP);
END;
$proc$;

CREATE or replace PROCEDURE _pgf_internal_delete_metadata (
	id TEXT
)
LANGUAGE plpgsql AS $proc$
BEGIN
	delete from pgf_metadata m where m.id = _pgf_internal_delete_metadata.id;
END;
$proc$;

-- Get a row from the metadata table. args is a JSON object containing procedure arguments + a special attribute 'kind".
CREATE or replace FUNCTION _pgf_internal_get_metadata (
	id TEXT
)
RETURNS JSONB
LANGUAGE plpgsql AS $proc$
DECLARE
	res JSONB;
BEGIN
	CREATE TABLE IF NOT EXISTS pgf_metadata(id TEXT primary key, kind TEXT, args JSONB, created_at TIMESTAMP);
	select args || jsonb_build_object('kind', kind) INTO res from pgf_metadata m where m.id = _pgf_internal_get_metadata.id;
	return res;
END;
$proc$;

CREATE OR REPLACE FUNCTION _pgf_internal_jsonb_to_text_array(j jsonb)
RETURNS text[] LANGUAGE sql IMMUTABLE AS $$
    SELECT array_agg(value)
    FROM jsonb_array_elements_text(j) AS t(value);
$$;

-- join fragments using the specified patter.
-- For instance, given fragments = ['a', 'b', 'c'] and pattern='%s = OLD.%s' and delimiter=' AND '
-- returns the string: "a = OLD.a AND b = OLD.b"
create or replace function _pgf_internal_join(fragments TEXT[], pattern TEXT default '%s', delimiter TEXT default ', ', quote_fragments boolean default true)
returns text
LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
	res TEXT = ''; -- result
	i int; -- loop index
	fragments_quoted TEXT[];
BEGIN
	-- leave early on empty array
	if fragments is null or array_length(fragments, 1) is null then
		return '';
	end if;

	-- quote fragments if asked
	fragments_quoted := fragments;
	if quote_fragments then
		for i in 1..array_length(fragments, 1) LOOP
			fragments_quoted[i] := quote_ident(fragments[i]);
		end loop;
	end if;

	for i in 1..array_length(fragments, 1) loop
		res := res || replace(pattern, '%s', fragments[i]);
		if i < array_length(fragments, 1) then
			res := res || delimiter;
		end if;
	end loop;
	return res;
END;
$$;

--------------------------------------------------------------------------------
-- COMMON FUNCTIONS
--------------------------------------------------------------------------------
create or replace procedure pgf_refresh(
	id TEXT
)
LANGUAGE plpgsql AS $proc$
DECLARE
	args JSONB;
	kind TEXT;
BEGIN
	args := _pgf_internal_get_metadata(id);
	kind := args->>'kind';
	IF kind = 'revdate' or kind = 'audit_table' then
		-- no op
	ELSE
		execute format('call "_pgf_internal_refresh_%I"();', id);
	end IF;
END;
$proc$;

CREATE or replace PROCEDURE pgf_drop (
	id TEXT
)
LANGUAGE plpgsql AS $proc$
declare
	args JSONB;
	kind TEXT;
	table_name TEXT;
	base_table_name TEXT;
    sub_tables TEXT[];
    sync_direction TEXT;
begin
	args := _pgf_internal_get_metadata(id);
	kind := args->>'kind';
	
	-- drop refresh procedure
	execute format('drop procedure if exists _pgf_internal_refresh_%I', kind, id);

	-- drop other objects
	if kind = 'revdate' then
		table_name := args->>'table_name';
		execute format('DROP TRIGGER IF EXISTS _pgf_internal_revdate_trg_%I ON %I;', id, table_name);
		execute format('DROP function if exists _pgf_internal_revdate_trgfun_%I();', id);

	ELSIF kind = 'count' then
		table_name := args->>'linked_table_name';
		execute format('drop trigger if exists _pgf_internal_count_trg_%I on %I', id, table_name);
		execute format('drop trigger if exists _pgf_internal_count_trg_truncate_%I on %I', id, table_name);
		execute format('drop function if exists _pgf_internal_count_trgfun_%I()', id);

	ELSIF kind = 'minmax_table' then
		table_name := args->>'table_name';
		execute format('drop trigger if exists _pgf_internal_minmax_table_trg_%I on %I;', id, table_name);
		execute format('drop function if exists _pgf_internal_minmax_table_trgfun_%I()', id);

	ELSIF kind = 'treelevel' then
		table_name := args->>'table_name';
		execute format('DROP TRIGGER IF EXISTS _pgf_internal_treelevel_trg_%s ON %I;', id, table_name);
    	execute format('DROP FUNCTION IF EXISTS _pgf_internal_treelevel_trgfun_%s();', id);

	ELSIF kind = 'inheritance_table' then
		base_table_name := args->>'base_table_name';
		sub_tables := _pgf_internal_jsonb_to_text_array(args->'sub_tables');
		sync_direction := args->>'sync_direction';

		IF sync_direction = 'SUB_TO_BASE' THEN
			FOR i IN 1..array_length(sub_tables, 1) LOOP
				execute format('drop trigger if exists _pgf_internal_inheritance_table_trg_%s_%s on %I; ', id, sub_tables[i], sub_tables[i]);
				execute format('drop function if exists _pgf_internal_inheritance_table_trgfun_%s_%s; ', id, sub_tables[i]);
			END LOOP;
		ELSE
			FOR i IN 1..array_length(sub_tables, 1) LOOP
				execute format('drop trigger if exists _pgf_internal_inheritance_table_trg_%s_%s on %I; ', id, sub_tables[i], base_table_name);
			END LOOP;
			execute format('drop function if exists _pgf_internal_inheritance_table_trgfun_%s_%s; ', id, base_table_name);
		END IF;

	ELSIF kind = 'audit_table' then
		sub_tables := _pgf_internal_jsonb_to_text_array(args->'audited_table_names');

		-- Drop triggers and trigger functions for each audited table
		FOR i IN 1..array_length(sub_tables, 1) LOOP
			execute format('DROP TRIGGER IF EXISTS _pgf_internal_audit_table_trg_%s_%s ON %I;', id, sub_tables[i], sub_tables[i]);
			execute format('DROP FUNCTION IF EXISTS _pgf_internal_audit_table_trgfun_%s_%s();', id, sub_tables[i]);
		END LOOP;

	ELSIF kind = 'sync' then
		table_name := args->>'table_name';
		execute format('DROP TRIGGER IF EXISTS _pgf_internal_sync_trg_%I ON %I;', id, table_name);
		execute format('DROP FUNCTION IF EXISTS _pgf_internal_sync_trgfun_%I();', id);
	end if;

	call _pgf_internal_delete_metadata(id);

end;
$proc$;

CREATE or replace PROCEDURE pgf_set_enabled (
	id TEXT,
	enabled boolean
)
LANGUAGE plpgsql AS $proc$
declare
	args JSONB;
	kind TEXT;
	table_name TEXT;
	base_table_name TEXT;
    sub_tables TEXT[];
    sync_direction TEXT;
	enable_fragment TEXT; -- either 'enable' or 'disable'
begin
	args := _pgf_internal_get_metadata(id);
	kind := args->>'kind';
	enable_fragment := case when enabled then 'enable' else 'disable' end;

	if kind = 'revdate' then
		table_name := args->>'table_name';
		execute format('ALTER TABLE %I %s TRIGGER _pgf_internal_revdate_trg_%I;', table_name, enable_fragment, id);

	elsif kind = 'count' then
		table_name := args->>'linked_table_name';
		if enabled then
			execute format('LOCK TABLE %I IN EXCLUSIVE MODE;', table_name); -- allow reads but not writes
		end if;
		execute format('alter table %I %s trigger _pgf_internal_count_trg_%I', table_name, enable_fragment, id);
		execute format('alter table %I %s trigger _pgf_internal_count_trg_truncate_%I', table_name, enable_fragment, id);

	elsif kind = 'minmax_table' then
		table_name := args->>'table_name';
		if enabled then
			execute format('LOCK TABLE %I IN EXCLUSIVE MODE;', table_name); -- allow reads but not writes
		end if;
		execute format('alter table %I %s trigger _pgf_internal_minmax_table_trg_%I;', table_name, enable_fragment, id);

	elsif kind = 'treelevel' then
		table_name := args->>'table_name';
		if enabled then
			execute format('LOCK TABLE %I IN EXCLUSIVE MODE;', table_name); -- allow reads but not writes
		end if;
		execute format('ALTER TABLE %I %s TRIGGER _pgf_internal_treelevel_trg_%s;', table_name, enable_fragment, id);

	elsif kind = 'inheritance_table' then
		base_table_name := args->>'base_table_name';
		sub_tables := _pgf_internal_jsonb_to_text_array(args->'sub_tables');
		sync_direction := args->>'sync_direction';

		IF sync_direction = 'SUB_TO_BASE' THEN
			if enabled then
				FOR i IN 1..array_length(sub_tables, 1) LOOP
					
					execute format('LOCK TABLE %I IN EXCLUSIVE MODE;', sub_tables[i]); -- allow reads but not writes
				END LOOP;
			end if;
			FOR i IN 1..array_length(sub_tables, 1) LOOP
				execute format('alter table %I enable trigger _pgf_internal_inheritance_table_trg_%s_%s;', sub_tables[i], id, sub_tables[i]);
			END LOOP;
		ELSE
			if enabled then
				execute format('LOCK TABLE %I IN EXCLUSIVE MODE;', base_table_name); -- allow reads but not writes
			end if;
			FOR i IN 1..array_length(sub_tables, 1) LOOP
				execute format('alter table %I enable trigger _pgf_internal_inheritance_table_trg_%s_%s;', base_table_name, id, sub_tables[i]);
			END LOOP;
		END IF;

	elsif kind = 'audit_table' then
		sub_tables := _pgf_internal_jsonb_to_text_array(args->'audited_table_names');
		FOR i IN 1..array_length(sub_tables, 1) LOOP
			execute format('ALTER TABLE %I %s TRIGGER _pgf_internal_audit_table_trg_%s_%s;', sub_tables[i], enable_fragment, id, sub_tables[i]);
		END LOOP;

	elsif kind = 'sync' then
		table_name := args->>'table_name';
		execute format('ALTER TABLE %I %s TRIGGER _pgf_internal_sync_trg_%I;', table_name, enable_fragment, id);
	
	else
		raise exception 'Unknown value for argument "kind": %', kind;
	end if;

	call pgf_refresh(id);
end;
$proc$;



-------------------------------------------------------------------------------
-- REVDATE
--------------------------------------------------------------------------------
CREATE or replace PROCEDURE pgf_revdate (
	id TEXT,
    table_name TEXT,
    column_name TEXT
)
LANGUAGE plpgsql AS $proc$
BEGIN
	call _pgf_internal_insert_metadata(id, 'revdate', jsonb_build_object('table_name', table_name, 'column_name', column_name));
	execute format($fun$
		CREATE OR REPLACE FUNCTION _pgf_internal_revdate_trgfun_%I()
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
		CREATE or replace TRIGGER _pgf_internal_revdate_trg_%I
		before insert or UPDATE ON %I
		FOR EACH ROW
		execute procedure _pgf_internal_revdate_trgfun_%I();
		$trg$, id,
		table_name,
		id
	);

	-- no full refresh necessay here

END;
$proc$;


--------------------------------------------------------------------------------
-- COUNT
--------------------------------------------------------------------------------
CREATE or replace PROCEDURE pgf_count (
	id TEXT,
    base_table_name TEXT,
    base_pk TEXT,
    base_count_column TEXT,
    linked_table_name TEXT,
    linked_fk TEXT
)
LANGUAGE plpgsql AS $proc$
BEGIN
	call _pgf_internal_insert_metadata(id, 'count', jsonb_build_object(
		'base_table_name', base_table_name,
		'base_pk', base_pk,
		'base_count_column', base_count_column,
		'linked_table_name', linked_table_name,
		'linked_fk', linked_fk
	));

	execute format($fun$
		CREATE OR REPLACE FUNCTION _pgf_internal_count_trgfun_%I() -- id
		RETURNS TRIGGER AS $inner_trg$
			BEGIN
				IF TG_OP='INSERT' then
			    	update %I set %I=%I+1 where %I=NEW.%I; -- base_table_name, base_count_column, base_count_column, base_pk, linked_fk
				ELSIF TG_OP='DELETE' then
					update %I set %I=%I-1 where %I=OLD.%I; -- base_table_name, base_count_column, base_count_column, base_pk, linked_fk
				ELSIF TG_OP='UPDATE' and OLD.%I <> NEW.%I then -- linked_fk, linked_fk
					update %I set %I=%I-1 where %I=OLD.%I; -- base_table_name, base_count_column, base_count_column, base_pk, linked_fk
					update %I set %I=%I+1 where %I=NEW.%I; -- base_table_name, base_count_column, base_count_column, base_pk, linked_fk
				ELSIF TG_OP='TRUNCATE' then
					update %I set %I=0; -- base_table_name, base_count_column
				END IF;
				RETURN NEW;
			END;
			$inner_trg$ LANGUAGE plpgsql;
		$fun$,
			id,
			base_table_name, base_count_column, base_count_column, base_pk, linked_fk,
			base_table_name, base_count_column, base_count_column, base_pk, linked_fk,
			linked_fk, linked_fk,
			base_table_name, base_count_column, base_count_column, base_pk, linked_fk,
			base_table_name, base_count_column, base_count_column, base_pk, linked_fk,
			base_table_name, base_count_column
		);

	execute format($inner_proc$
		CREATE or replace PROCEDURE "_pgf_internal_refresh_%I"() -- id
		LANGUAGE plpgsql
		AS $inner_proc2$
			begin
			    update %I set %I = sub.cpt -- base_table_name, base_count_column
				from (
					select %I as id, count(*) as cpt -- linked_fk
					from %I -- linked_table_name
					group by %I -- linked_fk
				) as sub
				where %I.%I = sub.id; -- base_table_name, base_pk
			end;
			$inner_proc2$;
		$inner_proc$,
		id, -- function name
		base_table_name, base_count_column,
		linked_fk,
		linked_table_name,
		linked_fk,
		base_table_name, base_pk
	);

    execute format($trg$
		CREATE or replace TRIGGER _pgf_internal_count_trg_%I -- id
		after delete or insert or update ON %I -- linked_table_name
		FOR EACH ROW
		execute procedure _pgf_internal_count_trgfun_%I(); -- id
		$trg$,
		id,
		linked_table_name,
		id
	);

    execute format($trg$
		CREATE or replace TRIGGER _pgf_internal_count_trg_truncate_%I -- id
		after truncate ON %I -- linked_table_name
		FOR EACH STATEMENT
		execute procedure _pgf_internal_count_trgfun_%I(); -- id
		$trg$,
		id,
		linked_table_name,
		id
	);

	call pgf_refresh(id);

END;
$proc$;


--------------------------------------------------------------------------------
-- SUM
--------------------------------------------------------------------------------
CREATE or replace PROCEDURE pgf_sum (
	id TEXT,
    base_table_name TEXT,
    base_pk TEXT,
    base_aggregate_column TEXT,
    linked_table_name TEXT,
    linked_fk TEXT,
	linked_value_column TEXT -- column to be summed
)
LANGUAGE plpgsql AS $proc$
BEGIN
	call _pgf_internal_insert_metadata(id, 'sum', jsonb_build_object(
		'base_table_name', base_table_name,
		'base_pk', base_pk,
		'base_aggregate_column', base_aggregate_column,
		'linked_table_name', linked_table_name,
		'linked_fk', linked_fk,
		'linked_value_column', linked_value_column
	));

	execute format($fun$
		CREATE OR REPLACE FUNCTION _pgf_internal_sum_trgfun_%I() -- id
		RETURNS TRIGGER AS $inner_trg$
			BEGIN
				IF TG_OP='INSERT' then
			    	update %I set %I=%I+NEW.%I where %I=NEW.%I; -- base_table_name, base_aggregate_column, base_aggregate_column, linked_value_column, base_pk, linked_fk
				ELSIF TG_OP='DELETE' then
					update %I set %I=%I-OLD.%I where %I=OLD.%I; -- base_table_name, base_aggregate_column, base_aggregate_column, linked_value_column, base_pk, linked_fk
				ELSIF TG_OP='UPDATE' and OLD.%I <> NEW.%I then -- linked_fk, linked_fk
					update %I set %I=%I-OLD.%I where %I=OLD.%I; -- base_table_name, base_aggregate_column, base_aggregate_column, linked_value_column, base_pk, linked_fk
					update %I set %I=%I+NEW.%I where %I=NEW.%I; -- base_table_name, base_aggregate_column, base_aggregate_column, linked_value_column, base_pk, linked_fk
				ELSIF TG_OP = 'UPDATE' and OLD.%I <> NEW.%I then -- linked_value_column, linked_value_column
					update %I set %I = %I - OLD.%I + NEW.%I where %I = OLD.%I; -- base_table_name, base_aggregate_column, base_aggregate_column, linked_value_column, linked_value_column, base_pk, linked_fk
				ELSIF TG_OP='TRUNCATE' then
					update %I set %I=0; -- base_table_name, base_aggregate_column
				END IF;
				RETURN NEW;
			END;
			$inner_trg$ LANGUAGE plpgsql;
		$fun$
			, id
			, base_table_name, base_aggregate_column, base_aggregate_column, linked_value_column, base_pk, linked_fk
			, base_table_name, base_aggregate_column, base_aggregate_column, linked_value_column, base_pk, linked_fk
			, linked_fk, linked_fk
			, base_table_name, base_aggregate_column, base_aggregate_column, linked_value_column, base_pk, linked_fk
			, base_table_name, base_aggregate_column, base_aggregate_column, linked_value_column, base_pk, linked_fk
			, base_table_name, base_aggregate_column
		);

	execute format($inner_proc$
		CREATE or replace PROCEDURE "_pgf_internal_refresh_%I"() -- id
		LANGUAGE plpgsql
		AS $inner_proc2$
			begin
			    update %I set %I = sub.cpt -- base_table_name, base_aggregate_column
				from (
					select %I as id, sum(%I) as cpt -- linked_fk, linked_value_column
					from %I -- linked_table_name
					group by %I -- linked_fk
				) as sub
				where %I.%I = sub.id; -- base_table_name, base_pk
			end;
			$inner_proc2$;
		$inner_proc$
		, id -- function name
		, base_table_name, base_aggregate_column
		, linked_fk, linked_value_column
		, linked_table_name
		, linked_fk
		, base_table_name, base_pk
	);

    execute format($trg$
		CREATE or replace TRIGGER _pgf_internal_sum_trg_%I -- id
		after delete or insert or update ON %I -- linked_table_name
		FOR EACH ROW
		execute procedure _pgf_internal_sum_trgfun_%I(); -- id
		$trg$,
		id,
		linked_table_name,
		id
	);

    execute format($trg$
		CREATE or replace TRIGGER _pgf_internal_sum_trg_truncate_%I -- id
		after truncate ON %I -- linked_table_name
		FOR EACH STATEMENT
		execute procedure _pgf_internal_sum_trgfun_%I(); -- id
		$trg$,
		id,
		linked_table_name,
		id
	);

	call pgf_refresh(id);

END;
$proc$;


--------------------------------------------------------------------------------
-- MINMAX_TABLE
--------------------------------------------------------------------------------
CREATE or replace PROCEDURE pgf_minmax_table (
	id text,
    table_name TEXT,
	pk TEXT,
    aggregate_column TEXT,
	options JSONB
)
LANGUAGE plpgsql AS $proc$
DECLARE
	group_by_column TEXT[];
    agg_table TEXT;
	str TEXT := ''; -- string buffer
	c TEXT; -- loop index
	i int; -- loop index
	group_by_columns_joined TEXT; -- group by column names, joined with ','
	group_by_columns_new_joined TEXT; -- group by column names where each name is prefixed with 'NEW.', joined with ','
	where_condition_on_group_by TEXT := ''; -- SQL fragment : "grp1 = OLD.grp1 AND grp2 = OLD.grp2..."
	where_condition_on_group_by_OLDNEW TEXT := ''; -- SQL fragment : "OLD.grp1 = NEW.grp1 AND OLD.grp2 = NEW.grp2..."
	where_condition_on_group_by_qual TEXT := ''; -- SQL fragment : "grp1 = table_name.grp1 AND grp2 = table_name.grp2..."
BEGIN

	create table if not exists log(msg text);

	-- set default values for optional arguments
	options := jsonb_build_object(
		'group_by_column', '[]'::jsonb,
		'agg_table', table_name || '_minmax'
	) || options;

	call _pgf_internal_insert_metadata(id, 'minmax_table', jsonb_build_object(
		'table_name', table_name,
		'pk', pk,
		'aggregate_column', aggregate_column,
		'options', options
	));
	

	group_by_column = _pgf_internal_jsonb_to_text_array(options->'group_by_column');
	agg_table = options->>'agg_table';


	group_by_columns_joined := _pgf_internal_join(group_by_column);
	group_by_columns_new_joined := _pgf_internal_join(group_by_column, 'NEW.%s');
	
	-- init where condition SQL fragments
	where_condition_on_group_by := _pgf_internal_join(group_by_column, '%s = OLD.%s', ' AND ');
	where_condition_on_group_by_qual := _pgf_internal_join(group_by_column, '%s = ' || table_name || '.%s', ' AND ');
	where_condition_on_group_by_OLDNEW := _pgf_internal_join(group_by_column, 'OLD.%s = NEW.%s', ' AND ');
	
	-- create aggregate table
	execute format($tbl$
		create table %I as
		select %s, %I as min_value, %I as id_of_min, %I as max_value, %I as id_of_max, 0::bigint as row_count
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
	str := format($fun$
		CREATE OR REPLACE FUNCTION _pgf_internal_minmax_table_trgfun_%I() --id
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
							min_value = (select min(%I) from %I where %s), -- aggregate_column, table_name, where_condition_on_group_by
							id_of_min = (select %I from %I where %s order by %I asc limit 1) --pk, table_name, where_condition_on_group_by, aggregate_column
						where %s; -- where_condition_on_group_by
					end if;
					if id_of_max_val = OLD.%I then -- pk
						update %I set -- agg_table
							max_value = (select max(%I) from %I where %s), -- aggregate_column, table_name, where_condition_on_group_by
							id_of_max = (select %I from %I where %s order by %I desc limit 1) --pk, table_name, where_condition_on_group_by, aggregate_column
						where %s; -- where_condition_on_group_by
					end if;
		$fun$
		, id
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
		, aggregate_column, table_name, where_condition_on_group_by
		, pk, table_name, where_condition_on_group_by, aggregate_column
		, where_condition_on_group_by
		, pk
		, agg_table
		, aggregate_column, table_name, where_condition_on_group_by
		, pk, table_name, where_condition_on_group_by, aggregate_column
		, where_condition_on_group_by
		) || format($fun$
				ELSIF TG_OP = 'UPDATE' then
					/* Only update if group by columns changed */
					/* compare each group by column between OLD and NEW */
					IF NOT (%s) THEN -- where_condition_on_group_by_OLDNEW
						/* Decrement row_count and update min/max for OLD group */
						select id_of_min, id_of_max
						into id_of_min_val, id_of_max_val
						from %I -- agg_table
						where %s; -- where_condition_on_group_by

						update %I set row_count=row_count-1 -- agg_table
						where %s; -- where_condition_on_group_by

						if id_of_min_val = OLD.%I then -- pk
							update %I set -- agg_table
								min_value = (select min(%I) from %I where %s), -- aggregate_column, table_name, where_condition_on_group_by
								id_of_min = (select %I from %I where %s order by %I asc limit 1) -- pk, table_name, where_condition_on_group_by, aggregate_column
							where %s; -- where_condition_on_group_by
						end if;
						if id_of_max_val = OLD.%I then -- pk
							update %I set -- agg_table
								max_value = (select max(%I) from %I where %s), -- aggregate_column, table_name, where_condition_on_group_by
								id_of_max = (select %I from %I where %s order by %I desc limit 1) -- pk, table_name, where_condition_on_group_by, aggregate_column
							where %s; -- where_condition_on_group_by
						end if;

						/* Increment row_count and update min/max for NEW group */
						insert into %I(%s, min_value, id_of_min, max_value, id_of_max, row_count) -- agg_table, group_by_columns_joined
						values(%s, NEW.%I, NEW.%I, NEW.%I, NEW.%I, 1) -- group_by_columns_new_joined, aggregate_column, pk, aggregate_column, pk
						on conflict(%s) do update set -- group_by_columns_joined
							min_value = least(%I.min_value, NEW.%I), -- agg_table, aggregate_column
							id_of_min=case when NEW.%I < %I.min_value then NEW.%I else %I.id_of_min END, -- aggregate_column, agg_table, pk, agg_table
							max_value=GREATEST(%I.max_value, NEW.%I), -- agg_table, aggregate_column
							id_of_max=case when  NEW.%I > %I.max_value then NEW.%I else %I.id_of_max END, -- aggregate_column, agg_table, pk, agg_table
							row_count=%I.row_count+1; -- agg_table
					/* If group by columns did not change, only update min/max if aggregate_column changed */
					ELSIF OLD.%I <> NEW.%I THEN -- aggregate_column, aggregate_column
						update %I set -- agg_table
							min_value = (select min(%I) from %I where %s), -- aggregate_column, table_name, where_condition_on_group_by
							id_of_min = (select %I from %I where %s order by %I asc limit 1), -- pk, table_name, where_condition_on_group_by, aggregate_column
							max_value = (select max(%I) from %I where %s), -- aggregate_column, table_name, where_condition_on_group_by
							id_of_max = (select %I from %I where %s order by %I desc limit 1) -- pk, table_name, where_condition_on_group_by, aggregate_column
						where %s; -- where_condition_on_group_by
					END IF;
				END IF;
				RETURN NEW;
			END;
			$inner_trg$ LANGUAGE plpgsql;
		$fun$
		, where_condition_on_group_by_OLDNEW
		, agg_table
		, where_condition_on_group_by
		, agg_table
		, where_condition_on_group_by
		, pk
		, agg_table
		, aggregate_column, table_name, where_condition_on_group_by
		, pk, table_name, where_condition_on_group_by, aggregate_column
		, where_condition_on_group_by
		, pk
		, agg_table
		, aggregate_column, table_name, where_condition_on_group_by
		, pk, table_name, where_condition_on_group_by, aggregate_column
		, where_condition_on_group_by
		, agg_table, group_by_columns_joined
		, group_by_columns_new_joined, aggregate_column, pk, aggregate_column, pk
		, group_by_columns_joined
		, agg_table, aggregate_column
		, aggregate_column, agg_table, pk, agg_table
		, agg_table, aggregate_column
		, aggregate_column, agg_table, pk, agg_table
		, agg_table
		, aggregate_column, aggregate_column
		, agg_table
		, aggregate_column, table_name, where_condition_on_group_by
		, pk, table_name, where_condition_on_group_by, aggregate_column
		, aggregate_column, table_name, where_condition_on_group_by
		, pk, table_name, where_condition_on_group_by, aggregate_column
		, where_condition_on_group_by
	);
	execute str;

    execute format($trg$
		CREATE or replace TRIGGER _pgf_internal_minmax_table_trg_%I
		after delete or insert or update ON %I
		FOR EACH ROW
		execute procedure _pgf_internal_minmax_table_trgfun_%I();
		$trg$,
		id,
		quote_ident(table_name),
		id
	);

	execute format($inner_proc$
		CREATE or replace PROCEDURE _pgf_internal_refresh_%I() -- id
		LANGUAGE plpgsql
		AS $body$
			begin
			    delete from %I; -- agg_table

				WITH t_ranked AS (
				SELECT
					%s, -- group_by_columns_joined
					%I, -- pk
					%I, -- aggregate_column
					ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %I ASC) AS rn_min, -- group_by_columns_joined, aggregate_column
					ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %I DESC) AS rn_max, -- group_by_columns_joined, aggregate_column
					COUNT(*)  OVER (PARTITION BY %s) AS row_count -- group_by_columns_joined
				FROM %I -- table_name
				)
				insert into %I -- agg_table
				SELECT
				%s, -- group_by_columns_joined
				MIN(CASE WHEN rn_min = 1 THEN %I END) AS min_value, -- aggregate_column
				MIN(CASE WHEN rn_min = 1 THEN %I END) AS id_of_min, -- pk
				MAX(CASE WHEN rn_max = 1 THEN %I END) AS max_value, -- aggregate_column
				MAX(CASE WHEN rn_max = 1 THEN %I END) AS id_of_max, -- pk
				MIN(row_count) as row_count
				FROM t_ranked
				GROUP BY %s; -- group_by_columns_joined
			end;
			$body$;
		$inner_proc$
		, id
		, agg_table
		, group_by_columns_joined
		, pk
		, aggregate_column
		, group_by_columns_joined, aggregate_column
		, group_by_columns_joined, aggregate_column
		, group_by_columns_joined
		, table_name
		, agg_table
		, group_by_columns_joined
		, aggregate_column
		, pk
		, aggregate_column
		, pk
		, group_by_columns_joined
	);

	call pgf_refresh(id);
END;
$proc$;


--------------------------------------------------------------------------------
-- TREELEVEL
--------------------------------------------------------------------------------
-- TREELEVEL: Update a "level" column in a table representing a tree structure.
-- Arguments:
--   id TEXT: Unique identifier for this trigger set
--   table_name TEXT: Name of the table
--   pk_column TEXT: Name of the primary key column
--   parent_column TEXT: Name of the column referencing the parent node (nullable for root)
--   level_column TEXT: Name of the column to store the level (integer, must exist in table)
CREATE OR REPLACE PROCEDURE pgf_treelevel(
    id TEXT,
    table_name TEXT,
    pk_column TEXT,
    parent_column TEXT,
    level_column TEXT
) LANGUAGE plpgsql AS $proc$
DECLARE
    trg_func_name TEXT := format('_pgf_internal_treelevel_trgfun_%s', id);
    trg_name TEXT := format('_pgf_internal_treelevel_trg_%s', id);
BEGIN
	call _pgf_internal_insert_metadata(id, 'treelevel', jsonb_build_object(
		'table_name', table_name,
		'pk_column', pk_column,
		'parent_column', parent_column,
		'level_column', level_column
	));

    -- Create the trigger function
    execute format($f$
        CREATE OR REPLACE FUNCTION %I() -- trg_func_name
        RETURNS TRIGGER AS $$
        DECLARE
            new_level INT;
            old_level INT;
        BEGIN
            /* Compute new level for the current node */
            IF NEW.%I IS NULL THEN -- parent_column
                new_level := 0;
            ELSE
                SELECT COALESCE(%I, 0) + 1 INTO new_level -- level_column
                FROM %I WHERE %I = NEW.%I; -- table_name, pk_column, parent_column
            END IF;
            old_level := NEW.%I; -- level_column
            NEW.%I := new_level; -- level_column

            /* Only update children if the level actually changed */
            IF TG_OP = 'UPDATE' AND new_level != old_level THEN
				WITH RECURSIVE node_levels AS (
  				SELECT
					%I, -- pk_column
					%I, -- parent_column
					new_level AS level
				FROM %I -- table_name
				WHERE %I = NEW.%I -- pk_column, pk_column
				UNION ALL
				SELECT
					n.%I, -- pk_column
					n.%I, -- parent_column
					nl.level + 1 as level
				FROM %I n -- table_name
				JOIN node_levels nl ON n.%I = nl.%I -- parent_column, pk_column
				)
				UPDATE %I -- table_name
				SET %I = node_levels.level -- level_column
				FROM node_levels
				WHERE %I.%I = node_levels.%I --table_name, pk_column, pk_column
				AND node_levels.%I <> NEW.%I; /* modifying current row is forbidden in BEFORE triggers */ -- pk_column, pk_column
            END IF;

            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    $f$,
	  trg_func_name
	, parent_column
	, level_column
	, table_name
	, pk_column, parent_column
	, level_column
	, level_column
	, pk_column
	, parent_column
	, table_name
	, pk_column, pk_column
	, pk_column
	, parent_column
	, table_name
	, parent_column, pk_column
	, table_name
	, level_column
	, table_name, pk_column, pk_column
	, pk_column, pk_column
	);

    -- Drop existing trigger if exists
    execute format('DROP TRIGGER IF EXISTS %I ON %I;', trg_name, table_name);

    -- Create the trigger
    execute format($trg$
        CREATE TRIGGER %I BEFORE INSERT OR UPDATE OF %I ON %I -- trg_name, parent_column, table_name
         FOR EACH ROW EXECUTE FUNCTION %I(); -- trg_func_name
		 $trg$
		 ,
        trg_name, parent_column, table_name,
		trg_func_name
    );

	execute format($inner_proc$
		CREATE OR REPLACE PROCEDURE _pgf_internal_refresh_%I() -- id
		LANGUAGE plpgsql AS $inner_proc2$
		BEGIN
			-- Full refresh: update all levels in the table
			execute format($f$
				WITH RECURSIVE node_levels AS (
				SELECT
					%I, -- pk_column
					%I, -- parent_column
					0 AS level
				FROM %I -- table_name
				WHERE %I is null -- parent_column
				UNION ALL
				SELECT
					n.%I, -- pk_column
					n.%I, -- parent_column
					nl.level + 1 as level
				FROM %I n -- table_name
				JOIN node_levels nl ON n.%I = nl.%I -- parent_column, pk_column
				)
				UPDATE %I -- table_name
				SET %I = node_levels.level -- level_column
				FROM node_levels
				WHERE %I.%I = node_levels.%I --table_name, pk_column, pk_column
			$f$
			);
		END;
		$inner_proc2$
	$inner_proc$
	, id
	, pk_column
	, parent_column
	, table_name
	, parent_column
	, pk_column
	, parent_column
	, table_name
	, parent_column, pk_column
	, table_name
	, level_column
	, table_name, pk_column, pk_column
	);
    -- Full refresh: update all levels in the table
	call pgf_refresh(id);

END;
$proc$;


--------------------------------------------------------------------------------
-- INHERITANCE_TABLE
--------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE pgf_inheritance_table(
    id TEXT,
    base_table_name TEXT,
    sub_tables TEXT[],
    sync_direction TEXT,
	options JSONB DEFAULT '{}'::JSONB
)
LANGUAGE plpgsql
AS $proc$
DECLARE
    i INT;
    all_columns TEXT[];
    col_name TEXT;
    col_type TEXT;
    col_defs TEXT := '';
    cols_union TEXT := '';
    sub_cols TEXT[];
    sub_col_names TEXT;
    insert_cols TEXT;
    select_expr TEXT;
	sql TEXT;
	discriminator_column TEXT;
	discriminator_values TEXT[];
BEGIN
	-- Apply default values to options
	options := jsonb_build_object(
        'discriminator_column', 'discriminator',
		'discriminator_values', sub_tables
    ) || options;
	discriminator_column := options->>'discriminator_column';
	discriminator_values := _pgf_internal_jsonb_to_text_array(options->'discriminator_values');

	call _pgf_internal_insert_metadata(id, 'inheritance_table', jsonb_build_object(
		'base_table_name', base_table_name,
		'sub_tables', sub_tables,
		'sync_direction', sync_direction,
		'options', options
	));

    -- Gather all columns from all sub-tables (excluding duplicates)
    col_defs := format('%I TEXT', discriminator_column);
    FOR col_name, col_type IN
		select column_name, data_type from (
			SELECT DISTINCT on (column_name) column_name, data_type, ordinal_position
			FROM information_schema.columns
			WHERE table_name = ANY(sub_tables)
			AND column_name <> discriminator_column
		) t order by ordinal_position
	LOOP
        col_defs := col_defs || format(', %I %s', col_name, col_type);
		all_columns := all_columns || col_name;
    END LOOP;

    -- Create union table with all columns
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I (%s);', base_table_name, col_defs);

    -- create refresh procedure: copy all sub-tables into base table
	sql := '';
    FOR i IN 1..array_length(sub_tables, 1) LOOP
        -- Get columns for this sub-table
        sub_cols := ARRAY[]::TEXT[];
        sub_col_names := '';
        FOR col_name IN
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = sub_tables[i]
              AND column_name <> discriminator_column
        LOOP
            sub_cols := array_append(sub_cols, col_name);
        END LOOP;

        -- Build insert column list for union table
        insert_cols := '';
        select_expr := '';
        FOR col_name IN SELECT unnest(all_columns) LOOP
            insert_cols := insert_cols || format('%I, ', col_name);
            IF col_name = ANY(sub_cols) THEN
                select_expr := select_expr || format('%I, ', col_name);
            ELSE
                select_expr := select_expr || 'NULL, ';
            END IF;
        END LOOP;
        insert_cols := insert_cols || format('%I', discriminator_column);
        select_expr := select_expr || format('%L', discriminator_values[i]);

        -- Insert data from sub-table
        sql := sql || format(
            'INSERT INTO %I (%s) SELECT %s FROM %I;',
            base_table_name, insert_cols, select_expr, sub_tables[i]
        );
    END LOOP;

	execute format($inner_proc$
		CREATE OR REPLACE PROCEDURE _pgf_internal_refresh_%I() -- id
		LANGUAGE plpgsql AS $inner_proc2$
		BEGIN
			%s -- sql
		END;
		$inner_proc2$
	$inner_proc$
	, id
	, sql
	);

    -- Create triggers for sync_direction
    IF sync_direction = 'SUB_TO_BASE' THEN
        -- Propagate changes from sub-tables to base table
        FOR i IN 1..array_length(sub_tables, 1) LOOP
            -- Get columns for this sub-table
            sub_cols := ARRAY[]::TEXT[];
            FOR col_name IN
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = sub_tables[i]
                  AND column_name <> discriminator_column
            LOOP
                sub_cols := array_append(sub_cols, col_name);
            END LOOP;

            -- Build insert column list and select expr for NEW row
            insert_cols := '';
            select_expr := '';
            FOR col_name IN SELECT unnest(all_columns) LOOP
                insert_cols := insert_cols || format('%I, ', col_name);
                IF col_name = ANY(sub_cols) THEN
                    select_expr := select_expr || format('NEW.%I, ', col_name);
                ELSE
                    select_expr := select_expr || 'NULL, ';
                END IF;
            END LOOP;
            insert_cols := insert_cols || format('%I', discriminator_column);
            select_expr := select_expr || format('%L', discriminator_values[i]);

            EXECUTE format($f$
                CREATE OR REPLACE FUNCTION _pgf_internal_inheritance_table_trgfun_%s_%s() -- id, sub_tables[i]
                RETURNS TRIGGER AS $$
                BEGIN
                    IF TG_OP = 'INSERT' THEN
                        INSERT INTO %I (%s) VALUES (%s); -- base_table_name, insert_cols, select_expr
                    ELSIF TG_OP = 'UPDATE' THEN
                        UPDATE %I SET (%s) = (%s) -- base_table_name, insert_cols, select_expr
                        WHERE id = NEW.id AND %I = %L; -- discriminator_column, discriminator_values[i]
                    ELSIF TG_OP = 'DELETE' THEN
                        DELETE FROM %I WHERE id = OLD.id AND %I = %L; -- base_table_name, discriminator_column, discriminator_values[i]
                    END IF;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
            $f$, id, sub_tables[i], base_table_name, insert_cols, select_expr,
                base_table_name, insert_cols, select_expr,
				discriminator_column, discriminator_values[i],
                base_table_name, discriminator_column, discriminator_values[i]);

            EXECUTE format($t$
                CREATE TRIGGER _pgf_internal_inheritance_table_trg_%s_%s
                AFTER INSERT OR UPDATE OR DELETE ON %I
                FOR EACH ROW EXECUTE PROCEDURE _pgf_internal_inheritance_table_trgfun_%s_%s();
            $t$, id, sub_tables[i], sub_tables[i], id, sub_tables[i]);
        END LOOP;
    ELSE
        -- Propagate changes from base table to sub-tables
        FOR i IN 1..array_length(sub_tables, 1) LOOP
            -- Get columns for this sub-table
            sub_cols := ARRAY[]::TEXT[];
            FOR col_name IN
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = sub_tables[i]
                  AND column_name <> discriminator_column
            LOOP
                sub_cols := array_append(sub_cols, col_name);
            END LOOP;

            -- Build insert column list and select expr for NEW row
            insert_cols := '';
            select_expr := '';
            FOR col_name IN SELECT unnest(sub_cols) LOOP
                insert_cols := insert_cols || format('%I, ', col_name);
                select_expr := select_expr || format('NEW.%I, ', col_name);
            END LOOP;
            -- Remove trailing comma
            IF length(insert_cols) > 0 THEN
                insert_cols := left(insert_cols, length(insert_cols)-2);
                select_expr := left(select_expr, length(select_expr)-2);
            END IF;

            EXECUTE format($f$
                CREATE OR REPLACE FUNCTION _pgf_internal_inheritance_table_trgfun_%s_%s() -- id, sub_tables[i]
                RETURNS TRIGGER AS $$
                BEGIN
					IF TG_OP = 'INSERT' AND NEW.%I = %L THEN -- discriminator_column, discriminator_values[i]
						INSERT INTO %I (%s) VALUES (%s); -- sub_tables[i], insert_cols, select_expr
					ELSIF TG_OP = 'UPDATE' AND NEW.%I = %L THEN -- -- discriminator_column, discriminator_values[i]
						UPDATE %I SET (%s) = (%s) WHERE id = NEW.id; -- sub_tables[i], insert_cols, select_expr
					ELSIF TG_OP = 'DELETE' AND OLD.%I = %L THEN -- -- discriminator_column, discriminator_values[i]
						DELETE FROM %I WHERE id = OLD.id; -- sub_tables[i]
                    END IF;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
            $f$
				, id, sub_tables[i]
				, discriminator_column, discriminator_values[i]
				, sub_tables[i], insert_cols, select_expr
				, discriminator_column, discriminator_values[i]
				, sub_tables[i], insert_cols, select_expr
				, discriminator_column, discriminator_values[i]
				, sub_tables[i]
			);

            EXECUTE format($t$
                CREATE TRIGGER _pgf_internal_inheritance_table_trg_%s_%s
                AFTER INSERT OR UPDATE OR DELETE ON %I
                FOR EACH ROW EXECUTE PROCEDURE _pgf_internal_inheritance_table_trgfun_%s_%s();
            $t$, id, sub_tables[i], base_table_name, id, sub_tables[i]);
        END LOOP;
    END IF;

	call pgf_refresh(id);

END;
$proc$;


--------------------------------------------------------------------------------
-- AUDIT_TABLE
--------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE pgf_audit_table(
    id TEXT,
    audit_table_name TEXT,
	audited_table_names TEXT[],
    options JSONB DEFAULT '{}'::JSONB
)
LANGUAGE plpgsql
AS $proc$
DECLARE
    i INT;
    operation_column_name TEXT;
    operations_mapping JSONB;
    old_value_column_name TEXT;
    new_value_column_name TEXT;
    op_insert TEXT;
    op_update TEXT;
    op_delete TEXT;
    trg_func_name TEXT;
    trg_name TEXT;
	is_insert_audited TEXT; -- 'true' if insert operations are audited, 'false' otherwise
	is_update_audited TEXT; -- 'true' if update operations are audited, 'false' otherwise
	is_delete_audited TEXT; -- 'true' if delete operations are audited, 'false' otherwise
BEGIN
    -- Set default options
    options := jsonb_build_object(
		'operation_column_name', 'OPERATION',
        'operations_mapping', jsonb_build_object('INSERT', 'INSERT', 'UPDATE', 'UPDATE', 'DELETE', 'DELETE'),
        'old_value_column_name', 'OLD_VALUE',
        'new_value_column_name', 'NEW_VALUE',
		'audited_operations', json_build_array('INSERT', 'UPDATE', 'DELETE')
	) || options;

    operation_column_name := options->>'operation_column_name';
    operations_mapping := options->'operations_mapping';
    old_value_column_name := options->>'old_value_column_name';
    new_value_column_name := options->>'new_value_column_name';

    op_insert := operations_mapping->>'INSERT';
    op_update := operations_mapping->>'UPDATE';
    op_delete := operations_mapping->>'DELETE';

	is_insert_audited = (options->'audited_operations' @> '["INSERT"]'::jsonb)::text; -- @> is the 'contains' JSONB operator
	is_update_audited = (options->'audited_operations' @> '["UPDATE"]'::jsonb)::text; -- @> is the 'contains' JSONB operator
	is_delete_audited = (options->'audited_operations' @> '["DELETE"]'::jsonb)::text; -- @> is the 'contains' JSONB operator

    -- Insert metadata
    call _pgf_internal_insert_metadata(id, 'audit_table', jsonb_build_object(
        'audited_table_names', audited_table_names,
        'audit_table_name', audit_table_name,
        'options', options
    ));

    -- Create audit table if not exists
    execute format(
        'CREATE TABLE IF NOT EXISTS %I (
            id SERIAL PRIMARY KEY,
            table_name TEXT,
            %s TEXT,
            %s JSONB,
            %s JSONB,
            event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );',
        audit_table_name,
        operation_column_name,
        old_value_column_name,
        new_value_column_name
    );

    -- Create triggers for each audited table
    FOR i IN 1..array_length(audited_table_names, 1) LOOP
        trg_func_name := format('_pgf_internal_audit_table_trgfun_%s_%s', id, audited_table_names[i]);
        trg_name := format('_pgf_internal_audit_table_trg_%s_%s', id, audited_table_names[i]);

        EXECUTE format($f$
            CREATE OR REPLACE FUNCTION %I() -- trg_func_name
            RETURNS TRIGGER AS $$
            BEGIN
                IF %s AND TG_OP = 'INSERT' THEN -- is_insert_audited
                    INSERT INTO %I(table_name, %s, %s, %s) -- audit_table_name, operation_column_name, old_value_column_name, new_value_column_name
                    VALUES (
                        TG_TABLE_NAME,
                        %L, -- op_insert
                        NULL,
                        to_jsonb(NEW)
                    );
                ELSIF %s and TG_OP = 'UPDATE' THEN --  -- is_update_audited
                    INSERT INTO %I(table_name, %s, %s, %s) -- audit_table_name, operation_column_name, old_value_column_name, new_value_column_name
                    VALUES (
                        TG_TABLE_NAME,
                        %L, -- op_update
                        to_jsonb(OLD),
                        to_jsonb(NEW)
                    );
                ELSIF %s and TG_OP = 'DELETE' THEN --  -- is_delete_audited
                    INSERT INTO %I(table_name, %s, %s, %s) -- audit_table_name, operation_column_name, old_value_column_name, new_value_column_name
                    VALUES (
                        TG_TABLE_NAME,
                        %L, -- op_delete
                        to_jsonb(OLD),
                        NULL
                    );
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        $f$
			, trg_func_name
			, is_insert_audited
			, audit_table_name, operation_column_name, old_value_column_name, new_value_column_name
			, op_insert
			, is_update_audited
			, audit_table_name, operation_column_name, old_value_column_name, new_value_column_name
			, op_update
			, is_delete_audited
			, audit_table_name, operation_column_name, old_value_column_name, new_value_column_name
			, op_delete
        );

        EXECUTE format($t$
            DROP TRIGGER IF EXISTS %I ON %I;
            CREATE TRIGGER %I
            AFTER INSERT OR UPDATE OR DELETE ON %I
            FOR EACH ROW EXECUTE PROCEDURE %I();
        $t$,
            trg_name, audited_table_names[i],
            trg_name, audited_table_names[i],
            trg_func_name
        );
    END LOOP;
END;
$proc$;

--------------------------------------------------------------------------------
-- SYNC
--------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE pgf_sync(
    id TEXT,
    table_name TEXT,
    column1 TEXT,
    column2 TEXT
)
LANGUAGE plpgsql
AS $proc$
DECLARE
    trg_func_name TEXT := format('_pgf_internal_sync_trgfun_%s', id);
    trg_name TEXT := format('_pgf_internal_sync_trg_%s', id);
BEGIN
    -- Insert metadata
    call _pgf_internal_insert_metadata(id, 'sync', jsonb_build_object(
        'table_name', table_name,
        'column1', column1,
        'column2', column2
    ));

    -- Create trigger function
    EXECUTE format($f$
        CREATE OR REPLACE FUNCTION %I() -- trg_func_name
        RETURNS TRIGGER AS $$
        BEGIN
            IF TG_OP = 'INSERT' THEN
                IF NEW.%I IS NOT NULL THEN -- column1
                    NEW.%I := NEW.%I; -- column2, column1
                ELSE
                    NEW.%I := NEW.%I; -- column1, column2
                END IF;
            ELSIF TG_OP = 'UPDATE' THEN
                IF (OLD.%I IS DISTINCT FROM NEW.%I) THEN -- column1, column1
                    NEW.%I := NEW.%I; -- column2, column1
                ELSIF (OLD.%I IS DISTINCT FROM NEW.%I) THEN -- column2, column2
                    NEW.%I := NEW.%I; -- column1, column2
                END IF;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    $f$
		, trg_func_name
		, column1
		, column2, column1
		, column1, column2
		, column1, column1
		, column2, column1
		, column2, column2
		, column1, column2
    );

    -- Drop existing trigger if exists
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I;', trg_name, table_name);

    -- Create trigger
    EXECUTE format($trg$
        CREATE TRIGGER %I -- trg_name
        BEFORE INSERT OR UPDATE ON %I -- table_name
        FOR EACH ROW EXECUTE FUNCTION %I(); -- trg_func_name
    $trg$,
        trg_name,
		table_name,
		trg_func_name
    );

	-- create refresh procedure
	execute format($inner_proc$
		CREATE or replace PROCEDURE "_pgf_internal_refresh_%I"() -- id
		LANGUAGE plpgsql
		AS $inner_proc2$
			begin
			    update %I -- table_name
				set %I = CASE WHEN %I IS NULL THEN %I ELSE %I END, -- column1, column1, column2, column1
				%I = CASE WHEN %I IS NULL THEN %I ELSE %I END; -- column2, column1, column2, column1
			end;
			$inner_proc2$;
		$inner_proc$
		, id
		, table_name
		, column1, column1, column2, column1
		, column2, column1, column2, column1
	);

	-- refresh
	call pgf_refresh(id);

END;
$proc$;

--------------------------------------------------------------------------------
-- INTERSECT_TABLE
--------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE pgf_intersect_table(
    id TEXT,
    table_names TEXT[],
	column_names TEXT[],
    intersect_table_name TEXT
)
LANGUAGE plpgsql
AS $proc$
DECLARE
	column_names_joined TEXT; -- SQL fragment: 'column1, column2, ...'
	column_names_joined_OLD TEXT; -- SQL fragment: 'OLD.column1, OLD.column2, ...'
	column_names_joined_NEW TEXT; -- SQL fragment: 'NEW.column1, NEW.column2, ...'
	row_count_column_names_joined TEXT; -- SQL fragment: 'pgf_row_count_table1, pgf_row_count_table1, ...'
	i int; -- loop index
	t text; -- loop variable
	pgf_row_count_gt_0_clause TEXT; -- SQL fragment : 'pgf_row_count_table1 > 0 AND pgf_row_count_table2 > 0 AND ...'
	pgf_row_count_eq_0_clause TEXT; -- SQL fragment: 'pgf_row_count_table1 = 0 AND pgf_row_count_table2 = 0 AND ...'
	pgf_row_count_def_fragment TEXT; -- SQL fragment
	inner_table_fragment TEXT; -- SQL fragment
BEGIN
    -- Insert metadata
    call _pgf_internal_insert_metadata(id, 'intersect_table', jsonb_build_object(
        'table_names', table_names,
        'column_names', column_names,
        'intersect_table_name', intersect_table_name
    ));

	-- prepare sql fragments
	row_count_column_names_joined := _pgf_internal_join(table_names, '0::bigint as pgf_row_count_%s', quote_fragments => false);
	column_names_joined := _pgf_internal_join(column_names);
	column_names_joined_OLD := _pgf_internal_join(column_names, 'OLD.%s');
	column_names_joined_NEW := _pgf_internal_join(column_names, 'NEW.%s');
	pgf_row_count_gt_0_clause := _pgf_internal_join(table_names, 'pgf_row_count_%s > 0', delimiter => ' AND ');
	pgf_row_count_eq_0_clause := _pgf_internal_join(table_names, 'pgf_row_count_%s = 0', delimiter => ' AND ');

	-- create intersection table
	execute format($$
		create table %I -- intersect_table_name
		as select %s, %s  -- column_names_joined, row_count_column_names_joined
		from %I -- table_names[1]
		limit 0;
	$$
		, intersect_table_name
		, column_names_joined, row_count_column_names_joined
		, table_names[1]
	);

	execute format('alter table %I add column is_intersect boolean generated always as (%s) stored', intersect_table_name, pgf_row_count_gt_0_clause);
	execute format('alter table %I add primary key (%s)', intersect_table_name, column_names_joined);
	for i in 1..array_length(table_names, 1) loop
		execute format('alter table %I alter column pgf_row_count_%I set default 0;', intersect_table_name, table_names[i]);
	end loop;

    -- Create trigger functions
	foreach t in array table_names loop
		EXECUTE format($f$
			CREATE OR REPLACE FUNCTION _pgf_internal_intersect_table_trgfun_%I_%I() -- id, t
			RETURNS TRIGGER AS $$
			BEGIN
				/* increment new row */
				IF TG_OP = 'INSERT' or (TG_OP = 'UPDATE' and (%s) <> (%s)) THEN -- column_names_joined_OLD, column_names_joined_NEW
					insert into %I(%s, pgf_row_count_%I) values -- intersect_table_name, column_names_joined, t
					(%s, 1) -- column_names_joined_NEW
					on conflict(%s) do update set pgf_row_count_%I = %I.pgf_row_count_%I + 1; -- column_names_joined, t, intersect_table_name, t
				end if;

				/* decrement (and optionally remove) old row */
				if TG_OP = 'DELETE' or (TG_OP = 'UPDATE' and (%s) <> (%s)) then -- column_names_joined_OLD, column_names_joined_NEW
					update %I -- intersect_table_name
					set pgf_row_count_%I = pgf_row_count_%I - 1 -- t, t
					where (%s) = (%s); -- column_names_joined, column_names_joined_OLD

					delete from %I -- intersect_table_name
					where (%s) = (%s) AND %s; -- column_names_joined, column_names_joined_OLD, pgf_row_count_eq_0_clause
				END IF;

				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql;
		$f$
			, id, t
			, column_names_joined_OLD, column_names_joined_NEW
			, intersect_table_name, column_names_joined, t
			, column_names_joined_NEW
			, column_names_joined, t, intersect_table_name, t
			, column_names_joined_OLD, column_names_joined_NEW
			, intersect_table_name
			, t, t
			, column_names_joined, column_names_joined_OLD
			, intersect_table_name
			, column_names_joined, column_names_joined_OLD, pgf_row_count_eq_0_clause
		);

		-- Drop existing trigger if exists
		EXECUTE format('DROP TRIGGER IF EXISTS _pgf_internal_intersect_table_trg_%I_%I ON %I;', id, t, t);

		-- Create trigger
		EXECUTE format($trg$
			CREATE TRIGGER _pgf_internal_intersect_table_trg_%I_%I -- id, t
			BEFORE INSERT OR UPDATE OR DELETE ON %I -- t
			FOR EACH ROW EXECUTE FUNCTION _pgf_internal_intersect_table_trgfun_%I_%I(); -- id, t
		$trg$
			, id, t
			, t
			, id, t
		);
	end loop;

	-- create refresh procedure
	
	pgf_row_count_def_fragment := _pgf_internal_join(table_names, 'count(*) filter (where pgf_source_table = ''%s'') as pgf_row_count_%s');
	inner_table_fragment := _pgf_internal_join(table_names, 'select ''%s'' as pgf_source_table, ' || column_names_joined || ' from %s', delimiter => ' UNION ALL ');

	execute format($inner_proc$
		CREATE or replace PROCEDURE "_pgf_internal_refresh_%I"() -- id
		LANGUAGE plpgsql
		AS $inner_proc2$
			begin
				delete from %I; -- intersect_table_name
				insert into %I -- intersect_table_name
				select %s, %s -- column_names_joined, pgf_row_count_def_fragment
				from (%s) t -- inner_table_fragment
				group by %s; -- column_names_joined
			end;
			$inner_proc2$;
		$inner_proc$
			, id
			, intersect_table_name
			, intersect_table_name
			, column_names_joined, pgf_row_count_def_fragment
			, inner_table_fragment
			, column_names_joined
	);

	-- refresh
	call pgf_refresh(id);

END;
$proc$;
