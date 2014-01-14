-- AUDIT.sql
--
-- Author:      Felix Kunde <fkunde@virtualcitysystems.de>
--
--              This skript is free software under the LGPL Version 3
--              See the GNU Lesser General Public License at
--              http://www.gnu.org/copyleft/lgpl.html
--              for more details.
-------------------------------------------------------------------------------
-- About:
-- This package allows auditing and versioning for PostgreSQL databases.
--
-------------------------------------------------------------------------------
--
-- ChangeLog:
--
-- Version | Date       | Description                       | Author
-- 1.0.0     2014-01-09   alpha version                       FKun
--

/**********************************************************
* AUDIT SCHEMA
*
* Addtional schema that contains the audit_log table and
* all functions and procedures to version the database.
*
* C-o-n-t-e-n-t:
* TABLES:
*   audit_log
*
* INDEXES:
*   audit_log_op_idx
*   audit_log_table_idx
*   audit_log_date_idx
*   audit_log_audit_idx
*
* FUNCTIONS:
*   create_schema_audit(schema_name VARCHAR DEFAULT 'public', except_tables VARCHAR[] DEFAULT '{}') RETURNS SETOF VOID
*   create_schema_audit_id(schema_name VARCHAR DEFAULT 'public', except_tables VARCHAR[] DEFAULT '{}') RETURNS SETOF VOID
*   create_schema_log_trigger(schema_name VARCHAR DEFAULT 'public', except_tables VARCHAR[] DEFAULT '{}') RETURNS SETOF VOID
*   create_table_audit(table_name VARCHAR, schema_name VARCHAR DEFAULT 'public') RETURNS SETOF VOID
*   create_table_audit_id(table_name VARCHAR, schema_name VARCHAR DEFAULT 'public') RETURNS SETOF VOID
*   create_table_log_trigger(table_name VARCHAR, schema_name VARCHAR DEFAULT 'public') RETURNS SETOF VOID
*   drop_schema_audit(schema_name VARCHAR DEFAULT 'public', except_tables VARCHAR[] DEFAULT '{}') RETURNS SETOF VOID
*   drop_schema_audit_id(schema_name VARCHAR DEFAULT 'public', except_tables VARCHAR[] DEFAULT '{}') RETURNS SETOF VOID
*   drop_schema_log_trigger(schema_name VARCHAR DEFAULT 'public', except_tables VARCHAR[] DEFAULT '{}') RETURNS SETOF VOID
*   drop_table(table_name VARCHAR, target_schema_name VARCHAR DEFAULT 'public') RETURNS SETOF VOID
*   drop_table_audit(table_name VARCHAR, schema_name VARCHAR DEFAULT 'public') RETURNS SETOF VOID
*   drop_table_audit_id(table_name VARCHAR, schema_name VARCHAR DEFAULT 'public') RETURNS SETOF VOID
*   drop_table_log_trigger(table_name VARCHAR, schema_name VARCHAR DEFAULT 'public') RETURNS SETOF VOID
*   drop_table_relations(table_name VARCHAR, target_schema_name VARCHAR DEFAULT 'public') RETURNS SETOF VOID
*   fkey_schema_state(target_schema_name VARCHAR, original_schema_name VARCHAR DEFAULT 'public', 
*     except_tables VARCHAR[] DEFAULT '{}') RETURNS SETOF VOID
*   fkey_table_state(table_name VARCHAR, target_schema_name VARCHAR, original_schema_name VARCHAR DEFAULT 'public') 
*     RETURNS SETOF VOID
*   get_log_entries(queried_date TIMESTAMP, search_date TIMESTAMP, original_table_name VARCHAR, original_schema_name VARCHAR)
*     RETURNS SETOF INTEGER
*   index_schema_state(target_schema_name VARCHAR, original_schema_name VARCHAR DEFAULT 'public', 
*     except_tables VARCHAR[] DEFAULT '{}') RETURNS SETOF VOID
*   index_table_state(table_name VARCHAR, target_schema_name VARCHAR, original_schema_name VARCHAR DEFAULT 'public') 
*     RETURNS SETOF VOID
*   log_schema_state(schema_name VARCHAR DEFAULT 'public', except_tables VARCHAR[] DEFAULT '{}') RETURNS SETOF VOID
*   log_table_state(table_name VARCHAR, schema_name VARCHAR DEFAULT 'public') RETURNS SETOF VOID
*   pkey_schema_state(target_schema_name VARCHAR, original_schema_name VARCHAR DEFAULT 'public', 
*     except_tables VARCHAR[] DEFAULT '{}') RETURNS SETOF VOID
*   pkey_table_state(table_name VARCHAR, target_schema_name VARCHAR, original_schema_name VARCHAR DEFAULT 'public') 
*     RETURNS SETOF VOID
*   produce_schema_state(queried_date TIMESTAMP, original_schema_name VARCHAR, target_schema_name VARCHAR, 
*     target_table_type VARCHAR DEFAULT 'VIEW', except_tables VARCHAR[] DEFAULT '{}') RETURNS SETOF VOID
*   produce_table_state(queried_date TIMESTAMP, original_table_name VARCHAR, original_schema_name VARCHAR, 
*     target_schema_name VARCHAR, target_table_type VARCHAR DEFAULT 'VIEW') RETURNS SETOF VOID
*   recreate_schema_state(schema_name VARCHAR, target_schema_name VARCHAR DEFAULT 'public', except_tables VARCHAR[] DEFAULT '{}') 
*     RETURNS SETOF VOID
*   recreate_table_state(table_name VARCHAR, schema_name VARCHAR, target_schema_name VARCHAR DEFAULT 'public') RETURNS SETOF VOID
*
* TRIGGER FUNCTIONS
*   log_change() RETURNS trigger
*
***********************************************************/
DROP SCHEMA audit CASCADE;
CREATE SCHEMA audit;

-- all changes are logged into the audit_log table
CREATE TABLE audit.audit_log
(
  id serial,
  table_operation VARCHAR(10),
  schema_name VARCHAR(50)
  table_name VARCHAR(50),
  db_date TIMESTAMP,
  audit_id INTEGER,
  table_content JSON
);

ALTER TABLE audit.audit_log
ADD CONSTRAINT audit_log_pk PRIMARY KEY (id);

-- create indexes on all columns that are queried later
CREATE INDEX audit_log_op_idx ON audit.audit_log (table_operation);
CREATE INDEX audit_log_table_idx ON audit.audit_log (table_name, schema_name);
CREATE INDEX audit_log_date_idx ON audit.audit_log (db_date);
CREATE INDEX audit_log_audit_idx ON audit.audit_log (audit_id);


/**********************************************************
* TABLE AUDIT
*
* Enables Audit for a specified table.
***********************************************************/
-- create Audit for one table
CREATE OR REPLACE FUNCTION audit.create_table_audit( 
  table_name VARCHAR,
  schema_name VARCHAR DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  -- create log trigger
  PERFORM audit.create_table_log_trigger(table_name, schema_name);

  -- add audit_id column
  PERFORM audit.create_table_audit_id(table_name, schema_name);

  -- log all entires of the table
  PERFORM audit.log_table_state(table_name, schema_name);
END;
$$
LANGUAGE plpgsql;

-- perform create_table_audit on multiple tables in one schema
CREATE OR REPLACE FUNCTION audit.create_schema_audit(
  schema_name VARCHAR DEFAULT 'public',
  except_tables VARCHAR[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE 'SELECT audit.create_table_audit(tablename::varchar, schemaname::varchar) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING schema_name, except_tables;
END;
$$
LANGUAGE plpgsql;

-- drop Audit for one table
CREATE OR REPLACE FUNCTION audit.drop_table_audit(
  t_name VARCHAR,
  s_name VARCHAR DEFAULT 'public' 
  ) RETURNS SETOF VOID AS
$$
BEGIN
  -- delete all entires of table in audit_log table
  EXECUTE 'DELETE FROM audit.audit_log WHERE table_name = $1 AND schema_name = $2'
             USING t_name, s_name;

  -- drop audit_id column
  PERFORM audit.drop_table_audit_id(t_name, s_name);

  -- drop log trigger
  PERFORM audit.drop_table_log_trigger(t_name, s_name);
END;
$$
LANGUAGE plpgsql;

-- perform drop_table_audit on multiple tables in one schema
CREATE OR REPLACE FUNCTION audit.drop_schema_audit(
  schema_name VARCHAR DEFAULT 'public',
  except_tables VARCHAR[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE 'SELECT audit.drop_table_audit(tablename::varchar, schemaname::varchar) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING schema_name, except_tables;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* LOGGING TRIGGER
*
* Define trigger on a table to fire events when rows are
* inserted, updated or deleted or when the table is truncated.
***********************************************************/
-- create logging triggers for one table
CREATE OR REPLACE FUNCTION audit.create_table_log_trigger( 
  table_name VARCHAR,
  schema_name VARCHAR DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE format('CREATE TRIGGER log_trigger AFTER INSERT OR UPDATE OR DELETE ON %I.%I
                    FOR EACH ROW EXECUTE PROCEDURE audit.log_change()', schema_name, table_name);
  EXECUTE format('CREATE TRIGGER log_truncate_trigger BEFORE TRUNCATE ON %I.%I
                    FOR EACH STATEMENT EXECUTE PROCEDURE audit.log_change()', schema_name, table_name);
END;
$$
LANGUAGE plpgsql;

-- perform create_table_log_trigger on multiple tables in one schema
CREATE OR REPLACE FUNCTION audit.create_schema_log_trigger(
  schema_name VARCHAR DEFAULT 'public',
  except_tables VARCHAR[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE 'SELECT audit.create_table_log_trigger(tablename::varchar, schemaname::varchar) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING schema_name, except_tables;
END;
$$
LANGUAGE plpgsql;

-- drop logging triggers for one table
CREATE OR REPLACE FUNCTION audit.drop_table_log_trigger(
  table_name VARCHAR,
  schema_name VARCHAR DEFAULT 'public' 
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE format('DROP TRIGGER IF EXISTS log_trigger ON %I.%I', schema_name, table_name);
  EXECUTE format('DROP TRIGGER IF EXISTS log_truncate_trigger ON %I.%I', schema_name, table_name);
END;
$$
LANGUAGE plpgsql;

-- perform drop_table_log_trigger on multiple tables in one schema
CREATE OR REPLACE FUNCTION audit.drop_schema_log_trigger(
  schema_name VARCHAR DEFAULT 'public',
  except_tables VARCHAR[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE 'SELECT audit.drop_table_log_trigger(tablename::varchar, schemaname::varchar) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING schema_name, except_tables;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* AUDIT ID COLUMN
*
* Add an extra column 'audit_id' to a table to trace 
* changes on rows over time.
***********************************************************/
-- add column 'audit_id' to a table
CREATE OR REPLACE FUNCTION audit.create_table_audit_id(
  table_name VARCHAR,
  schema_name VARCHAR DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE format('ALTER TABLE %I.%I ADD COLUMN audit_id SERIAL', schema_name, table_name);
END;
$$
LANGUAGE plpgsql;

-- perform create_table_audit_id on multiple tables in one schema
CREATE OR REPLACE FUNCTION audit.create_schema_audit_id(
  schema_name VARCHAR DEFAULT 'public',
  except_tables VARCHAR[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE 'SELECT audit.create_table_audit_id(tablename::varchar, schemaname::varchar) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING schema_name, except_tables;
END;
$$
LANGUAGE plpgsql;

-- drop column 'audit_id' from a table
CREATE OR REPLACE FUNCTION audit.drop_table_audit_id(
  table_name VARCHAR,
  schema_name VARCHAR DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE format('ALTER TABLE %I.%I DROP COLUMN audit_id', schema_name, table_name);
END;
$$
LANGUAGE plpgsql;

-- perform drop_table_audit_id on multiple tables in one schema
CREATE OR REPLACE FUNCTION audit.drop_schema_audit_id(
  schema_name VARCHAR DEFAULT 'public',
  except_tables VARCHAR[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE 'SELECT audit.drop_table_audit_id(tablename::varchar, schemaname::varchar) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING schema_name, except_tables;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* TRIGGER PROCEDURE log_change
*
* Procedure that is called when a log trigger event is fired.
* Rows of a logged table are logged in the audit_log table
* via row_to_json(NEW). Values of deleted rows are not logged.
***********************************************************/
CREATE OR REPLACE FUNCTION audit.log_change() RETURNS trigger AS
$$
DECLARE
  rec RECORD;
BEGIN
  IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
    EXECUTE 'INSERT INTO audit.audit_log VALUES (nextval(''audit.AUDIT_LOG_ID_SEQ''), $1, $2, $3, now()::timestamp, $4, $5)' 
               USING TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME, NEW.audit_id, row_to_json(NEW);
  ELSIF TG_OP = 'DELETE' THEN
    EXECUTE 'INSERT INTO audit.audit_log VALUES (nextval(''audit.AUDIT_LOG_ID_SEQ''), $1, $2, $3, now()::timestamp, $4, NULL)' 
               USING TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME, OLD.audit_id;
  ELSIF TG_OP = 'TRUNCATE' THEN
    FOR rec IN EXECUTE format('SELECT * FROM %I', TG_TABLE_NAME) LOOP
      EXECUTE 'INSERT INTO audit.audit_log VALUES (nextval(''audit.AUDIT_LOG_ID_SEQ''), $1, $2, $3, now()::timestamp, $4, NULL)' 
                 USING TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME, rec.audit_id;
	END LOOP;
  END IF;

  RETURN NULL;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* LOG TABLE STATE
*
* Log table content in audit_log table (as inserted values)
* which is useful when starting versioning a table.
***********************************************************/
-- log all rows of a table in the audit_log table as inserted values
CREATE OR REPLACE FUNCTION audit.log_table_state(
  table_name VARCHAR,
  schema_name VARCHAR DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
DECLARE
  rec RECORD;
BEGIN
  FOR rec IN EXECUTE format('SELECT * FROM %I.%I', schema_name, table_name) LOOP
    EXECUTE 'INSERT INTO audit.audit_log VALUES (nextval(''audit.AUDIT_LOG_ID_SEQ''), $1, $2, now(), $3, $4)' USING 'INSERT', table_name, rec.audit_id, row_to_json(rec);
  END LOOP;
END;
$$
LANGUAGE plpgsql;

-- perform log_table_state on multiple tables in one schema
CREATE OR REPLACE FUNCTION audit.log_schema_state(
  schema_name VARCHAR DEFAULT 'public',
  except_tables VARCHAR[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE 'SELECT audit.log_table_state(tablename::varchar, schemaname::varchar) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING schema_name, except_tables;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* PRODUCE TABLE STATE
*
* See what the table looked like at a given date.
* The table state will be produced in a separate schema.
* The use can choose if the target table will appear as a
* VIEW or TABLE.
***********************************************************/
CREATE OR REPLACE FUNCTION audit.produce_table_state(
  queried_date TIMESTAMP,
  original_table_name VARCHAR,
  original_schema_name VARCHAR,
  target_schema_name VARCHAR,
  target_table_type VARCHAR DEFAULT 'VIEW'
  ) RETURNS SETOF VOID AS
$$
DECLARE
  is_set_schema INTEGER := 0;
  is_set_table INTEGER := 0;
  tab_type VARCHAR(20);
  logged INTEGER := 0;
BEGIN
  -- test if target schema already exist
  EXECUTE 'SELECT 1 FROM information_schema.schemata WHERE schema_name = $1' INTO is_set_schema USING target_schema_name;

  IF is_set_schema IS NOT NULL THEN
    RAISE NOTICE 'Schema ''%'' does already exist.', target_schema_name;
  ELSE
    EXECUTE format('CREATE SCHEMA %I', target_schema_name);
  END IF;

  -- change target_table_type
  IF target_table_type = 'TABLE' THEN
    tab_type := 'BASE TABLE';
  ELSE
    tab_type := target_table_type;
  END IF;

  -- test if table or view already exist in target schema
  EXECUTE 'SELECT 1 FROM information_schema.tables WHERE table_name = $1 AND table_schema = $2 AND table_type = $3' 
             INTO is_set_table USING original_table_name, target_schema_name, tab_type;

  IF is_set_table IS NOT NULL THEN
    RAISE NOTICE '% ''%'' in schema ''%'' does already exist. Either delete the % or choose another name for a target schema.',
                    upper(target_table_type), original_table_name, target_schema_name, upper(target_table_type);
  ELSE
    -- check if logging entries exist in the audit_log table
    EXECUTE 'SELECT 1 FROM audit.audit_log WHERE table_name = $1 AND schema_name = $2 LIMIT 1'
               INTO logged USING original_table_name, original_schema_name;

    IF logged IS NOT NULL THEN
      -- let's go back in time - produce a table state at a given date
      IF upper(target_table_type) = 'VIEW' OR upper(target_table_type) = 'TABLE' THEN
        EXECUTE format('CREATE ' || target_table_type || ' %I.%I AS 
                   SELECT * FROM json_populate_recordset(null::%I.%I,(
                     SELECT json_agg(table_content) FROM audit.audit_log 
                       WHERE id IN 
                         (SELECT audit.get_log_entries(%L, db_date, %L, %L)
                            FROM audit.audit_log 
                              WHERE db_date <= %L 
                              AND table_name = %L
                              GROUP BY db_date ORDER BY db_date DESC
                          )
                     ))',
                target_schema_name, original_table_name, original_schema_name, original_table_name, 
				queried_date, original_table_name, original_schema_name,
				queried_date, original_table_name);
      ELSE
        RAISE NOTICE 'Table type ''%'' not supported. Use ''VIEW'' or ''TABLE''.', target_table_type;
      END IF;
    ELSE
      -- no entries found in log table - recent state of table will be transferred to the requested state
      RAISE NOTICE 'Did not found entries in log table for table ''%''.', original_table_name;
      IF upper(target_table_type) = 'VIEW' OR upper(target_table_type) = 'TABLE' THEN
        EXECUTE format('CREATE ' || target_table_type || ' %I.%I AS SELECT * FROM %I.%I', target_schema_name, original_table_name, original_schema_name, original_table_name);
      ELSE
        RAISE NOTICE 'Table type ''%'' not supported. Use ''VIEW'' or ''TABLE''.', target_table_type;
      END IF;
    END IF;
  END IF;
END;
$$
LANGUAGE plpgsql;

-- method to collect all entries that have been existed at a given date
CREATE OR REPLACE FUNCTION audit.get_log_entries(
  queried_date TIMESTAMP,
  search_date TIMESTAMP,
  original_table_name VARCHAR,
  original_schema_name VARCHAR
  ) RETURNS SETOF INTEGER AS
$$
BEGIN
  RETURN QUERY EXECUTE 'SELECT id FROM audit.audit_log
                          WHERE table_name = $1 AND schema_name = $2
                          AND db_date = $3
                          AND (table_operation = ''INSERT'' OR table_operation = ''UPDATE'')
                          AND audit_id NOT IN (
                            SELECT audit_id FROM audit.audit_log
                              WHERE table_name = $1 AND schema_name = $2
                              AND (db_date > $3 AND db_date <= $4)
                          )
                          ORDER BY id ASC'
                          USING original_table_name, original_schema_name, 
                                search_date, queried_date;
END;
$$
LANGUAGE plpgsql;

-- perform produce_table_state on multiple tables in one schema
CREATE OR REPLACE FUNCTION audit.produce_schema_state(
  queried_date TIMESTAMP, 
  original_schema_name VARCHAR,
  target_schema_name VARCHAR, 
  target_table_type VARCHAR DEFAULT 'VIEW',
  except_tables VARCHAR[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE 'SELECT audit.produce_table_state($1::timestamp, tablename::varchar, schemaname::varchar, $2, $3) FROM pg_tables 
             WHERE schemaname = $4 AND tablename <> ALL ($5)'
             USING queried_date, target_schema_name, target_table_type, original_schema_name, except_tables;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* PKEY TABLE STATE
*
* If a table state is produced as a table it will not have
* a primary key. The primary key might be reconstruced by
* querying the recent primary key of the table. If no primary
* can be redefined the audit_id column will be used.
***********************************************************/
-- define a primary key for a produced table
CREATE OR REPLACE FUNCTION audit.pkey_table_state( 
  table_name VARCHAR,
  target_schema_name VARCHAR,
  original_schema_name VARCHAR DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
DECLARE
  pkey_columns VARCHAR(256) := '';
BEGIN
  -- rebuild primary key columns to index produced tables
  EXECUTE 'SELECT array_to_string(array_agg(pga.attname::varchar),'','') FROM pg_index pgi, pg_class pgc, pg_attribute pga 
             WHERE pgc.oid = $1::regclass 
             AND pgi.indrelid = pgc.oid 
             AND pga.attrelid = pgc.oid 
             AND pga.attnum = ANY(pgi.indkey) AND pgi.indisprimary' 
               INTO pkey_columns USING '"' || original_schema_name || '".' || table_name;

  IF length(pkey_columns) = 0 THEN
    RAISE NOTICE 'Table ''%'' has no primary key defined. Column ''audit_id'' will be used as primary key.', table_name;
    pkey_columns := 'audit_id';
  END IF;

  EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I_PK PRIMARY KEY (' || pkey_columns || ')', target_schema_name, table_name, table_name);
END;
$$
LANGUAGE plpgsql;

-- perform pkey_table_state on multiple tables in one schema
CREATE OR REPLACE FUNCTION audit.pkey_schema_state(
  target_schema_name VARCHAR, 
  original_schema_name VARCHAR DEFAULT 'public',
  except_tables VARCHAR[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE 'SELECT audit.pkey_table_state(tablename::varchar, schemaname::varchar, $3) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING target_schema_name, except_tables, original_schema_name;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* FKEY TABLE STATE
*
* If multiple table states are produced as tables they are not
* referenced which each other. Foreign key relations might be
* reconstruced by querying the recent foreign keys of the table.
***********************************************************/
-- define foreign keys between produced tables
CREATE OR REPLACE FUNCTION audit.fkey_table_state( 
  table_name VARCHAR,
  target_schema_name VARCHAR,
  original_schema_name VARCHAR DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
DECLARE
  fkey RECORD;
BEGIN
  -- rebuild foreign key contraints
  FOR fkey IN EXECUTE 'SELECT tc.constraint_name AS fkey_name, kcu.column_name AS fkey_column, ccu.table_name AS ref_table, ccu.column_name AS ref_column
                        FROM information_schema.table_constraints AS tc 
                        JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
                        JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
                          WHERE constraint_type = ''FOREIGN KEY'' AND tc.table_schema = $1 AND tc.table_name=$2' 
                          USING original_schema_name, table_name LOOP
    BEGIN
      -- test query
      EXECUTE format('SELECT 1 FROM %I.%I a, %I.%I b WHERE a.%I = b.%I LIMIT 1',
                        target_schema_name, table_name, target_schema_name, fkey.ref_table, fkey.fkey_column, fkey.ref_column);

      -- recreate foreign key of original table
      EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I FOREIGN KEY (%I) REFERENCES %I.%I ON UPDATE CASCADE ON DELETE RESTRICT',
                        target_schema_name, table_name, fkey.fkey_name, fkey.fkey_column, target_schema_name, fkey.ref_table, fkey.ref_column);

      EXCEPTION
        WHEN OTHERS THEN
          RAISE NOTICE 'Could not recreate foreign key constraint ''%'' on table ''%'': %', fkey.fkey_name, table_name, SQLERRM;
          NULL;
    END;
  END LOOP;
END;
$$
LANGUAGE plpgsql;

-- perform fkey_table_state on multiple tables in one schema
CREATE OR REPLACE FUNCTION audit.fkey_schema_state(
  target_schema_name VARCHAR, 
  original_schema_name VARCHAR DEFAULT 'public',
  except_tables VARCHAR[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE 'SELECT audit.fkey_table_state(tablename::varchar, schemaname::varchar, $3) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING target_schema_name, except_tables, original_schema_name;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* INDEX TABLE STATE
*
* If a produced table shall be used for queries indexes will 
* be necessary in order to guarantee high performance. Indexes
* might be reconstruced by querying recent indexes of the table.
***********************************************************/
-- define index(es) on columns of a produced table
CREATE OR REPLACE FUNCTION audit.index_table_state( 
  table_name VARCHAR,
  target_schema_name VARCHAR,
  original_schema_name VARCHAR DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
DECLARE
  idx RECORD;
  dim INTEGER;
BEGIN  
  -- rebuild user defined indexes
  FOR idx IN EXECUTE 'SELECT pgc.relname AS idx_name, pgam.amname AS idx_type, array_to_string(
                      ARRAY(SELECT pg_get_indexdef(pgi.indexrelid, k + 1, true) FROM generate_subscripts(pgi.indkey, 1) as k ORDER BY k)
                      , '','') as idx_columns
                      FROM pg_index pgi
                      JOIN pg_class pgc ON pgc.oid = pgi.indexrelid
                      JOIN pg_am pgam ON pgam.oid = pgc.relam
                        AND pgi.indrelid = $1::regclass
                        AND pgi.indisprimary = ''f''' 
                        USING '"' || original_schema_name || '".' || table_name LOOP
    BEGIN
      -- reset dim variable
      dim := 0;	  
	  
	  -- test query
      EXECUTE format('SELECT ' || idx.idx_columns || ' FROM %I.%I LIMIT 1', target_schema_name, table_name);

	  -- if a gist index has been found, it can be a spatial index of the PostGIS extension
      IF idx.idx_type = 'gist' THEN
        BEGIN		  
		  -- query view 'geometry_columns' view to get the dimension of possible spatial column
          EXECUTE 'SELECT coord_dimension FROM geometry_columns 
                     WHERE f_table_schema = $1 AND f_table_name = $2 AND f_geometry_column = $3'
                       INTO dim USING original_schema_name, table_name, idx.idx_columns;

          EXCEPTION
            WHEN OTHERS THEN
              RAISE NOTICE 'An error occurred when querying the PostGIS table ''geometry_columns'': %', SQLERRM;
              NULL;
        END;
      END IF;

      -- recreate the index
      IF dim = 3 THEN
        EXECUTE format('CREATE INDEX %I ON %I.%I USING GIST(%I gist_geometry_ops_nd)', idx.idx_name, target_schema_name, table_name, idx.idx_columns);
      ELSE
        EXECUTE format('CREATE INDEX %I ON %I.%I USING ' || idx.idx_type || '(' || idx.idx_columns || ')', idx.idx_name, target_schema_name, table_name);
      END IF;

      EXCEPTION
        WHEN OTHERS THEN
          RAISE NOTICE 'Could not recreate index ''%'' on table ''%'': %', idx.idx_name, table_name, SQLERRM;
          NULL;
    END;
  END LOOP;
END;
$$
LANGUAGE plpgsql;

-- perform index_table_state on multiple tables in one schema
CREATE OR REPLACE FUNCTION audit.index_schema_state(
  target_schema_name VARCHAR, 
  original_schema_name VARCHAR DEFAULT 'public',
  except_tables VARCHAR[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE 'SELECT audit.index_table_state(tablename::varchar, schemaname::varchar, $3) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING target_schema_name, except_tables, original_schema_name;
END;
$$
LANGUAGE plpgsql;


/**********************************************************
* RECREATE SCHEMA STATE
*
* If a schema state shall be recreated as the actual database
* the recent tables are truncated and dropped first and the
* the former state is rebuild from the schema that contains
* the former state.
*
* NOTE: In order to rebuild primary keys, foreign keys and 
*       indexes corresponding functions must have been executed
*       on target schema.
***********************************************************/
CREATE OR REPLACE FUNCTION audit.recreate_schema_state(
  schema_name VARCHAR,
  target_schema_name VARCHAR DEFAULT 'public',
  except_tables VARCHAR[] DEFAULT '{}'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  -- drop foreign keys in target schema
  EXECUTE 'SELECT audit.drop_table_relations(tablename::varchar, schemaname::varchar) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING target_schema_name, except_tables;

  -- drop tables in target schema
  EXECUTE 'SELECT audit.drop_table(tablename::varchar, schemaname::varchar) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING target_schema_name, except_tables;

  -- copy tables of chosen schema into target schema
  EXECUTE 'SELECT audit.recreate_table_state(tablename::varchar, schemaname::varchar, $3) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING schema_name, except_tables, target_schema_name;

  -- create primary keys for tables in target schema
  EXECUTE 'SELECT audit.pkey_table_state(tablename::varchar, schemaname::varchar, $3) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING target_schema_name, except_tables, schema_name;

  -- create foreign keys for tables in target schema
  EXECUTE 'SELECT audit.fkey_table_state(tablename::varchar, schemaname::varchar, $3) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING target_schema_name, except_tables, schema_name;

  -- index tables in target schema
  EXECUTE 'SELECT audit.index_table_state(tablename::varchar, schemaname::varchar, $3) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING target_schema_name, except_tables, schema_name;

  -- activate loggin triggers in target schema 
  EXECUTE 'SELECT audit.create_table_log_trigger(tablename::varchar, schemaname::varchar) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING target_schema_name, except_tables;

  -- fill audit_log table with entries from new tables in target schema
  EXECUTE 'SELECT audit.log_table_state(tablename::varchar, schemaname::varchar) FROM pg_tables 
             WHERE schemaname = $1 AND tablename <> ALL ($2)' 
             USING target_schema_name, except_tables;
END;
$$
LANGUAGE plpgsql;

-- drop foreign key contraints
CREATE OR REPLACE FUNCTION audit.drop_table_relations(
  table_name VARCHAR,
  target_schema_name VARCHAR DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
DECLARE
  fkey VARCHAR;
BEGIN
  FOR fkey IN EXECUTE 'SELECT constraint_name AS fkey_name FROM information_schema.table_constraints 
                         WHERE constraint_type = ''FOREIGN KEY'' AND table_schema = $1 AND table_name= $2'
                          USING target_schema_name, table_name LOOP
    EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT %I', target_schema_name, table_name, fkey);
  END LOOP;
END;
$$
LANGUAGE plpgsql;

-- truncate and drop table and all depending objects
CREATE OR REPLACE FUNCTION audit.drop_table(
  table_name VARCHAR,
  target_schema_name VARCHAR DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  -- trigger the log_truncate_trigger
  EXECUTE format('TRUNCATE TABLE %I.%I', target_schema_name, table_name);

  -- dropping the table
  EXECUTE format('DROP TABLE %I.%I CASCADE', target_schema_name, table_name);
END;
$$
LANGUAGE plpgsql;

-- recreate table state into the schema used as the recent database state
CREATE OR REPLACE FUNCTION audit.recreate_table_state(
  table_name VARCHAR,
  schema_name VARCHAR,
  target_schema_name VARCHAR DEFAULT 'public'
  ) RETURNS SETOF VOID AS
$$
BEGIN
  EXECUTE format('CREATE TABLE %I.%I AS SELECT * FROM %I.%I', target_schema_name, table_name, schema_name, table_name);
END;
$$
LANGUAGE plpgsql;
