--Expected parameters for this file:
-- pagesfunctionname
-- totalpages
-- qtablename
-- cursortablename
-- cursorprimarykeyconstraintname
-- gctriggerfunctionname
-- gctriggername

--This function holds number of pages this Q has(Different Q can have different pages)
CREATE OR REPLACE FUNCTION $[pagesfunctionname:name]() RETURNS integer IMMUTABLE LANGUAGE SQL AS $$ SELECT $[totalpages] $$;

--This is the Queue table holds data injected by users
CREATE TABLE $[qtablename:name]
(
	"Timestamp" timestamp without time zone NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC'),
    "Serial" bigserial,
    "Page" integer NOT NULL,
	"CursorId" bigint,
    "Payload" jsonb,
    PRIMARY KEY ("Serial","Page")
) PARTITION BY RANGE ("Page") ;

--Following code creates partitions for Q table
DO $$
DECLARE 
"Pages" integer :=$[totalpages];
"DSql" TEXT;
BEGIN
	WHILE "Pages" > 0 LOOP
	SELECT 'CREATE TABLE '|| quote_ident( $[qtablename] || '-' ||"Pages"-1) ||' PARTITION OF $[qtablename:name] FOR VALUES FROM ('||"Pages"-1 ||') TO ('||"Pages"||');'
	INTO "DSql";
	EXECUTE "DSql";
	-- RAISE NOTICE '%',"DSql";
	"Pages":="Pages"-1;
	END LOOP;
END$$;


--This is cursor table holds pointer to Q of what is being read currently.
CREATE TABLE $[cursortablename:name]
(
    "Serial" bigint NOT NULL,
	"Page" integer NOT NULL,
    "CursorId" bigserial,
    "Ack" integer,
	"Fetched" timestamp without time zone NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC'),
    "Token" integer NOT NULL DEFAULT (floor(random()*(10000000-0+1))+0),
    CONSTRAINT $[cursorprimarykeyconstraintname:name] PRIMARY KEY ("CursorId")
)WITH (fillfactor=50);

CREATE INDEX ON $[cursortablename:name] USING btree ("CursorId" ASC NULLS LAST) INCLUDE("Serial", "Page", "Ack", "Fetched") WITH (fillfactor=50);

--This function is a part of trigger used to invoke garbage collection(Remove items from Q which are processed)
CREATE OR REPLACE FUNCTION $[gctriggerfunctionname:name]()
  RETURNS TRIGGER 
  LANGUAGE PLPGSQL  
  AS
$$
DECLARE
"DSql" TEXT;
"IsMinimum" BOOLEAN := FALSE;
BEGIN
	IF NEW."Page" <> OLD."Page" THEN
		SELECT MIN("Page")= OLD."Page" INTO "IsMinimum" FROM $[cursortablename:name];
		IF "IsMinimum" THEN
			SELECT 'TRUNCATE '|| quote_ident(TG_TABLE_SCHEMA)||'.' ||quote_ident($[qtablename] || '-' || OLD."Page")
			INTO "DSql"
			FROM $[cursortablename:name];
			EXECUTE "DSql";
		END IF;
	END IF;
	RETURN NEW;
END;
$$ ;

--Trigger for Garbage collection function 
CREATE TRIGGER $[gctriggername:name] BEFORE UPDATE ON $[cursortablename:name] FOR EACH ROW EXECUTE PROCEDURE $[gctriggerfunctionname:name]();







-- --Query Tells you page size and rotating tables sizes
-- SELECT REPLACE(relname, 'Q-7a28fd25d95c0969bff16b963af1c832', 'Page-' ) AS "relation",
-- pg_size_pretty (pg_table_size (C .oid)) AS "TableSize",
-- pg_size_pretty (pg_indexes_size (C .oid)) AS "IndexSize",
-- reltuples AS approximate_row_count
-- FROM pg_class C
-- LEFT JOIN pg_namespace N ON (N.oid = C .relnamespace)
-- WHERE relname like 'Q-7a28fd25d95c0969bff16b963af1c832%'
-- AND C .relkind ='r' AND pg_total_relation_size (C .oid) > 16384 --16KB is  the default space a table takes
-- ORDER BY pg_total_relation_size (C .oid) DESC 
