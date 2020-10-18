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
		SELECT MIN("Page")= OLD."Page" INTO "IsMinimum" FROM "Q"."Cursor";
		IF "IsMinimum" THEN
			SELECT 'TRUNCATE '|| quote_ident(TG_TABLE_SCHEMA)||'.' ||quote_ident($[qtablename]|| OLD."Page")
			INTO "DSql"
			FROM "Q"."Cursor";
			EXECUTE "DSql";
		END IF;
	END IF;
	RETURN NEW;
END;
$$ ;

--Trigger for Garbage collection function 
CREATE TRIGGER $[gctriggername:name] BEFORE UPDATE ON $[cursortablename:name] FOR EACH ROW EXECUTE PROCEDURE $[gctriggerfunctionname:name]();











-- --Insert Query
-- INSERT INTO $[qTableName:name] ("Payload","Page")
--     SELECT $1,(SELECT CASE WHEN "WriterShouldBe"="GC" THEN -1 ELSE "WriterShouldBe" END AS "Writer"
--     FROM (
--     SELECT COALESCE(MOD(MAX("Page")+1,$[totalpagesFunctionName:name]()),1) AS "WriterShouldBe",
--     COALESCE(MIN("Page"),$[totalpagesFunctionName:name]()-1) AS "GC"
--     FROM $[cursorTableName:name]
--     )AS "T")


-- --NEXT
-- —What  happens when the  serial overflows?
-- —What happens when the DB is new?
-- —What  happens when both  timeout and ack evaluate  to true?
-- —What happens  when serial overflows
-- INSERT INTO "Q"."Cursor" ("Serial","Page","CursorId","Ack","Token","Fetched")
-- SELECT "QID"[1] AS "Serial","QID"[2] AS "Page","CursorId","Ack","Token","Fetched"
-- FROM(
-- SELECT
-- 	CASE 
-- 		WHEN "Ack"=1 AND "Serial"=9223372036854775807 THEN (SELECT ARRAY["Serial","Page"] FROM "Q"."Q" WHERE "Serial" > 0 ORDER BY "Serial" ASC LIMIT 1)
-- 		WHEN "Ack"=0 AND ((NOW() AT TIME ZONE 'UTC')-"Cursor"."Fetched") > (10 * INTERVAL '1 Second') THEN ARRAY["Serial","Page"]
-- 		WHEN "Ack"=0 AND ((NOW() AT TIME ZONE 'UTC')-"Cursor"."Fetched") < (10 * INTERVAL '1 Second') THEN ARRAY[-1,-1]
-- 		ELSE (SELECT ARRAY["Serial","Page"] FROM "Q"."Q" WHERE "Serial" > "Cursor"."Serial" ORDER BY "Serial" ASC LIMIT 1)
-- 	END AS "QID",
-- 	 1 AS "CursorId",0 AS "Ack", (floor(random()*(10000000-0+1))+0) AS "Token", NOW() AT TIME ZONE 'UTC' AS "Fetched"
-- FROM (
-- 	SELECT "Serial","Page","Ack", "Fetched","CursorId"
-- 	FROM "Q"."Cursor"
-- 	WHERE "CursorId"=1
-- 	UNION ALL
-- 	SELECT 0,-1,1,NOW() AT TIME ZONE 'UTC',1
-- 	ORDER BY "Serial" DESC
-- 	LIMIT 1
-- ) AS "Cursor"
-- ) AS "Temp"
-- WHERE "QID" IS NOT NULL
-- ON CONFLICT ON CONSTRAINT "Cursor_PKey"
-- DO UPDATE 
-- SET 
-- "Serial"=Excluded."Serial",
-- "Page"=Excluded."Page",
-- "Ack"=Excluded."Ack",
-- "Fetched"=Excluded."Fetched",
-- "Token"= Excluded."Token"
-- WHERE Excluded."Serial" != -1
-- RETURNING *

-- —ACK
-- UPDATE "Q"."Cursor"
-- SET "Ack"=1 
-- WHERE "CursorId"=1 
-- AND "Token"=8271913
-- RETURNING *