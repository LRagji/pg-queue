--Expected parameters for this file:
-- pagesfunctionname
-- totalpages
-- qtablename
-- cursortablename
-- cursorprimarykeyconstraintname
-- gctriggerfunctionname
-- gctriggername
-- dequeuefunctionname
-- acknowledgepayloadfunctionname
-- trydequeuefunctionname
-- tryacknowledgepayloadfunctionname

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

--Deque function: Helps to retieve item from the Que should always be called from Serializable transaction to avoid concurrency issues.
CREATE OR REPLACE FUNCTION $[dequeuefunctionname:name]("FetchCursorId" integer,"TimeoutInSeconds" integer) RETURNS JSONB
LANGUAGE PLPGSQL
AS $$
DECLARE
"AcquiredPayload" $[cursortablename:name]%ROWTYPE;
"Result" JSONB;
BEGIN
CREATE TEMP TABLE "TruncateTriggerIsolation" ON COMMIT DROP AS
SELECT "QID"[1] AS "Serial","QID"[2] AS "Page","CursorId","Ack","Token","Fetched"
FROM(
SELECT
	CASE 
		WHEN "Ack"=1 AND "Serial"=9223372036854775807 THEN (SELECT ARRAY["Serial","Page"] FROM $[qtablename:name] WHERE "Serial" > 0 ORDER BY "Serial" ASC LIMIT 1)
		WHEN "Ack"=0 AND ((NOW() AT TIME ZONE 'UTC')-"Cursor"."Fetched") > ("TimeoutInSeconds" * INTERVAL '1 Second') THEN ARRAY["Serial","Page"]
		WHEN "Ack"=0 AND ((NOW() AT TIME ZONE 'UTC')-"Cursor"."Fetched") < ("TimeoutInSeconds" * INTERVAL '1 Second') THEN ARRAY[-1,-1]
		ELSE (SELECT ARRAY["Serial","Page"] FROM $[qtablename:name] WHERE "Serial" > "Cursor"."Serial" ORDER BY "Serial" ASC LIMIT 1)
	END AS "QID",
	"Cursor"."CursorId",0 AS "Ack", (floor(random()*(10000000-0+1))+0) AS "Token", NOW() AT TIME ZONE 'UTC' AS "Fetched"
FROM (
	SELECT "Serial","Page","Ack", "Fetched","CursorId"
	FROM $[cursortablename:name]
	WHERE "CursorId"="FetchCursorId"
	UNION ALL
	SELECT 0,-1,1,NOW() AT TIME ZONE 'UTC',"FetchCursorId"
	ORDER BY "Serial" DESC
	LIMIT 1
) AS "Cursor"
) AS "Temp"
WHERE "QID" IS NOT NULL;

INSERT INTO $[cursortablename:name] ("Serial","Page","CursorId","Ack","Token","Fetched")
SELECT "Serial","Page","CursorId","Ack","Token","Fetched" FROM "TruncateTriggerIsolation"
ON CONFLICT ON CONSTRAINT $[cursorprimarykeyconstraintname:name]
DO UPDATE 
SET 
"Serial"=Excluded."Serial",
"Page"=Excluded."Page",
"Ack"=Excluded."Ack",
"Fetched"=Excluded."Fetched",
"Token"= Excluded."Token"
WHERE Excluded."Serial" != -1
RETURNING * INTO "AcquiredPayload";

SELECT jsonb_agg(jsonpacket)
			FROM(
				SELECT "Payload","AcquiredPayload"."Serial","AcquiredPayload"."Page","AcquiredPayload"."CursorId","AcquiredPayload"."Token" 
				FROM $[qtablename:name]
				WHERE "Page"="AcquiredPayload"."Page" AND "Serial"="AcquiredPayload"."Serial"
			)as jsonpacket INTO "Result";

RETURN "Result";

END
$$;

--Function to acknowledge a message, should always be called from serialization transaction to avoid concurrency issues.
CREATE OR REPLACE FUNCTION $[acknowledgepayloadfunctionname:name]("MessagesToAckSerialized" JSONB) 
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
"Result" JSONB;
BEGIN
	WITH "MessagesToAck" AS 
	(
	SELECT "CursorId","Token"
	FROM jsonb_to_recordset("MessagesToAckSerialized")
	AS ("CursorId" Bigint, "Token" integer)
	),
	"AckedPayloads" AS( 
	UPDATE $[cursortablename:name]
	SET "Ack"=1
	FROM "MessagesToAck"
	WHERE $[cursortablename:name]."CursorId"="MessagesToAck"."CursorId" 
	AND $[cursortablename:name]."Token"="MessagesToAck"."Token"
	RETURNING $[cursortablename:name]."Serial", $[cursortablename:name]."Page", $[cursortablename:name]."CursorId", $[cursortablename:name]."Token"
	)
	SELECT jsonb_agg(jsonpacket)
				FROM(
					SELECT * 
					FROM "MessagesToAck"
					WHERE "MessagesToAck"."CursorId" NOT IN (SELECT "CursorId" FROM "AckedPayloads")
	)as jsonpacket INTO "Result";

	RETURN "Result";
END
$$;

--Try to dequeue this is a proc as it uses serialized transactions 
CREATE OR REPLACE PROCEDURE $[trydequeuefunctionname:name](
	"FetchCursorId" integer,
	"TimeoutInSeconds" integer,
	"Attempts" integer  DEFAULT 10,
	INOUT "Results" JSONB DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
"Continue" boolean :=True;
BEGIN
	WHILE "Attempts" > 0 AND "Continue" LOOP
	COMMIT;--<-- This is just so that it starts a new transaction where i set isolation level
	SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE;
		BEGIN
			SELECT $[dequeuefunctionname:name]("FetchCursorId","TimeoutInSeconds") INTO "Results";
			"Continue":=False;
		EXCEPTION WHEN SQLSTATE '40001' THEN --<-- Its ok for serialization error to occur, just retry
		"Continue":=True;
		RAISE NOTICE 'DQ Retrying %',"Attempts";
		END;
		"Attempts":="Attempts"-1;
	END LOOP;
	COMMIT;--<-- Final commit and reset the transaction level
END
$$;


--Try to ack payloads this is a proc as it uses serialized transactions 
CREATE OR REPLACE PROCEDURE $[tryacknowledgepayloadfunctionname:name](
	INOUT "MessagesToAckSerialized" JSONB DEFAULT NULL,
	"Attempts" integer  DEFAULT 10
)
LANGUAGE plpgsql
AS $$
DECLARE
"Continue" boolean :=True;
BEGIN
	WHILE "Attempts" > 0 AND "Continue" LOOP
	COMMIT;--<-- This is just so that it starts a new transaction where i set isolation level
	SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE;
		BEGIN
			SELECT $[acknowledgepayloadfunctionname:name]("MessagesToAckSerialized") INTO "MessagesToAckSerialized";
			"Continue":=False;
		EXCEPTION WHEN SQLSTATE '40001' THEN 
		"Continue":=True;
		RAISE NOTICE 'Ack Retrying %',"Attempts";
		END;
		"Attempts":="Attempts"-1;
	END LOOP;
	COMMIT;--<-- Final commit and reset the transaction level
END
$$;


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
