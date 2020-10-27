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
-- subscriberstablename
-- subscriberregistrationfunctionname
-- qname
-- deletesubscriberfunctionname
-- processprocname

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

--This is subscriber table which holds information of what cursors are held by what subscribers
CREATE TABLE $[subscriberstablename:name]
(
    "Name" character(32) NOT NULL,
    "Cursors" bigint[] NOT NULL,
   	PRIMARY KEY ("Name")
)WITH (FILLFACTOR = 50);

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

-- This function registers a subscriber or if the subscriber exists it return MessagesPerBatch for the same.
CREATE OR REPLACE FUNCTION $[subscriberregistrationfunctionname:name] ("SubscriberName" character(32),"MessagesPerBatch" integer) RETURNS INTEGER
LANGUAGE PLPGSQL
AS $$
BEGIN
	IF NOT EXISTS(SELECT 1 FROM $[subscriberstablename:name] WHERE "Name"="SubscriberName") THEN 
		INSERT INTO $[subscriberstablename:name] ("Name","Cursors")
		SELECT "SubscriberName", ARRAY_AGG(nextval(pg_get_serial_sequence('$[cursortablename:name]', 'CursorId')))
		FROM generate_series(1,"MessagesPerBatch");
	END IF;
	RETURN (SELECT ARRAY_LENGTH("Cursors",1) FROM $[subscriberstablename:name] WHERE "Name"="SubscriberName");
END
$$;

--Deque function: Helps to retieve items from the Que should always be called from transaction to avoid concurrency issues.
CREATE OR REPLACE FUNCTION $[dequeuefunctionname:name]("SubscriberName" character(32),"TimeoutInSeconds" integer) 
RETURNS TABLE ("S" BIGINT,  "P" INTEGER,"T" INTEGER, "C" BIGINT,"M" JSONB )
LANGUAGE PLPGSQL
AS $$
DECLARE
"TruncateTriggerIsolation" JSONB;
"ConfirmedCusrosrs" JSONB;
BEGIN

IF pg_try_advisory_xact_lock(hashtext($[qname]))=FALSE THEN RETURN; END IF;--Guardian of concurrency problem NEEDS A Transaction

WITH "ExpandedCurors" AS(
	SELECT UNNEST("Cursors") AS "CursorId" 
	FROM $[subscriberstablename:name]
	WHERE "Name"="SubscriberName"
)
,"CursorState" AS (
SELECT "CursorId","QID"[1] AS "Serial","QID"[2] AS "Page","QID"[3] AS "Status",0 AS "Ack",(floor(random()*(10000000-0+1))+0)AS "Token",clock_timestamp() AT TIME ZONE 'UTC' AS "Fetched"
,COALESCE((SELECT MAX("Serial") --This has to consider even the allocated/active serials so a seperate query
FROM $[cursortablename:name]
WHERE "CursorId" = ANY (SELECT "CursorId" FROM "ExpandedCurors")),0) as "MaxSerial"
,ROW_NUMBER() OVER(PARTITION BY "QID"[3]) AS "Id"
FROM (
	SELECT "CursorId",
	COALESCE(
	(SELECT 
	CASE --["Serial","Page","PayloadAssignStatus"] 0=Assign New Payload,1= Assign Same,2= Donot Assign
		WHEN "Ack"=1 AND "Serial"=9223372036854775807 THEN ARRAY[0,-1,0] --ROLLOVER OF Q Table SERIAL CASE, Assign new payload
		WHEN "Ack"=0 AND ((clock_timestamp() AT TIME ZONE 'UTC')-"Fetched") > ("TimeoutInSeconds" * INTERVAL '1 Second') THEN ARRAY["Serial","Page",1] --Timeout Case, Assign same payload again
		WHEN "Ack"=0 AND ((clock_timestamp() AT TIME ZONE 'UTC')-"Fetched") < ("TimeoutInSeconds" * INTERVAL '1 Second') THEN ARRAY["Serial","Page",2] --Active with cursor, Do not assign
		ELSE ARRAY["Serial","Page",0]--"Ack"=1 AND "Serial" < Rollover, Assign new payload.
	END AS "QID"
	FROM $[cursortablename:name] 
	WHERE "CursorId"="ExpandedCurors"."CursorId"),ARRAY[0,-1,0]) AS "QID"
	FROM "ExpandedCurors"
)AS  "CS" --"CursorsState"
WHERE "QID"[3] != 2--These status ones are active and we dont want them to be included in further calculation they were only used for not generating ground zero state
)
,"NextCursors" AS(
	SELECT "Q"."Serial","Q"."Page",
	ROW_NUMBER() OVER() AS "Id"
	FROM $[qtablename:name] AS "Q"
	WHERE "Q"."Serial" > (SELECT "MaxSerial" FROM "CursorState" LIMIT 1)
	LIMIT CASE WHEN TRUE THEN COALESCE((SELECT COUNT(1) FROM "CursorState" WHERE "Status"=0),0) END
)
SELECT jsonb_agg(jsonpacket) INTO "TruncateTriggerIsolation"
	FROM (
			SELECT "CursorId","Ack","Token","Fetched"
			,CASE WHEN "Status"=1 THEN "CursorState"."Serial" ELSE "NextCursors"."Serial" END AS "Serial"
			,CASE WHEN "Status"=1 THEN "CursorState"."Page" ELSE "NextCursors"."Page" END AS "Page","Status"
			FROM "CursorState" LEFT JOIN "NextCursors" ON "CursorState"."Id" ="NextCursors"."Id"--LEFT JOIN When all rows have timed out
		) as jsonpacket
	WHERE "Serial" IS NOT NULL AND "Page" IS NOT NULL;-- Join can introduce nulls when no more messages are left in Q

--We need to isolate below insert statement from above CTE as Update statement can trigger GC trigger which will try to truncate Q page and will fail cause of active reference in the query
--To do that we tried to use TEMP tables problem with them is they are not friendly with SERIALIZED Transactions on Procedures so we have to serial to local variable using JSON.

WITH "CC" AS (
	INSERT INTO $[cursortablename:name] ("Serial","Page","CursorId","Ack","Token","Fetched")
	SELECT "Serial","Page","CursorId","Ack","Token","Fetched" 
	FROM jsonb_to_recordset("TruncateTriggerIsolation")
	AS ("Serial" BIGINT,"Page" INTEGER,"CursorId" BIGINT,"Ack" INTEGER,"Token" INTEGER,"Fetched" TIMESTAMP WITHOUT TIME ZONE)
	ORDER BY "CursorId"
	ON CONFLICT ON CONSTRAINT $[cursorprimarykeyconstraintname:name]
	DO UPDATE 
	SET 
	"Serial"=Excluded."Serial",
	"Page"=Excluded."Page",
	"Ack"=Excluded."Ack",
	"Fetched"=Excluded."Fetched",
	"Token"= Excluded."Token"
	RETURNING *
)
SELECT jsonb_agg("CC".*) INTO "ConfirmedCusrosrs"
FROM "CC";

--Need to keep this also isolated from insert and update statement
RETURN QUERY SELECT "Serial", "Page", "Token", "CC"."CursorId", 
(SELECT "Payload" FROM $[qtablename:name] WHERE "Page"="CC"."Page" AND "Serial"="CC"."Serial")--This is required this way only cause serial can be repeating across pages(rollover condition).
FROM jsonb_to_recordset("ConfirmedCusrosrs")
AS "CC" ("Serial" BIGINT,"Page" INTEGER,"CursorId" BIGINT,"Ack" INTEGER,"Token" INTEGER,"Fetched" TIMESTAMP WITHOUT TIME ZONE);

END
$$;

--Function to acknowledge a message, should always be called from transaction to avoid concurrency issues.
CREATE OR REPLACE FUNCTION $[acknowledgepayloadfunctionname:name]("MessagesToAckSerialized" JSONB) 
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
"Result" JSONB;
BEGIN

	IF pg_try_advisory_xact_lock(hashtext($[qname]))=FALSE THEN RETURN "MessagesToAckSerialized"; END IF;--Guardian of concurrency problem NEEDS A Transaction

	WITH "MessagesToAck" AS 
	(
	SELECT "C" AS "CursorId","T" AS "Token"
	FROM jsonb_to_recordset("MessagesToAckSerialized")
	AS ("C" Bigint, "T" integer)
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
					SELECT "CursorId" AS "C" ,"Token" AS "T"
					FROM "MessagesToAck"
					WHERE "MessagesToAck"."CursorId" NOT IN (SELECT "CursorId" FROM "AckedPayloads")
	)as jsonpacket INTO "Result";

	RETURN "Result";
END
$$;

--Function to delete a subscriber, should always be called from transaction to avoid concurrency issues.
CREATE OR REPLACE FUNCTION $[deletesubscriberfunctionname:name]("SubscriberName" character(32)) 
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
BEGIN
IF pg_try_advisory_xact_lock(hashtext($[qname]))=FALSE THEN RETURN FALSE ; END IF;--Guardian of concurrency problem NEEDS A Transaction

DELETE FROM $[cursortablename:name] WHERE "CursorId" = ANY (Select UNNEST("Cursors") FROM $[subscriberstablename:name] WHERE "Name"="SubscriberName");
DELETE FROM $[subscriberstablename:name] WHERE "Name"="SubscriberName";
RETURN TRUE;
END
$$;




-------------------------------------------------FOLLOWING FUNCTIONS CAN BE USED DIRECTLY ON PG (NOT USED BY PACKAGE)---------------------------------------------
--Following proc uses trnsaction control to acquire and release locks be care full to call this function on a seperate connection if required.
CREATE PROCEDURE $[processprocname:name](
	"SubscriberName" character(32),
	"TimeoutInSeconds" integer DEFAULT 3600,
	"Attempts" integer  DEFAULT 10,
	INOUT "PendingTokensToAck" JSONB DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
"Payload" JSONB;
"PayloadTokensToAck" JSONB;
"Message" RECORD;
"ExitOnException" BOOLEAN :=FALSE;
BEGIN
	COMMIT;-- <-- Required to start a new transaction for lock
	BEGIN
		SELECT jsonb_agg(jsonpacket.*) INTO "Payload"
		FROM "DQ-7a28fd25d95c0969bff16b963af1c832"('Secondary',10) 
		AS jsonpacket;-- <-- DQ Payload from Q 
	EXCEPTION WHEN OTHERS THEN
	"ExitOnException" := TRUE;
	END;
	COMMIT;-- <-- Required to release lock
	IF "ExitOnException" THEN RETURN; END IF; -- <-- Exit if there is any kind of exception.
	
	FOR "Message" IN SELECT * FROM jsonb_to_recordset("Payload") 
	AS "Temp" ("S" BIGINT,  "P" INTEGER,"T" INTEGER, "C" BIGINT,"M" JSONB )
	LOOP
	
		RAISE NOTICE 'Processed: %',"Message"."M";-- <-- Your Processing Code will go here
		
		--Section to extract token out for acks
		IF "PendingTokensToAck" IS NULL THEN 
			"PendingTokensToAck" :=  jsonb_build_array(jsonb_build_object('T',"Message"."T",'C',"Message"."C"));
		ELSE
			"PendingTokensToAck" := "PendingTokensToAck" || jsonb_build_array(jsonb_build_object('T',"Message"."T",'C',"Message"."C"));
		END IF;
	END LOOP;
	
	--Following code acks the message.
	WHILE "PendingTokensToAck" IS NOT NULL AND "Attempts" > 0 LOOP
		SELECT "ACK-7a28fd25d95c0969bff16b963af1c832"("PendingTokensToAck") INTO "PendingTokensToAck";
		"Attempts":="Attempts"-1;
	END LOOP;
	COMMIT;-- <-- Required to release lock
END
$$;

-----------------------------------------------------------------------------DEBUG Section-------------------------------------------------------------------------
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
