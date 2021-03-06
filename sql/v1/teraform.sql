CREATE TABLE IF NOT EXISTS $1:name
(
    "Timestamp" timestamp without time zone NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC'),
    "Serial" bigserial NOT NULL,
    "CursorId" integer,
    "Payload" jsonb,
    PRIMARY KEY ("Timestamp", "Serial")
);

CREATE TABLE IF NOT EXISTS $2:name
(
    "Timestamp" timestamp without time zone NOT NULL,
    "Serial" bigint NOT NULL,
    "CursorId" bigserial,
    "Ack" integer,
	"Fetched" timestamp without time zone NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC'),
    "Token" integer NOT NULL DEFAULT (floor(random()*(10000000-0+1))+0),
    CONSTRAINT $3:name PRIMARY KEY ("CursorId")
)WITH (fillfactor=50);

CREATE INDEX  ON $2:name USING btree("CursorId" ASC NULLS LAST, "Ack" ASC NULLS LAST);
CREATE INDEX  ON $2:name USING btree("CursorId" ASC NULLS LAST, "Token" ASC NULLS LAST);