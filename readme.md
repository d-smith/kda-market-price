# market price

Sample KDA app to join a quote stream and a position stream to calculate
market value of position as data arrives

Note that only a single input stream is supported by KDA-S, so you have
to pump all the data into a single stream, then filter them into separate application streams via SQL filtering...

Something like...

```
CREATE OR REPLACE STREAM "POSITIONS_SQL_STREAM" (OWNER VARCHAR(12), TICKER VARCHAR(4), AMOUNT REAL);
CREATE OR REPLACE PUMP "STREAM_PUMP" AS INSERT INTO "POSITIONS_SQL_STREAM"
SELECT STREAM "owner", "ticker", "amount"
FROM "SOURCE_SQL_STREAM_001"
WHERE "owner" IS NOT NULL;

CREATE OR REPLACE STREAM "QUOTES_SQL_STREAM" (
    TICKER VARCHAR(4), 
    min_price     DOUBLE);
-- CREATE OR REPLACE PUMP to insert into output
CREATE OR REPLACE PUMP "STREAM_PUMP2" AS 
  INSERT INTO "QUOTES_SQL_STREAM" 
    SELECT STREAM 
        TICKER,
        MIN(price) AS min_price
    FROM "SOURCE_SQL_STREAM_001"
    WHERE "owner" IS NULL
     GROUP BY TICKER, STEP("SOURCE_SQL_STREAM_001".ROWTIME BY INTERVAL '60' SECOND);
```

