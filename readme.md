# market price

Sample KDA app to join a quote stream and a position stream to calculate
market value of position as data arrives

Note that only a single input stream is supported by KDA-S, so you have
to pump all the data into a single stream, then filter them into separate application streams via SQL filtering...

This project has the following notebooks:

* kda-sql-app.ipynb - set up and tear down the app components (Kinesis data streams, IAM, application definition and config, etc)
* quotes-writer.ipynb - notebook to generate a quotes stream
* positions-writer.ipynb - notebook to generate a stream of positions
* scenario.ipynb - notebook to interactively write positions and quotes to the application input stream, good for exploring what the application produces on a steam entry by entry basis
* stream-reader.ipynb - notebook to read the application output stream

Something like...

```
CREATE OR REPLACE STREAM "POSITIONS_SQL_STREAM" (OWNER VARCHAR(12), SYMBOL VARCHAR(4), AMOUNT REAL);
CREATE OR REPLACE PUMP "STREAM_PUMP" AS INSERT INTO "POSITIONS_SQL_STREAM"
SELECT STREAM OWNER, SYMBOL, AMOUNT
FROM "SOURCE_SQL_STREAM_001"
WHERE OWNER IS NOT NULL;

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
    WHERE OWNER IS NULL
     GROUP BY TICKER, STEP("SOURCE_SQL_STREAM_001".ROWTIME BY INTERVAL '60' SECOND);
```

